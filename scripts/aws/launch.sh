#!/usr/bin/env bash
# Launch a full jkp-data run on EC2.
#
#   scripts/aws/launch.sh [RUN_TAG]
#
# This is the launcher, not the entry point. A monthly production build starts
# at scripts/production-run.sh, which runs every gate, refreshes Fama-French and
# captures identifiers before calling this. Invoking this directly skips all of
# that, which is right only for a one-off re-run of a month already prepared.
#
# Idempotent: reuses the S3 bucket, IAM role/profile and security group if they
# already exist. Ships the COMPUSTAT credential via an SSM SecureString that is
# read once by the instance and then deleted, so no plaintext reaches SSM command
# history or CloudTrail.
#
# Requires: aws CLI authenticated to account 485357734136, and a .env at the repo
# root containing the COMPUSTAT connection string.
set -euo pipefail

# Git Bash rewrites bare /dev/... and /app/... arguments into Windows paths.
export MSYS_NO_PATHCONV=1

RUN_TAG="${1:-prod-$(date -u +%Y%m%d)}"
REGION=eu-central-1
ACCOUNT=485357734136
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$REGISTRY/jkp-data:production"
BUCKET="jkp-data-runs-$ACCOUNT-$REGION"
TOPIC="arn:aws:sns:$REGION:$ACCOUNT:jkp-spot-run-alerts"
ROLE=jkp-data-run-role
PROFILE=jkp-data-run-profile

# The RDS has no public endpoint, so the instance must sit in its VPC. This
# subnet is in the RDS's AZ and routes outbound through the NAT gateway, which
# the ECR pull and S3 uploads need.
VPC=vpc-0712bd9cff9966754
SUBNET=subnet-0a74f7b1e230a2f12

# r7i.16xlarge: 64 vCPU / 512 GiB, $5.107/h on demand in eu-central-1.
#
# Cheaper AND faster than m6a.32xlarge for this workload, which is not the
# obvious result -- m6a has twice the vCPUs. The middle phases (security_panels,
# market_returns, accounting_characteristics) are lightly threaded and run at
# single-digit CPU, so they are bound by per-core speed, where Sapphire Rapids
# beats EPYC decisively: security_panels 760s on r7i against ~1,300s on m6a,
# market_returns 449s against ~770s. Whole run 1h55m against ~2h30m, at
# $5.107/h against $6.624/h -- about $9.80 per run against $16.60.
#
# m6a was the right default only while MARKET defaulted to spot, where its ~77%
# discount beat r7i's ~49% and made it cheaper per hour. On demand that inverts.
# If you ever switch this back to spot, re-check both prices before assuming the
# type should change with it.
#
# Do not "upgrade" to m7i.16xlarge: 256 GiB against a ~351 GiB peak, so it OOMs.
INSTANCE_TYPE="${INSTANCE_TYPE:-r7i.16xlarge}"
VOLUME_GB="${VOLUME_GB:-750}"
VOLUME_IOPS="${VOLUME_IOPS:-8000}"
VOLUME_MBPS="${VOLUME_MBPS:-1000}"
# Unset means "use config.DAILY_DOWNLOAD_WORKERS". Set it only to A/B a
# different count; the flag overrides the config, so a default here would
# silently pin every run to that number.
WORKERS="${WORKERS:-}"
# Unset means the pipeline computes END_DATE minus config.ROLLING_INPUT_YEARS.
# Pin it only to reproduce an older run; a window shorter than 240 months nulls
# the seasonality characteristics.
START_DATE="${START_DATE:-}"
# Last month end, computed rather than pinned. This was hardcoded to a literal
# date, which is silent and wrong the moment the month rolls over: the run
# succeeds, uploads, and delivers the previous month again. It matches the target
# check_source_ready.py tests, so the month that was verified is the month built.
END_DATE="${END_DATE:-$(date -u -d "$(date -u +%Y-%m-01) -1 day" +%Y-%m-%d)}"
# Empty means every country the run produces (~95 GiB, ~$2.20/month in S3).
# Set it to a space-separated list to upload only those, e.g.
# COUNTRIES="usa can deu ita jpn hkg fra gbr ind nor" for the ten-country subset.
# Whatever is not uploaded dies with the volume.
COUNTRIES="${COUNTRIES:-}"
# On demand by default: every run this launches is a delivery, and a spot reclaim
# costs one outright -- the 200+ GB on the volume cannot leave in the 120 seconds
# the notice gives. MARKET=spot remains available for a throwaway re-run where
# losing the host only costs the time; the host arms a reclaim watcher when set.
MARKET="${MARKET:-ondemand}"
# Retain interim/ and raw/ so the accounting artefacts survive the run. Ignored
# by images that predate --keep-interim; the host probes for it before starting.
KEEP_INTERIM="${KEEP_INTERIM:-1}"
# 1 passes --db-update: after the outputs are written, the production CSVs are
# uploaded incrementally to the research MSSQL database named by the
# RESEARCH_UPDATE URL in the credential parameter (currently research_test).
# The URL must name "ODBC Driver 18 for SQL Server" -- that is the driver baked
# into the image. Ignored by images that predate --db-update; the host probes
# for it before starting. 0 skips the upload.
DB_UPDATE="${DB_UPDATE:-1}"
# Point at an existing SSM SecureString (see scripts/put-production-credentials.sh)
# instead of staging one from .env. The host then keeps the parameter rather than
# deleting it, which is what lets a scheduled run start with no human and no .env
# anywhere. Empty keeps the per-run staged-and-deleted credential.
CREDENTIAL_PARAM="${CREDENTIAL_PARAM:-}"
# 1 runs scripts/production-run.sh --unattended on the host before the pipeline:
# the readiness poll, the FF refresh and the identifier capture. 0 assumes a human
# already did those on their own machine, which is what production-run.sh does.
UNATTENDED="${UNATTENDED:-0}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

# Native Windows aws.exe cannot open Git Bash's /tmp paths while path
# conversion is disabled (which is required for /dev/xvda and /app paths).
# Convert only file:// arguments explicitly; leave all other paths untouched.
aws_file_uri() {
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) printf 'file://%s\n' "$(cygpath -m "$1")" ;;
    *)                    printf 'file://%s\n' "$1" ;;
  esac
}

command -v aws >/dev/null || { echo "aws CLI not found" >&2; exit 1; }
if [ -n "$CREDENTIAL_PARAM" ]; then
  # Fail here rather than on the host: a missing parameter is a five-second
  # check locally and a twenty-minute boot-and-die remotely.
  aws ssm get-parameter --region "$REGION" --name "$CREDENTIAL_PARAM" >/dev/null \
    || { echo "CREDENTIAL_PARAM $CREDENTIAL_PARAM not found in SSM; run scripts/put-production-credentials.sh" >&2; exit 1; }
  if [ "$DB_UPDATE" = "1" ]; then
    # The upload runs last, hours in; a wrong driver name would waste the whole
    # run. The image ships only "ODBC Driver 18 for SQL Server". Grep, never
    # print: the value is a SecureString for a reason.
    aws ssm get-parameter --region "$REGION" --name "$CREDENTIAL_PARAM" --with-decryption \
      --query Parameter.Value --output text \
      | grep -q '^RESEARCH_UPDATE=.*ODBC+Driver+18+for+SQL+Server' \
      || { echo "DB_UPDATE=1 but $CREDENTIAL_PARAM carries no RESEARCH_UPDATE naming ODBC Driver 18 (the image's driver). Re-seed with scripts/put-production-credentials.sh, or set DB_UPDATE=0." >&2; exit 1; }
  fi
else
  grep -q '^COMPUSTAT=' "$REPO/.env" || { echo "No COMPUSTAT= line in $REPO/.env" >&2; exit 1; }
  if [ "$DB_UPDATE" = "1" ]; then
    # Environment wins over the file, as everywhere in this repo. A workstation
    # .env usually names driver 17 (the Windows install); the container has 18.
    RESEARCH_UPDATE_VALUE="${RESEARCH_UPDATE:-$(grep '^RESEARCH_UPDATE=' "$REPO/.env" | cut -d= -f2- || true)}"
    case "$RESEARCH_UPDATE_VALUE" in
      *ODBC+Driver+18+for+SQL+Server*) ;;
      "") echo "DB_UPDATE=1 but RESEARCH_UPDATE is not set (environment or .env). Set it or pass DB_UPDATE=0." >&2; exit 1 ;;
      *) echo "DB_UPDATE=1 but RESEARCH_UPDATE does not name 'ODBC+Driver+18+for+SQL+Server' -- the only driver in the image. Export the driver-18 form (see scripts/put-production-credentials.sh) or pass DB_UPDATE=0." >&2; exit 1 ;;
    esac
  fi
fi
aws ecr describe-images --region "$REGION" --repository-name jkp-data \
  --image-ids imageTag=production >/dev/null \
  || { echo "Image jkp-data:production missing from ECR; build and push first" >&2; exit 1; }

echo "== ensuring S3 bucket =="
aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION" >/dev/null 2>&1 || true
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'

# RUN_TAG defaults to the UTC date, so a second launch on the same day would
# overwrite the first run's artifacts without warning.
if aws s3 ls "s3://$BUCKET/$RUN_TAG/" 2>/dev/null | grep -q .; then
  echo "s3://$BUCKET/$RUN_TAG/ already contains objects." >&2
  echo "Pass a distinct run tag: launch.sh ${RUN_TAG}b" >&2
  exit 1
fi

echo "== ensuring IAM role/profile =="
aws iam create-role --role-name "$ROLE" --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null 2>&1 || true
aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
aws iam put-role-policy --role-name "$ROLE" --policy-name jkp-run-bucket-and-alerts --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\",\"s3:ListBucket\",\"s3:AbortMultipartUpload\"],\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]},{\"Effect\":\"Allow\",\"Action\":\"sns:Publish\",\"Resource\":\"$TOPIC\"}]}"
# Standing grant on /jkp-data/* so the host can fetch its own credential and
# delete it afterwards. Persistent on purpose: a per-run grant has to be revoked
# by the launcher, and that coupling is what forced the old wait/poll handshake.
# Scoped to the prefix and to SSM-mediated KMS decrypts only.
#
# The explicit Deny carves /jkp-data/production/* back out of the delete grant.
# That parameter outlives every run, so a host that deletes it takes the next
# month's run down with it -- and unlike a per-run secret nobody would notice
# until 09:00 on the 1st. user-data.sh already refuses to delete a persistent
# parameter; this makes it true even if that logic is wrong.
aws iam put-role-policy --role-name "$ROLE" --policy-name jkp-read-run-env --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"ssm:GetParameter\",\"ssm:DeleteParameter\"],\"Resource\":\"arn:aws:ssm:$REGION:$ACCOUNT:parameter/jkp-data/*\"},{\"Effect\":\"Deny\",\"Action\":\"ssm:DeleteParameter\",\"Resource\":\"arn:aws:ssm:$REGION:$ACCOUNT:parameter/jkp-data/production/*\"},{\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"kms:ViaService\":\"ssm.$REGION.amazonaws.com\"}}}]}"
aws iam create-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1 || true
aws iam add-role-to-instance-profile --instance-profile-name "$PROFILE" --role-name "$ROLE" 2>/dev/null || true

echo "== ensuring security group =="
SG=$(aws ec2 describe-security-groups --region "$REGION" \
      --filters Name=group-name,Values=jkp-data-run-sg Name=vpc-id,Values="$VPC" \
      --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo None)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(aws ec2 create-security-group --region "$REGION" --group-name jkp-data-run-sg \
        --description "jkp-data run: egress only, no inbound (SSM)" --vpc-id "$VPC" \
        --query GroupId --output text)
fi

# The host reads this parameter itself at boot, so the credential is never handed
# over interactively and the launcher has nothing to wait for. A per-run parameter
# is deleted by the host after it reads it; a persistent one supplied through
# CREDENTIAL_PARAM is left alone, because the next run needs it.
if [ -n "$CREDENTIAL_PARAM" ]; then
  PARAM="$CREDENTIAL_PARAM"
  PARAM_EPHEMERAL=0
  echo "== using persistent credential $PARAM =="
  cleanup() { [ -z "${USER_DATA:-}" ] || rm -f "$USER_DATA"; }
else
  PARAM="/jkp-data/$RUN_TAG/env"
  PARAM_EPHEMERAL=1
  echo "== staging credential (SecureString, deleted by the host after it reads it) =="
  # On a clean exit the host owns the parameter and deletes it after reading. On
  # any failure we cannot know a host is coming, so remove the secret rather than
  # leave it sitting in Parameter Store.
  cleanup() {
    RC=$?
    [ -z "${TMP:-}" ] || rm -f "$TMP"
    [ -z "${USER_DATA:-}" ] || rm -f "$USER_DATA"
    if [ "$RC" -ne 0 ]; then
      aws ssm delete-parameter --region "$REGION" --name "$PARAM" >/dev/null 2>&1 || true
      echo "launch failed; removed $PARAM" >&2
    fi
  }
fi
trap cleanup EXIT
if [ "$PARAM_EPHEMERAL" = "1" ]; then
  TMP="$(mktemp)"
  grep '^COMPUSTAT=' "$REPO/.env" > "$TMP"
  if [ "$DB_UPDATE" = "1" ]; then
    printf 'RESEARCH_UPDATE=%s\n' "$RESEARCH_UPDATE_VALUE" >> "$TMP"
  fi
  aws ssm put-parameter --region "$REGION" --name "$PARAM" --type SecureString \
    --value "$(aws_file_uri "$TMP")" --overwrite >/dev/null
  rm -f "$TMP"
fi

AMI=$(aws ec2 describe-images --region "$REGION" --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-kernel-6.1-x86_64" "Name=state,Values=available" \
  --query "sort_by(Images,&CreationDate)[-1].ImageId" --output text)

USER_DATA="$(mktemp)"
sed -e "s|@@REGION@@|$REGION|g" -e "s|@@BUCKET@@|$BUCKET|g" -e "s|@@TOPIC@@|$TOPIC|g" \
    -e "s|@@IMAGE@@|$IMAGE|g" -e "s|@@RUN_TAG@@|$RUN_TAG|g" -e "s|@@COUNTRIES@@|$COUNTRIES|g" \
    -e "s|@@START_DATE@@|$START_DATE|g" -e "s|@@END_DATE@@|$END_DATE|g" -e "s|@@WORKERS@@|$WORKERS|g" \
    -e "s|@@KEEP_INTERIM@@|$KEEP_INTERIM|g" -e "s|@@DB_UPDATE@@|$DB_UPDATE|g" -e "s|@@PARAM@@|$PARAM|g" \
    -e "s|@@MARKET@@|$MARKET|g" -e "s|@@PARAM_EPHEMERAL@@|$PARAM_EPHEMERAL|g" \
    -e "s|@@UNATTENDED@@|$UNATTENDED|g" \
    "$HERE/user-data.sh" | tr -d '\r' > "$USER_DATA"

MARKET_OPT=(); [ "$MARKET" = "spot" ] && MARKET_OPT=(--instance-market-options 'MarketType=spot')

echo "== launching $INSTANCE_TYPE ($MARKET), ${VOLUME_GB}GB gp3 =="
INSTANCE=$(aws ec2 run-instances --region "$REGION" \
  --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
  --subnet-id "$SUBNET" --security-group-ids "$SG" \
  --iam-instance-profile "Name=$PROFILE" "${MARKET_OPT[@]}" \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3,Iops=$VOLUME_IOPS,Throughput=$VOLUME_MBPS,DeleteOnTermination=true,Encrypted=true}" \
  --metadata-options 'HttpTokens=required,HttpEndpoint=enabled' \
  --user-data "$(aws_file_uri "$USER_DATA")" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jkp-$RUN_TAG},{Key=Purpose,Value=jkp-data-production}]" \
  --query "Instances[0].InstanceId" --output text)
rm -f "$USER_DATA"
echo "instance: $INSTANCE"

# Spot capacity is the only thing worth blocking on here; everything after this
# the host does for itself, and reports by email.
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE"
echo "instance running; the host now fetches its credential and starts the run"

cat <<EOF

Launched. Instance: $INSTANCE   Artifacts: s3://$BUCKET/$RUN_TAG/
Instance type: $INSTANCE_TYPE ($MARKET)   Workers: ${WORKERS:-config default}
Keep interim: $KEEP_INTERIM   DB update: $DB_UPDATE   Countries to S3: ${COUNTRIES:-all (~95 GiB)}
Credential: $PARAM $([ "$PARAM_EPHEMERAL" = 1 ] && echo "(per-run, host deletes it)" || echo "(persistent, host keeps it)")
Unattended prep on host: $([ "$UNATTENDED" = 1 ] && echo "yes (readiness poll, FF refresh, identifier capture)" || echo "no")

The host installs docker, pulls the image, reads $PARAM, and starts the pipeline
on its own. Expect "JKP RUN: started" in a few minutes, or
"JKP RUN: BOOTSTRAP FAILED" with the reason if it cannot get that far.

Watch:
  scripts/aws/status.sh $INSTANCE

When finished (email arrives with the elapsed time):
  aws s3 sync s3://$BUCKET/$RUN_TAG/ "D:/jkp-full-run-${RUN_TAG#run-}/" --exclude "production/*"
  aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE
EOF
