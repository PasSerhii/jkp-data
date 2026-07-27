#!/usr/bin/env bash
# Launch a full jkp-data timing benchmark on EC2 (spot by default).
#
#   scripts/ec2-benchmark/launch.sh [RUN_TAG]
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

RUN_TAG="${1:-run-$(date -u +%Y%m%d)}"
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

INSTANCE_TYPE="${INSTANCE_TYPE:-m6a.32xlarge}"
VOLUME_GB="${VOLUME_GB:-750}"
VOLUME_IOPS="${VOLUME_IOPS:-8000}"
VOLUME_MBPS="${VOLUME_MBPS:-1000}"
# Unset means "use config.DAILY_DOWNLOAD_WORKERS". Set it only to A/B a
# different count; the flag overrides the config, so a default here would
# silently pin every run to that number.
WORKERS="${WORKERS:-}"
START_DATE="${START_DATE:-2000-01-01}"
END_DATE="${END_DATE:-2026-06-30}"
COUNTRIES="${COUNTRIES:-usa deu fra ita jpn}"
MARKET="${MARKET:-spot}"          # set MARKET=ondemand to disable spot

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

command -v aws >/dev/null || { echo "aws CLI not found" >&2; exit 1; }
grep -q '^COMPUSTAT=' "$REPO/.env" || { echo "No COMPUSTAT= line in $REPO/.env" >&2; exit 1; }
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

echo "== ensuring IAM role/profile =="
aws iam create-role --role-name "$ROLE" --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null 2>&1 || true
aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
aws iam put-role-policy --role-name "$ROLE" --policy-name jkp-run-bucket-and-alerts --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\",\"s3:ListBucket\",\"s3:AbortMultipartUpload\"],\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]},{\"Effect\":\"Allow\",\"Action\":\"sns:Publish\",\"Resource\":\"$TOPIC\"}]}"
aws iam create-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1 || true
aws iam add-role-to-instance-profile --instance-profile-name "$PROFILE" --role-name "$ROLE" 2>/dev/null || true

echo "== ensuring security group =="
SG=$(aws ec2 describe-security-groups --region "$REGION" \
      --filters Name=group-name,Values=jkp-data-run-sg Name=vpc-id,Values="$VPC" \
      --query "SecurityGroups[0].GroupId" --output text 2>/dev/null)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(aws ec2 create-security-group --region "$REGION" --group-name jkp-data-run-sg \
        --description "jkp-data run: egress only, no inbound (SSM)" --vpc-id "$VPC" \
        --query GroupId --output text)
fi

echo "== staging credential (SecureString, deleted after the instance reads it) =="
PARAM="/jkp-data/$RUN_TAG/env"
TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT
grep '^COMPUSTAT=' "$REPO/.env" > "$TMP"
aws ssm put-parameter --region "$REGION" --name "$PARAM" --type SecureString \
  --value "file://$TMP" --overwrite >/dev/null
aws iam put-role-policy --role-name "$ROLE" --policy-name jkp-read-run-env --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"ssm:GetParameter\",\"Resource\":\"arn:aws:ssm:$REGION:$ACCOUNT:parameter$PARAM\"},{\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"kms:ViaService\":\"ssm.$REGION.amazonaws.com\"}}}]}"

AMI=$(aws ec2 describe-images --region "$REGION" --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-kernel-6.1-x86_64" "Name=state,Values=available" \
  --query "sort_by(Images,&CreationDate)[-1].ImageId" --output text)

USER_DATA="$(mktemp)"
sed -e "s|@@REGION@@|$REGION|g" -e "s|@@BUCKET@@|$BUCKET|g" -e "s|@@TOPIC@@|$TOPIC|g" \
    -e "s|@@IMAGE@@|$IMAGE|g" -e "s|@@RUN_TAG@@|$RUN_TAG|g" -e "s|@@COUNTRIES@@|$COUNTRIES|g" \
    -e "s|@@START_DATE@@|$START_DATE|g" -e "s|@@END_DATE@@|$END_DATE|g" -e "s|@@WORKERS@@|$WORKERS|g" \
    "$HERE/user-data.sh" | tr -d '\r' > "$USER_DATA"

MARKET_OPT=(); [ "$MARKET" = "spot" ] && MARKET_OPT=(--instance-market-options 'MarketType=spot')

echo "== launching $INSTANCE_TYPE ($MARKET), ${VOLUME_GB}GB gp3 =="
INSTANCE=$(aws ec2 run-instances --region "$REGION" \
  --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
  --subnet-id "$SUBNET" --security-group-ids "$SG" \
  --iam-instance-profile "Name=$PROFILE" "${MARKET_OPT[@]}" \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3,Iops=$VOLUME_IOPS,Throughput=$VOLUME_MBPS,DeleteOnTermination=true,Encrypted=true}" \
  --metadata-options 'HttpTokens=required,HttpEndpoint=enabled' \
  --user-data "file://$USER_DATA" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jkp-$RUN_TAG},{Key=Purpose,Value=jkp-data-timing-benchmark}]" \
  --query "Instances[0].InstanceId" --output text)
rm -f "$USER_DATA"
echo "instance: $INSTANCE"

aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE"
echo "== waiting for SSM + image pull (a few minutes) =="
for _ in $(seq 1 40); do
  sleep 20
  READY=$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE" \
    --document-name AWS-RunShellScript \
    --parameters 'commands=["docker images --format \"{{.Repository}}\" | head -1"]' \
    --query "Command.CommandId" --output text 2>/dev/null) || continue
  sleep 12
  OUT=$(aws ssm get-command-invocation --region "$REGION" --command-id "$READY" \
    --instance-id "$INSTANCE" --query StandardOutputContent --output text 2>/dev/null || true)
  case "$OUT" in *dkr.ecr*) echo "image pulled"; break ;; esac
done

echo "== handing credential to the instance =="
CID=$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE" \
  --document-name AWS-RunShellScript \
  --parameters "commands=[\"aws ssm get-parameter --region $REGION --name $PARAM --with-decryption --query Parameter.Value --output text > /secure/jkp.env\",\"chmod 600 /secure/jkp.env\",\"wc -c < /secure/jkp.env\"]" \
  --query "Command.CommandId" --output text)
sleep 20
aws ssm get-command-invocation --region "$REGION" --command-id "$CID" --instance-id "$INSTANCE" \
  --query "{status:Status,bytes:StandardOutputContent}" --output json

aws ssm delete-parameter --region "$REGION" --name "$PARAM" >/dev/null 2>&1 || true
aws iam delete-role-policy --role-name "$ROLE" --policy-name jkp-read-run-env >/dev/null 2>&1 || true
echo "credential delivered; SecureString and read grant removed"

cat <<EOF

Run started. Instance: $INSTANCE   Artifacts: s3://$BUCKET/$RUN_TAG/

Watch:
  scripts/ec2-benchmark/status.sh $INSTANCE

When finished (email arrives with the elapsed time):
  aws s3 sync s3://$BUCKET/$RUN_TAG/ "D:/jkp-full-run-\${RUN_TAG#run-}/" --exclude "production/*"
  aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE
EOF
