#!/usr/bin/env bash
# Monthly production run: preflight, prepare the sources, launch.
#
#   scripts/production-run.sh                 # preflight, then launch
#   scripts/production-run.sh --check-only    # preflight only, launch nothing
#   scripts/production-run.sh prod-20260801   # explicit run tag
#   scripts/production-run.sh --unattended    # on the run host; prepare, do not launch
#
# Every gate below has already cost a real run at least once. The script refuses
# to launch unless all of them pass, because the failures are silent: a stale
# image downloads 76 years instead of 23, an uncaptured identifier month is lost
# for good, and an incomplete feed builds a month with most securities missing.
#
# Two modes, one script:
#
#   attended (default) runs on an operator's machine. It needs the aws CLI
#   authenticated to 485357734136, the VPN up (the RDS is private), a .env at
#   the repo root, and uv. It ends by launching an EC2 instance.
#
#   --unattended runs on that instance, where there is no repo, no uv and no
#   human. Credentials come from an env file the host fetched from SSM, the
#   Python steps run inside the pulled image, the feed check becomes a bounded
#   poll instead of a verdict, and the script returns rather than launching --
#   user-data.sh starts the pipeline once this exits 0. It is the same gates in
#   the same order deliberately: a divergent copy is how the two drift.
set -euo pipefail
export MSYS_NO_PATHCONV=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
REGION=eu-central-1
BUCKET=jkp-data-runs-485357734136-eu-central-1
RDS_HOST=compustat.ckctiqup2z9k.eu-central-1.rds.amazonaws.com

# How long an unattended run waits for the feed. The delivery is normally in by
# 08:10 Israel time and the run starts at 09:00, so this is slack, not a
# schedule. Waiting costs instance time, which is why it is bounded.
WAIT_MINUTES=60
POLL_INTERVAL=10

CHECK_ONLY=false
UNATTENDED=false
RUN_TAG="${RUN_TAG:-}"
for arg in "$@"; do
  case "$arg" in
    --check-only) CHECK_ONLY=true ;;
    --unattended) UNATTENDED=true ;;
    *)            RUN_TAG="$arg" ;;
  esac
done
RUN_TAG="${RUN_TAG:-prod-$(date -u +%Y%m%d)}"

# The month being built. Passed explicitly to both the readiness check and the
# launcher, so the month that was verified is the month that gets built. They
# each defaulted to "last month end" independently before, which agreed only by
# coincidence -- and launch.sh's default was a hardcoded literal date, so after
# the month rolled over it would quietly rebuild and redeliver the month before.
TARGET_MONTH_END="${END_DATE:-$(date -u -d "$(date -u +%Y-%m-01) -1 day" +%Y-%m-%d)}"
export END_DATE="$TARGET_MONTH_END"

# Set by user-data.sh in unattended mode; unused otherwise.
IMAGE="${IMAGE:-}"
ENV_FILE="${ENV_FILE:-/secure/jkp.env}"

if [ "$UNATTENDED" = true ]; then
  [ -n "$IMAGE" ] || { echo "--unattended requires IMAGE=<ecr image>" >&2; exit 1; }
  [ -s "$ENV_FILE" ] || { echo "--unattended requires a populated ENV_FILE ($ENV_FILE)" >&2; exit 1; }
  SCRIPTS=/opt/jkp/scripts
  SQL=/opt/jkp/sql/xpressfeed_views
  # The image already carries the pipeline's exact dependency set, so running the
  # ops scripts inside it removes any chance of a different psycopg or duckdb.
  py() { docker run --rm --env-file "$ENV_FILE" --entrypoint /app/.venv/bin/python "$IMAGE" "$@"; }
else
  # Relative to $REPO, which py() changes into. Absolute is wrong here: on the
  # Windows dev machine $REPO is a Git Bash path (/d/projects/...) that the
  # native python.exe cannot open.
  SCRIPTS=scripts
  SQL=sql/xpressfeed_views
  py() { (cd "$REPO" && uv run --quiet python "$@"); }
fi

FAILED=0
pass() { printf '  \033[32mOK\033[0m   %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILED=$((FAILED + 1)); }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$1"; }
skip() { printf '  \033[90mSKIP\033[0m %s\n' "$1"; }

# One place that knows how to reach the database, so every gate below asks the
# same way in both modes: COMPUSTAT from the environment, or the repo .env.
ff_latest() {
  py -c "
import psycopg
from jkp.data.database_sources import get_xpressfeed_connection_info
with psycopg.connect(get_xpressfeed_connection_info(), connect_timeout=20) as c, c.cursor() as cur:
    cur.execute('select max(date) from ff.factors_monthly')
    print(cur.fetchone()[0])
" 2>/dev/null || echo unavailable
}

echo "== preflight for $RUN_TAG, month end $TARGET_MONTH_END ($([ "$UNATTENDED" = true ] && echo unattended || echo attended)) =="

# 1. AWS ---------------------------------------------------------------------
if ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
  [ "$ACCOUNT" = "485357734136" ] && pass "AWS account $ACCOUNT" \
    || fail "AWS account is $ACCOUNT, expected 485357734136"
else
  fail "aws CLI not authenticated"
fi

# 2. Credential --------------------------------------------------------------
# COMPUSTAT is fatal: nothing runs without it. The WRDS pair only gates the
# Fama-French refresh, which is already allowed to fail without stopping a run.
if [ "$UNATTENDED" = true ]; then
  CRED_SRC="$ENV_FILE"
else
  CRED_SRC="$REPO/.env"
fi
grep -q '^COMPUSTAT=' "$CRED_SRC" 2>/dev/null \
  && pass "COMPUSTAT credential present in $CRED_SRC" \
  || fail "no COMPUSTAT= line in $CRED_SRC"
if grep -q '^ENV_USERNAME=' "$CRED_SRC" 2>/dev/null && grep -q '^ENV_PASSWORD=' "$CRED_SRC" 2>/dev/null; then
  pass "WRDS credentials present (Fama-French refresh enabled)"
  FF_REFRESH=true
else
  warn "no ENV_USERNAME/ENV_PASSWORD in $CRED_SRC - skipping the Fama-French refresh"
  FF_REFRESH=false
fi

# 3. Database reachable ------------------------------------------------------
if py -c "
import socket,sys
s=socket.socket(); s.settimeout(8)
try: s.connect(('$RDS_HOST',5432))
except Exception: sys.exit(1)
finally: s.close()
" 2>/dev/null; then
  pass "RDS reachable"
elif [ "$UNATTENDED" = true ]; then
  fail "RDS unreachable - check the subnet route and the RDS security group"
else
  fail "RDS unreachable - connect the VPN"
fi

# 4. Image freshness ---------------------------------------------------------
# A launcher newer than its image is the dangerous combination: this launcher
# stopped passing --start-date, so an image predating the rolling window falls
# back to ACCOUNTING_START_DATE and downloads from 1949.
if [ "$UNATTENDED" = true ]; then
  # Nothing to compare against: this script came out of the image it would be
  # checking, so the two are the same commit by construction. Report which.
  REV=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE" 2>/dev/null || true)
  skip "image freshness - running from inside ${IMAGE##*/} (revision ${REV:-unlabelled})"
elif PUSHED=$(aws ecr describe-images --region "$REGION" --repository-name jkp-data \
             --image-ids imageTag=production --query "imageDetails[0].imagePushedAt" \
             --output text 2>/dev/null); then
  PUSHED_EPOCH=$(date -d "$PUSHED" +%s 2>/dev/null || echo 0)
  # Same paths as the trigger in .github/workflows/docker-publish.yml. If they
  # drift, this gate fails a run for a change that never triggered a build.
  SRC_EPOCH=$(cd "$REPO" && git log -1 --format=%ct -- \
    src/ sql/ scripts/check_source_ready.py scripts/production-run.sh \
    Dockerfile .dockerignore pyproject.toml uv.lock .python-version 2>/dev/null || echo 0)
  if [ "$PUSHED_EPOCH" -ge "$SRC_EPOCH" ]; then
    pass "ECR image ($PUSHED) is newer than the last src/ change"
  else
    fail "ECR image ($PUSHED) predates the last src/ change ($(date -d @"$SRC_EPOCH" 2>/dev/null)) - rebuild and push, see DOCKER.md"
  fi
else
  fail "jkp-data:production missing from ECR"
fi

# 5. Feed completeness -------------------------------------------------------
# Attended: a verdict, because a human can come back in an hour. Unattended:
# a bounded poll, because nobody will.
if [ "$UNATTENDED" = true ]; then
  echo "  .... waiting for the feed (up to ${WAIT_MINUTES} min, checking every ${POLL_INTERVAL})"
  if py "$SCRIPTS/check_source_ready.py" "$TARGET_MONTH_END" \
       --wait-minutes "$WAIT_MINUTES" --poll-interval "$POLL_INTERVAL" 2>&1 | sed 's/^/       /'; then
    pass "Compustat feed complete for $TARGET_MONTH_END"
  else
    fail "feed still incomplete after ${WAIT_MINUTES} min - the delivery is late"
  fi
elif py "$SCRIPTS/check_source_ready.py" "$TARGET_MONTH_END" >/tmp/src_ready.log 2>&1; then
  pass "Compustat feed complete for $TARGET_MONTH_END"
else
  fail "check_source_ready.py failed: $(tail -3 /tmp/src_ready.log | tr '\n' ' ')"
fi

# 6. Run tag free ------------------------------------------------------------
if aws s3 ls "s3://$BUCKET/$RUN_TAG/" 2>/dev/null | grep -q .; then
  fail "s3://$BUCKET/$RUN_TAG/ already has objects - pass a distinct tag"
else
  pass "run tag $RUN_TAG is free in S3"
fi

# 7. Nothing already running -------------------------------------------------
# Unattended, this host is itself a match, so ask IMDS who we are and discount
# it. A sentinel keeps the filter from matching everything when IMDS is silent.
SELF=__none__
if [ "$UNATTENDED" = true ]; then
  IMDS_TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
  SELF=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo __none__)
  [ -n "$SELF" ] || SELF=__none__
fi
RUNNING=$(aws ec2 describe-instances --region "$REGION" \
  --filters Name=tag:Purpose,Values=jkp-data-production \
            Name=instance-state-name,Values=running,pending \
  --query "Reservations[].Instances[].InstanceId" --output text 2>/dev/null \
  | tr '\t' '\n' | grep -vx "$SELF" | tr '\n' ' ' | sed 's/ *$//' || true)
[ -z "$RUNNING" ] && pass "no other run already in flight" \
  || fail "instance(s) already running: $RUNNING"

# 8. FF freshness ------------------------------------------------------------
# Not fatal: the pipeline falls back to the last available month exactly as the
# production SAS does. It does change ret_exc, so the run should record which.
FF_BEFORE=$(ff_latest)
warn "ff.factors_monthly latest: $FF_BEFORE  (later months use this as fallback)"

echo
if [ "$FAILED" -gt 0 ]; then
  echo "== $FAILED gate(s) failed - not proceeding =="
  exit 1
fi
echo "== all gates passed =="

if [ "$CHECK_ONLY" = true ]; then
  echo "--check-only: stopping here."
  exit 0
fi

# Fama-French refresh --------------------------------------------------------
# Copies ff.factors_monthly from WRDS. Idempotent and atomic -- rows are staged
# and validated before a transactional swap -- so a no-op costs seconds and a
# reader never sees a partial month.
#
# Worth doing every run because the pipeline falls back to the last available
# month for anything newer, exactly as the production SAS does
# (project_macros.sas: coalesce(c.rf, &lffm.)). Each missing month is another
# month of ret_exc carrying a stale rate, which is what put a 20bp gap between
# the July run and Research. Not fatal if WRDS has nothing new: the run records
# what it used in source_snapshot_manifest.json either way.
echo
if [ "$FF_REFRESH" != true ]; then
  echo "== skipping the Fama-French refresh (no WRDS credentials) =="
else
  echo "== refreshing ff.factors_monthly from WRDS =="
  if py "$SQL/load_ff_factors.py"; then
    echo "   ff.factors_monthly now runs through $(ff_latest) (was $FF_BEFORE)"
  else
    # The run is still valid on the existing snapshot, so this does not stop it.
    warn "FF refresh failed; continuing on the snapshot already in the database"
  fi
fi

# Identifier capture ---------------------------------------------------------
# Must precede the download. Skipping does not fail the build, but that month's
# identifier changes are then lost for good: the feed only exposes current values.
echo
echo "== capturing point-in-time identifiers =="
py "$SQL/capture_sec_ids.py"

if [ "$UNATTENDED" = true ]; then
  echo
  echo "== sources prepared; user-data.sh takes it from here =="
  exit 0
fi

# Launch ---------------------------------------------------------------------
# MARKET is redundant -- launch.sh already defaults to ondemand -- and stated
# anyway: a reclaim here costs a delivery, so this path should not silently
# follow a default someone else can change.
# COUNTRIES unset uploads every country. START_DATE unset gives the rolling window.
echo
echo "== launching (on demand, all countries, month end $TARGET_MONTH_END) =="
MARKET=ondemand "$HERE/aws/launch.sh" "$RUN_TAG"
