#!/usr/bin/env bash
# Monthly production run on EC2, on demand.
#
#   scripts/production-run.sh                 # preflight, then launch
#   scripts/production-run.sh --check-only    # preflight only, launch nothing
#   scripts/production-run.sh prod-20260801   # explicit run tag
#
# Every gate below has already cost a real run at least once. The script refuses
# to launch unless all of them pass, because the failures are silent: a stale
# image downloads 76 years instead of 23, an uncaptured identifier month is lost
# for good, and an incomplete feed builds a month with most securities missing.
#
# Requires: aws CLI authenticated to 485357734136, VPN up (the RDS is private),
# and a .env at the repo root containing COMPUSTAT.
set -euo pipefail
export MSYS_NO_PATHCONV=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
REGION=eu-central-1
BUCKET=jkp-data-runs-485357734136-eu-central-1
RDS_HOST=compustat.ckctiqup2z9k.eu-central-1.rds.amazonaws.com

CHECK_ONLY=false
RUN_TAG=""
for arg in "$@"; do
  case "$arg" in
    --check-only) CHECK_ONLY=true ;;
    *)            RUN_TAG="$arg" ;;
  esac
done
RUN_TAG="${RUN_TAG:-prod-$(date -u +%Y%m%d)}"

FAILED=0
pass() { printf '  \033[32mOK\033[0m   %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILED=$((FAILED + 1)); }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$1"; }

echo "== preflight for $RUN_TAG =="

# 1. AWS ---------------------------------------------------------------------
if ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
  [ "$ACCOUNT" = "485357734136" ] && pass "AWS account $ACCOUNT" \
    || fail "AWS account is $ACCOUNT, expected 485357734136"
else
  fail "aws CLI not authenticated"
fi

# 2. Credential --------------------------------------------------------------
grep -q '^COMPUSTAT=' "$REPO/.env" 2>/dev/null \
  && pass "COMPUSTAT credential present in .env" \
  || fail "no COMPUSTAT= line in $REPO/.env"

# 3. VPN ---------------------------------------------------------------------
if python -c "
import socket,sys
s=socket.socket(); s.settimeout(8)
try: s.connect(('$RDS_HOST',5432))
except Exception: sys.exit(1)
finally: s.close()
" 2>/dev/null; then
  pass "RDS reachable (VPN up)"
else
  fail "RDS unreachable - connect the VPN"
fi

# 4. Image freshness ---------------------------------------------------------
# A launcher newer than its image is the dangerous combination: this launcher
# stopped passing --start-date, so an image predating the rolling window falls
# back to ACCOUNTING_START_DATE and downloads from 1949.
if PUSHED=$(aws ecr describe-images --region "$REGION" --repository-name jkp-data \
             --image-ids imageTag=production --query "imageDetails[0].imagePushedAt" \
             --output text 2>/dev/null); then
  PUSHED_EPOCH=$(date -d "$PUSHED" +%s 2>/dev/null || echo 0)
  # Same paths as the trigger in .github/workflows/docker-publish.yml. If they
  # drift, this gate fails a run for a change that never triggered a build.
  SRC_EPOCH=$(cd "$REPO" && git log -1 --format=%ct -- \
    src/ sql/ Dockerfile pyproject.toml uv.lock .python-version 2>/dev/null || echo 0)
  if [ "$PUSHED_EPOCH" -ge "$SRC_EPOCH" ]; then
    pass "ECR image ($PUSHED) is newer than the last src/ change"
  else
    fail "ECR image ($PUSHED) predates the last src/ change ($(date -d @"$SRC_EPOCH" 2>/dev/null)) - rebuild and push, see DOCKER.md"
  fi
else
  fail "jkp-data:production missing from ECR"
fi

# 5. Feed completeness -------------------------------------------------------
if (cd "$REPO" && uv run python scripts/check_source_ready.py >/tmp/src_ready.log 2>&1); then
  pass "Compustat feed complete for the target month"
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
RUNNING=$(aws ec2 describe-instances --region "$REGION" \
  --filters Name=tag:Purpose,Values=jkp-data-timing-benchmark \
            Name=instance-state-name,Values=running,pending \
  --query "Reservations[].Instances[].InstanceId" --output text 2>/dev/null || true)
[ -z "$RUNNING" ] && pass "no run already in flight" \
  || fail "instance(s) already running: $RUNNING"

# 8. FF freshness ------------------------------------------------------------
# Not fatal: the pipeline falls back to the last available month exactly as the
# production SAS does. It does change ret_exc, so the run should record which.
FF=$(cd "$REPO" && uv run --quiet --with "psycopg[binary]" python -c "
import re,pathlib,psycopg
t=pathlib.Path('.env').read_text()
dsn=re.search(r'^COMPUSTAT=(.*)\$',t,re.M).group(1).strip().strip('\"').replace('postgresql+psycopg2://','postgresql://')
with psycopg.connect(dsn, connect_timeout=20) as c, c.cursor() as cur:
    cur.execute('select max(date), max(rf) from ff.factors_monthly where date=(select max(date) from ff.factors_monthly)')
    d,r=cur.fetchone(); print(f'{d} rf={r}')
" 2>/dev/null || echo "unavailable")
warn "ff.factors_monthly latest: $FF  (later months use this as fallback)"

echo
if [ "$FAILED" -gt 0 ]; then
  echo "== $FAILED gate(s) failed - not launching =="
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
echo "== refreshing ff.factors_monthly from WRDS =="
if (cd "$REPO" && uv run --with "psycopg[binary]" python sql/xpressfeed_views/load_ff_factors.py); then
  FF_AFTER=$(cd "$REPO" && uv run --quiet --with "psycopg[binary]" python -c "
import re,pathlib,psycopg
t=pathlib.Path('.env').read_text()
dsn=re.search(r'^COMPUSTAT=(.*)\$',t,re.M).group(1).strip().strip('\"').replace('postgresql+psycopg2://','postgresql://')
with psycopg.connect(dsn, connect_timeout=20) as c, c.cursor() as cur:
    cur.execute('select max(date) from ff.factors_monthly')
    print(cur.fetchone()[0])
" 2>/dev/null || echo unknown)
  echo "   ff.factors_monthly now runs through $FF_AFTER (was ${FF%% *})"
else
  # The run is still valid on the existing snapshot, so this does not stop it.
  warn "FF refresh failed; continuing on the snapshot already in the database"
fi

# Identifier capture ---------------------------------------------------------
# Must precede the download. Skipping does not fail the build, but that month's
# identifier changes are then lost for good: the feed only exposes current values.
echo
echo "== capturing point-in-time identifiers =="
(cd "$REPO" && uv run --with "psycopg[binary]" python sql/xpressfeed_views/capture_sec_ids.py)

# Launch ---------------------------------------------------------------------
# On demand, not spot: a reclaim on a production run costs a delivery.
# COUNTRIES unset uploads every country. START_DATE unset gives the rolling window.
echo
echo "== launching (on demand, all countries) =="
MARKET=ondemand "$HERE/ec2-benchmark/launch.sh" "$RUN_TAG"
