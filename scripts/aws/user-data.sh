#!/bin/bash
# Bootstrap for a full jkp-data run on EC2.
#
# launch.sh substitutes the double-at tokens below before passing this as
# user-data; tests/unit/test_unattended_run_wiring.py checks that none is left
# behind. The host installs docker, pulls the image, reads its own credential
# from SSM, optionally prepares the sources (UNATTENDED=1), then runs the
# pipeline end to end and ships logs plus the production CSVs to S3.
set -xuo pipefail
exec > >(tee -a /var/log/jkp-setup.log) 2>&1

REGION=@@REGION@@
BUCKET=@@BUCKET@@
TOPIC=@@TOPIC@@
IMAGE=@@IMAGE@@
RUN_PREFIX="s3://$BUCKET/@@RUN_TAG@@"
COUNTRIES="@@COUNTRIES@@"
WORKERS="@@WORKERS@@"
START_DATE="@@START_DATE@@"
KEEP_INTERIM="@@KEEP_INTERIM@@"
PARAM="@@PARAM@@"
PARAM_EPHEMERAL="@@PARAM_EPHEMERAL@@"
MARKET="@@MARKET@@"
UNATTENDED="@@UNATTENDED@@"

# The interim files the accounting tests read. The whole tree is ~230 GiB, so
# only these ship to S3; --keep-interim leaves the rest on the volume to inspect
# before the instance is terminated.
INTERIM_KEEP="acc_std_ann.parquet acc_std_qtr.parquet achars_world.parquet qchars_world.parquet acc_chars_world.parquet"

notify() { aws sns publish --region "$REGION" --topic-arn "$TOPIC" --subject "$1" --message "$2" >/dev/null 2>&1 || true; }

install -d -m 700 /secure
install -d -o 10001 -g 10001 -m 700 /mnt/jkp-data

# Bootstrap errors must stop the host here. This script has no `set -e` (the
# upload section relies on `[ -f x ] && cp`), so every setup step is checked by
# hand: a silent failure leaves a paid instance idling with no image, which is
# how run-20260729 lost 20 minutes before anyone looked.
fail() {
  echo "BOOTSTRAP FAILED: $1"
  touch /mnt/jkp-data/BOOTSTRAP_FAILED 2>/dev/null
  notify "JKP RUN: BOOTSTRAP FAILED" \
"$1

The pipeline never started, so no output was produced.
Setup log: /var/log/jkp-setup.log
Instance still RUNNING for diagnosis — terminate it when done."
  exit 1
}

# A fresh AL2023 AMI sometimes ships a stale dnf cache that points at rpms which
# are no longer on the mirror; clearing it and retrying is the documented fix.
if ! dnf install -y docker; then
  dnf clean all
  rm -rf /var/cache/dnf
  dnf install -y docker || fail "dnf install docker failed twice (stale cache not the cause)"
fi
command -v docker >/dev/null || fail "dnf reported success but no docker binary is on PATH"

systemctl enable --now docker || fail "systemctl enable --now docker failed"
for _ in $(seq 1 30); do
  systemctl is-active --quiet docker && break
  sleep 2
done
systemctl is-active --quiet docker || fail "docker daemon never became active"

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${IMAGE%%/*}" \
  || fail "ECR login failed for ${IMAGE%%/*}"
docker pull "$IMAGE" || fail "docker pull $IMAGE failed"
docker image inspect "$IMAGE" >/dev/null 2>&1 \
  || fail "$IMAGE absent after a pull that reported success"

# Take the operational scripts out of the image rather than cloning the repo.
# production-run.sh needs bash/aws/docker and so has to run on the host, but it
# gates the very image it prepares for -- pulling it from anywhere else lets the
# two versions drift. Extracted like this they are the image, by construction.
REVISION=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE" 2>/dev/null)
CID=$(docker create "$IMAGE" 2>/dev/null)
if [ -n "$CID" ]; then
  rm -rf /opt/jkp
  docker cp "$CID:/opt/jkp" /opt/jkp 2>/dev/null
  docker rm -f "$CID" >/dev/null 2>&1
fi
echo "image revision: ${REVISION:-unlabelled}; host scripts: $(ls /opt/jkp/scripts 2>/dev/null | tr '\n' ' ')"

# Spot reclaim gives a 2-minute warning: alert and flush logs before the host dies.
# The rebalance recommendation usually lands earlier and carries no guarantee, so it
# only warns — it is the cue to get a replacement ready, not proof of a reclaim.
# There is no point saving the run itself: 200+ GB cannot leave in 120 seconds.
cat >/usr/local/bin/spot-watch.sh <<EOF
#!/bin/bash
IMDS=http://169.254.169.254
WARNED=0
while true; do
  T=\$(curl -sX PUT \$IMDS/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
  if [ "\$WARNED" = "0" ] && [ "\$(curl -s -o /dev/null -w "%{http_code}" -H "X-aws-ec2-metadata-token: \$T" \$IMDS/latest/meta-data/events/recommendations/rebalance)" = "200" ]; then
    WARNED=1
    aws sns publish --region "$REGION" --topic-arn "$TOPIC" \
      --subject "JKP RUN: spot rebalance recommendation" \
      --message "EC2 recommends moving off this host: capacity is tightening and a reclaim is likely, though not certain and not necessarily soon. The run is still going. If this run is a delivery, relaunch it on demand now rather than waiting: scripts/aws/launch.sh <new-tag>"
  fi
  if [ "\$(curl -s -o /dev/null -w "%{http_code}" -H "X-aws-ec2-metadata-token: \$T" \$IMDS/latest/meta-data/spot/instance-action)" = "200" ]; then
    aws s3 sync /mnt/jkp-data/run_logs "$RUN_PREFIX/run_logs/" --only-show-errors
    aws sns publish --region "$REGION" --topic-arn "$TOPIC" \
      --subject "JKP RUN INTERRUPTED (spot reclaim)" \
      --message "Spot reclaim, 2-minute notice. Logs flushed to $RUN_PREFIX/run_logs/. The run did NOT finish; the volume goes with the host. Relaunch on demand (the default)."
    break
  fi
  sleep 5
done
EOF
chmod +x /usr/local/bin/spot-watch.sh
# On demand there is nothing to watch: both endpoints 404 for the life of the host.
if [ "$MARKET" = "spot" ]; then
  nohup /usr/local/bin/spot-watch.sh >/var/log/spot-watch.log 2>&1 &
fi

cat >/usr/local/bin/log-sync.sh <<EOF
#!/bin/bash
while true; do
  sleep 300
  aws s3 sync /mnt/jkp-data/run_logs "$RUN_PREFIX/run_logs/" --only-show-errors 2>/dev/null
  [ -f /mnt/jkp-data/DONE ] && break
done
EOF
chmod +x /usr/local/bin/log-sync.sh
nohup /usr/local/bin/log-sync.sh >/var/log/log-sync.log 2>&1 &

# Read our own credential from Parameter Store via the instance role, then delete
# it. Nothing is handed to us, so there is no handshake to wait on and no timeout
# to tune; the retry loop covers IAM/SSM propagation on a cold account only.
for _ in $(seq 1 30); do
  aws ssm get-parameter --region "$REGION" --name "$PARAM" --with-decryption \
    --query Parameter.Value --output text > /secure/jkp.env 2>/tmp/ssm-fetch.err && \
    [ -s /secure/jkp.env ] && break
  sleep 10
done
[ -s /secure/jkp.env ] || fail "Could not read $PARAM within 5 minutes: $(tail -2 /tmp/ssm-fetch.err 2>/dev/null)"
chmod 600 /secure/jkp.env
grep -q '^COMPUSTAT=' /secure/jkp.env || fail "$PARAM held no COMPUSTAT= line; the credential is malformed"

# A per-run parameter has served its purpose once it is on disk; deleting it here
# keeps the secret's lifetime to the first minute of the run without the launcher
# having to coordinate the revoke. A persistent one belongs to the next run too,
# so leave it exactly where it is.
if [ "$PARAM_EPHEMERAL" = "1" ]; then
  aws ssm delete-parameter --region "$REGION" --name "$PARAM" >/dev/null 2>&1 \
    || notify "JKP RUN: could not delete $PARAM" \
"The run continues normally, but the SecureString is still there. Remove it:
  aws ssm delete-parameter --region $REGION --name $PARAM"
fi

# Readiness poll, Fama-French refresh and identifier capture, in that order.
# Attended runs do these on the operator's machine before launching; an
# unattended one has no operator, so the host does them for itself. Anything
# fatal here stops the run before the two-hour pipeline starts.
if [ "$UNATTENDED" = "1" ]; then
  if [ -x /opt/jkp/scripts/production-run.sh ]; then
    RUN_TAG="@@RUN_TAG@@" IMAGE="$IMAGE" ENV_FILE=/secure/jkp.env \
      /opt/jkp/scripts/production-run.sh --unattended \
      || fail "unattended preflight failed; see /var/log/jkp-setup.log"
  else
    fail "UNATTENDED=1 but $IMAGE ships no /opt/jkp/scripts/production-run.sh"
  fi
fi

# Only pass --keep-interim to an image that understands it; an older image would
# abort on the unknown option and waste the entire run.
KEEP_FLAG=""
if [ "$KEEP_INTERIM" = "1" ]; then
  if docker run --rm "$IMAGE" build --help 2>/dev/null | grep -q -- "--keep-interim"; then
    KEEP_FLAG="--keep-interim"
  else
    notify "JKP RUN: --keep-interim not supported by this image" \
"$IMAGE predates --keep-interim, so interim/ is deleted at the end of the run as before.
The processed/ accounting copies are still uploaded; the interim originals are not."
  fi
fi

START_EPOCH=$(date -u +%s)
date -u -d "@$START_EPOCH" +"%Y-%m-%dT%H:%M:%SZ" > /mnt/jkp-data/STARTED_AT
notify "JKP RUN: started" "Started $(cat /mnt/jkp-data/STARTED_AT). Fresh download, no raw reuse."

docker run --name jkp-run \
  --env-file /secure/jkp.env \
  --mount type=bind,source=/mnt/jkp-data,target=/data \
  "$IMAGE" \
  build /data \
  --force \
  --compustat-source xpressfeed \
  --bypass-crsp \
  --persistent-connection \
  ${START_DATE:+--start-date $START_DATE} \
  --end-date @@END_DATE@@ \
  --production \
  ${WORKERS:+--daily-download-workers $WORKERS} \
  ${KEEP_FLAG} \
  --metrics-interval 30 \
  > /mnt/jkp-data/container.log 2>&1
RC=$?

END_EPOCH=$(date -u +%s); ELAPSED=$((END_EPOCH - START_EPOCH))
printf '%dh%02dm%02ds (%ds)\n' $((ELAPSED/3600)) $(((ELAPSED%3600)/60)) $((ELAPSED%60)) "$ELAPSED" > /mnt/jkp-data/ELAPSED
date -u -d "@$END_EPOCH" +"%Y-%m-%dT%H:%M:%SZ" > /mnt/jkp-data/FINISHED_AT

aws s3 sync /mnt/jkp-data/run_logs "$RUN_PREFIX/run_logs/" --only-show-errors
for f in container.log ELAPSED STARTED_AT FINISHED_AT source_snapshot_manifest.json; do
  [ -f "/mnt/jkp-data/$f" ] && aws s3 cp "/mnt/jkp-data/$f" "$RUN_PREFIX/$f" --only-show-errors
done

# Accounting artefacts for the post-run tests. All of this runs after ELAPSED is
# recorded, so upload time is excluded from the reported elapsed.
#
# processed/accounting_data and processed/other_output are written by
# save_accounting_data/save_output_files and survive regardless of --keep-interim;
# accounting_data is acc_std_{ann,qtr} filtered to a non-null source. The interim
# originals below exist only when --keep-interim was in force, because
# save_full_files_and_cleanup otherwise deletes interim/ and raw/ outright — which
# is why an unconditional `sync interim/` placed here uploads nothing.
for d in accounting_data other_output; do
  [ -d "/mnt/jkp-data/processed/$d" ] && \
    aws s3 sync "/mnt/jkp-data/processed/$d" "$RUN_PREFIX/$d/" --only-show-errors
done
for f in $INTERIM_KEEP; do
  [ -f "/mnt/jkp-data/interim/$f" ] && \
    aws s3 cp "/mnt/jkp-data/interim/$f" "$RUN_PREFIX/interim/$f" --only-show-errors
done

# processed/production/ is the whole production deliverable: monthly/<country>.csv,
# daily/<country>.csv, and the six cross-country files the sasWrds uploader reads.
CSV_DIR=/mnt/jkp-data/processed/production
if [ -z "$COUNTRIES" ]; then
  # Default: everything. `sync` uploads in parallel and resumes cleanly, which
  # matters across ~95 GiB where a per-file `cp` loop would crawl.
  aws s3 sync "$CSV_DIR" "$RUN_PREFIX/production/" --only-show-errors
else
  for c in $COUNTRIES; do
    for freq in monthly daily; do
      [ -f "$CSV_DIR/$freq/$c.csv" ] && aws s3 cp "$CSV_DIR/$freq/$c.csv" "$RUN_PREFIX/production/$freq/$c.csv" --only-show-errors
    done
  done
  # The cross-country files are not per-country, so a country subset still needs
  # them explicitly or the uploader has nothing to read.
  for f in market_returns.csv market_returns_daily.csv nyse_cutoffs.csv \
           return_cutoffs.csv return_cutoffs_daily.csv world_ret_monthly.csv; do
    [ -f "$CSV_DIR/$f" ] && aws s3 cp "$CSV_DIR/$f" "$RUN_PREFIX/production/$f" --only-show-errors
  done
fi
aws s3 ls --recursive --summarize "$RUN_PREFIX/production/" > /mnt/jkp-data/UPLOADED 2>&1
aws s3 cp /mnt/jkp-data/UPLOADED "$RUN_PREFIX/UPLOADED" --only-show-errors
df -h /mnt/jkp-data > /mnt/jkp-data/DISK_FINAL
aws s3 cp /mnt/jkp-data/DISK_FINAL "$RUN_PREFIX/DISK_FINAL" --only-show-errors
touch /mnt/jkp-data/DONE

if [ "$RC" -eq 0 ]; then
  notify "JKP RUN: FINISHED OK ($(cat /mnt/jkp-data/ELAPSED))" \
"Total elapsed (download -> all CSVs): $(cat /mnt/jkp-data/ELAPSED)
Started:  $(cat /mnt/jkp-data/STARTED_AT)
Finished: $(cat /mnt/jkp-data/FINISHED_AT)
Artifacts: $RUN_PREFIX/
Countries uploaded: ${COUNTRIES:-all}
$(tail -3 /mnt/jkp-data/UPLOADED)
Full listing: $RUN_PREFIX/UPLOADED
Disk: $(tail -1 /mnt/jkp-data/DISK_FINAL)
Interim retained on the volume: ${KEEP_FLAG:-no}

Instance still RUNNING for verification. Terminate when done."
else
  notify "JKP RUN: FAILED (exit $RC) after $(cat /mnt/jkp-data/ELAPSED)" \
"Exit $RC. Logs: $RUN_PREFIX/run_logs/ and $RUN_PREFIX/container.log
$(tail -40 /mnt/jkp-data/container.log)

Instance still RUNNING for diagnosis."
fi
