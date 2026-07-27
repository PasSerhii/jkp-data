#!/bin/bash
# Bootstrap for a full jkp-data timing benchmark on EC2.
#
# launch.sh substitutes the @@PLACEHOLDER@@ tokens before passing this as
# user-data. The host prepares itself, waits for the operator-supplied
# credential at /secure/jkp.env, then runs the pipeline end to end and ships
# logs plus the country CSVs under test to S3.
set -xuo pipefail
exec > >(tee -a /var/log/jkp-setup.log) 2>&1

REGION=@@REGION@@
BUCKET=@@BUCKET@@
TOPIC=@@TOPIC@@
IMAGE=@@IMAGE@@
RUN_PREFIX="s3://$BUCKET/@@RUN_TAG@@"
COUNTRIES="@@COUNTRIES@@"
WORKERS="@@WORKERS@@"

notify() { aws sns publish --region "$REGION" --topic-arn "$TOPIC" --subject "$1" --message "$2" >/dev/null 2>&1 || true; }

dnf install -y docker >/dev/null
systemctl enable --now docker

install -d -m 700 /secure
install -d -o 10001 -g 10001 -m 700 /mnt/jkp-data

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${IMAGE%%/*}"
docker pull "$IMAGE"

# Spot reclaim gives a 2-minute warning: alert and flush logs before the host dies.
cat >/usr/local/bin/spot-watch.sh <<EOF
#!/bin/bash
while true; do
  T=\$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
  C=\$(curl -s -o /dev/null -w "%{http_code}" -H "X-aws-ec2-metadata-token: \$T" http://169.254.169.254/latest/meta-data/spot/instance-action)
  if [ "\$C" = "200" ]; then
    aws s3 sync /mnt/jkp-data/run_logs "$RUN_PREFIX/run_logs/" --only-show-errors
    aws sns publish --region "$REGION" --topic-arn "$TOPIC" \
      --subject "JKP RUN INTERRUPTED (spot reclaim)" \
      --message "Spot reclaim. Logs flushed to $RUN_PREFIX/run_logs/. The run did NOT finish; the timing measurement is void."
    break
  fi
  sleep 5
done
EOF
chmod +x /usr/local/bin/spot-watch.sh
nohup /usr/local/bin/spot-watch.sh >/var/log/spot-watch.log 2>&1 &

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

notify "JKP RUN: host ready, awaiting credential" \
"Docker installed and image pulled. Waiting for /secure/jkp.env; the pipeline starts the moment it appears."

while [ ! -s /secure/jkp.env ]; do sleep 10; done
chmod 600 /secure/jkp.env
sleep 5

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
  --start-date @@START_DATE@@ \
  --end-date @@END_DATE@@ \
  --production \
  ${WORKERS:+--daily-download-workers $WORKERS} \
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

CSV_DIR=/mnt/jkp-data/processed/production
for c in $COUNTRIES; do
  for freq in monthly daily; do
    [ -f "$CSV_DIR/$freq/$c.csv" ] && aws s3 cp "$CSV_DIR/$freq/$c.csv" "$RUN_PREFIX/production/$freq/$c.csv" --only-show-errors
  done
done
aws s3 ls "$RUN_PREFIX/production/monthly/" > /mnt/jkp-data/UPLOADED 2>&1
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
$(cat /mnt/jkp-data/UPLOADED)
Disk: $(tail -1 /mnt/jkp-data/DISK_FINAL)

Instance still RUNNING for verification. Terminate when done."
else
  notify "JKP RUN: FAILED (exit $RC) after $(cat /mnt/jkp-data/ELAPSED)" \
"Exit $RC. Logs: $RUN_PREFIX/run_logs/ and $RUN_PREFIX/container.log
$(tail -40 /mnt/jkp-data/container.log)

Instance still RUNNING for diagnosis."
fi
