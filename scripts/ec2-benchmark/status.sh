#!/usr/bin/env bash
# Progress snapshot for a running benchmark: scripts/ec2-benchmark/status.sh <instance-id>
#
# Step totals sum only depth-0 rows. Nested steps (export_production contains five
# children; rolling_daily_fanout contains nineteen) would otherwise be counted
# twice, and the concurrent rolling children sum to more wall clock than exists.
# For authoritative phase durations read phase_timings_seconds in run_summary.json.
set -euo pipefail
export MSYS_NO_PATHCONV=1
INSTANCE="${1:?usage: status.sh <instance-id>}"
REGION=eu-central-1

CID=$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE" \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["echo NOW=$(date -u +%H:%M:%SZ) START=$(cat /mnt/jkp-data/STARTED_AT 2>/dev/null)","echo ELAPSED=$(cat /mnt/jkp-data/ELAPSED 2>/dev/null || echo running)","docker ps -a --filter name=jkp-run --format \"{{.Status}}\"","grep HEARTBEAT /mnt/jkp-data/container.log 2>/dev/null | tail -1","grep -v HEARTBEAT /mnt/jkp-data/container.log 2>/dev/null | tail -3","echo \"--- phase totals (top-level steps only) ---\"","awk -F, \"NR>1 && \\$6!=\\\"\\\" && (\\$11==\\\"\\\" || \\$11==0) {s[\\$2]+=\\$6} END {for (p in s) printf \\\"%-28s %7.0fs\\n\\\", p, s[p]}\" /mnt/jkp-data/run_logs/*/step_timings.csv 2>/dev/null | sort -k2 -rn","echo \"--- rolling fan-out children ---\"","awk -F, \"NR>1 && \\$3 ~ /^roll_apply_daily\\\\[/ {n++; s+=\\$6} END {printf \\\"%d of 19 done, %.0fs of thread time\\n\\\", n, s}\" /mnt/jkp-data/run_logs/*/step_timings.csv 2>/dev/null","df -h /mnt/jkp-data | tail -1"]' \
  --query "Command.CommandId" --output text)
sleep 18
aws ssm get-command-invocation --region "$REGION" --command-id "$CID" \
  --instance-id "$INSTANCE" --query StandardOutputContent --output text
