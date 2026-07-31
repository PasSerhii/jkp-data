# EC2 full-run timing benchmark

Runs the whole pipeline on a fresh EC2 host — download included, no raw reuse —
and reports wall-clock time from container start through the last production CSV.
Used to validate performance changes and to check parity fixes against
`research.dbo.characteristicsproduction`.

**This is the benchmark entry point, not the production one.** For a monthly
production build use `scripts/production-run.sh`, which runs every gate,
refreshes Fama-French, captures identifiers, and then calls `launch.sh` for you.
See [OPERATIONS.md](../../OPERATIONS.md).

## Rerun

```bash
uv run python scripts/check_source_ready.py                          # feed complete? only in production not for test runs
uv run --with "psycopg[binary]" python sql/xpressfeed_views/capture_sec_ids.py
scripts/ec2-benchmark/launch.sh run-20260801
scripts/ec2-benchmark/status.sh i-0abc...                            # progress, any time
```

`capture_sec_ids.py` must run **before** the build downloads its raw tables. It
appends any identifier changes since the last run to `comp.sec_id_history`, which
is what gives historical rows their point-in-time CUSIP/ISIN/SEDOL instead of
today's value. Skipping it does not fail the build — the run falls back to the
security header — but that month's identifier changes are then lost for good,
since the feed only ever exposes the current value.

`launch.sh` is idempotent: it reuses the bucket, IAM role/profile and security
group if they exist. It writes the `COMPUSTAT` credential to an SSM SecureString
at `/jkp-data/<run-tag>/env`, launches the instance, and returns — it does not
wait for the run to begin. The host then reads that parameter itself through its
instance role and deletes it, so the credential's plaintext never appears in an
SSM command, in CloudTrail, or in the launcher's terminal, and there is no
handshake that can stall. The instance role holds a standing `ssm:GetParameter`
/`ssm:DeleteParameter` grant scoped to `/jkp-data/*`.

Email arrives on start, on completion (with elapsed time), on failure, on spot
reclaim, and on bootstrap failure. **A missing "started" email means the host
never got that far** — the bootstrap fails loudly rather than idling, so check
for `JKP RUN: BOOTSTRAP FAILED`, which carries the reason.

Overrides (environment variables): `INSTANCE_TYPE`, `VOLUME_GB`, `VOLUME_IOPS`,
`VOLUME_MBPS`, `WORKERS`, `START_DATE`, `END_DATE`, `COUNTRIES`, `MARKET=ondemand`,
`KEEP_INTERIM=0`, `CREDENTIAL_PARAM`, `UNATTENDED=1`.

`CREDENTIAL_PARAM` points the run at an existing SSM SecureString instead of
staging one from `.env`. The host then *keeps* the parameter after reading it,
which is what makes a run possible with no `.env` and no operator anywhere:

```bash
scripts/put-production-credentials.sh          # once, writes /jkp-data/production/env
CREDENTIAL_PARAM=/jkp-data/production/env MARKET=ondemand \
  scripts/ec2-benchmark/launch.sh prod-20260901
```

That parameter holds `COMPUSTAT` plus the `ENV_USERNAME`/`ENV_PASSWORD` pair the
Fama-French refresh needs against WRDS. An IAM `Deny` keeps the instance role
from deleting anything under `/jkp-data/production/*`, so a host cannot take the
next month's run down with it.

`UNATTENDED=1` runs `scripts/production-run.sh --unattended` on the host before
the pipeline: the feed-readiness poll (up to 60 minutes, every 10), the
Fama-French refresh, and the identifier capture. Attended runs do those on the
operator's machine, which is why the default is `0`. The host takes those scripts
out of the image with `docker cp` rather than cloning the repo, so the scripts
that gate a build are always the same commit as the build they gate.

`KEEP_INTERIM` defaults to `1`, passing `--keep-interim` so the run leaves
`interim/` and `raw/` on the volume instead of deleting them, and ships the
accounting artefacts to S3 (`accounting_data/`, `other_output/`, and the
`acc_std_*`/`*chars_world` interim parquets). Without it the pipeline's
`save_full_files_and_cleanup` wipes both directories, and there is nothing left to
upload. The host probes the image for the flag first, so an older image degrades
to the `processed/` copies rather than failing.

Leave `START_DATE` unset too. The pipeline then downloads a rolling
`config.ROLLING_INPUT_YEARS` (23) years back from the end date, which keeps the
run's cost flat instead of growing a year every year. 23 is the shortest window
that nulls no characteristic: `seas_16_20` needs 240 monthly *observations*, and
its gate counts a security's own rows rather than calendar months, so gappy
securities need more than 20 calendar years to reach 240. Pin `START_DATE` only
to reproduce an older run — a window under 240 months silently nulls the
seasonality columns, and the pipeline now warns when that happens. For a
complete-history build (re-seeding a downstream store, or reissuing after a
change that rewrites history) pass `jkp build --full-history` rather than
guessing a start date; it uses `config.ACCOUNTING_START_DATE`.

Leave `WORKERS` unset unless you are A/B testing a worker count:
`--daily-download-workers` overrides `config.DAILY_DOWNLOAD_WORKERS`, so setting
it pins the run to that number and the config is ignored.

`RUN_TAG` defaults to `run-<UTC date>`, so a second launch on the same day
overwrites the first in S3 without warning. Pass a distinct tag for reruns.

After the completion email:

```bash
aws s3 sync s3://jkp-data-runs-485357734136-eu-central-1/<run-tag>/ \
    "D:/jkp-full-run-<date>/" --exclude "production/*"        # logs only
aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0abc...
```

Terminating destroys the volume. By default the whole of `processed/production/`
reaches S3 first (~12 GiB with the 3-year output window, against ~95 GiB
unbounded); set `COUNTRIES` to a space-separated list to upload only those
countries, in which case the six cross-country files still ship. Whatever is not uploaded — the remaining CSVs and
all parquet output — is lost, and recovering it means a full rerun.

The per-country CSVs carry `config.PRODUCTION_OUTPUT_YEARS` (3) of history, not the
whole panel — the loader downstream only reads rows past its own high-water mark,
so the rest was written and shipped unread. `jkp build --production-years 0` emits
everything for a re-seed. This bounds the output only: characteristics are computed
over the full source window either way, so the retained rows are identical. The six
cross-country files keep full history.

`processed/production/` is the entire production deliverable:

```
processed/production/
    monthly/<country>.csv          # 455-column characteristics
    daily/<country>.csv            # returns, prices, identifiers
    market_returns.csv  market_returns_daily.csv
    nyse_cutoffs.csv    return_cutoffs.csv  return_cutoffs_daily.csv
    world_ret_monthly.csv
```

The former `processed/output/` tree is gone. It held a byte-identical second copy
of every country CSV — ~95 GiB per run, about half the export phase — purely to
present the SAS directory shape.

**The sasWrds uploader needs updating to match.** Its `Folder` enum walks
`CharacteristicsProduction/` and `DailyReturnsProduction/`; the layout above uses
`monthly/` and `daily/`. It also expects an `fx.csv` this pipeline has never
written. `FileFactory` returns `None` for unrecognised names and `WrdsUpdater` logs
nothing, so anything unmatched is skipped silently while the job still reports
success — point `FILE_PATH` at `processed/production/` and rename the two folder
constants.

## Fixed infrastructure

| | |
|---|---|
| Account / region | 485357734136 / eu-central-1 |
| Image | `485357734136.dkr.ecr.eu-central-1.amazonaws.com/jkp-data:production` |
| VPC / subnet | `vpc-0712bd9cff9966754` / `subnet-0a74f7b1e230a2f12` (eu-central-1b) |
| Security group | `jkp-data-run-sg` — egress only, no inbound |
| IAM | role `jkp-data-run-role`, profile `jkp-data-run-profile` |
| S3 | `jkp-data-runs-485357734136-eu-central-1` (AES256, public access blocked) |
| SNS | `jkp-spot-run-alerts` → serhii@alpha-beta.co.il (confirmed) |

**The subnet is not optional.** The Compustat RDS has no public endpoint; its
security group admits `10.10.0.0/16` on 5432, so the instance must sit in that
VPC. This subnet is in the RDS's AZ *and* routes outbound via the NAT gateway,
which the ECR pull and S3 uploads require. Host access is SSM only — no key pair,
no public IP, nothing inbound.

## Baselines

All three runs below predate the rolling window and pinned
`--start-date 2000-01-01`; a run today covers 23 years rather than 26, so expect
a shorter download than these figures. All: `--end-date 2026-06-30 --production`, fresh
download. The first two on 128 vCPU / ~500 GiB (`m6a.32xlarge`); 2026-07-29 on
64 vCPU / ~500 GiB (`r7i.16xlarge`), so its per-phase figures are not directly
comparable — only the total is.

| Phase (s) | 2026-07-23 | 2026-07-26 | 2026-07-29 | |
|---|---:|---:|---:|---|
| source_download | 5,673 | 3,343 | **2,564** | 2→4→8 workers |
| security_panels | 1,329 | 1,334 | 760 | ASOF exchange join added, no cost |
| market_returns | 760 | 776 | 449 | |
| accounting_characteristics | 2,110 | 1,860 | 1,131 | |
| factor_models | 324 | 272 | 229 | |
| daily_characteristics | 1,333 | 1,522 | 769 | |
| final_outputs | 1,274 | 1,119 | 987 | |
| **total** | **12,804 (3h33m)** | **10,252 (2h51m)** | **6,891 (1h55m)** | |

Config differences: 2026-07-23 ran 2 download workers on 1000 GiB gp3 at default
throughput; 2026-07-26 ran 4 workers on 750 GiB gp3 at 8000 IOPS / 1000 MB/s;
2026-07-29 ran 8 workers on the same volume, on half the vCPUs.

Download scaling was near-linear — 838 batches in 44.0 min versus 837 in 83.2 min,
1.60 → 3.03 MiB/s, saturation exactly 4.00×/4 workers, zero retries or timeouts
either time. Peak iowait fell 73.9% → 48.8% → 40.5%. Peak RAM 318 → 375 → 351 GiB
and peak disk 443 → 485 → 471 GiB, so keep the volume at 750 GiB or above.

The authoritative phase figures are `phase_timings_seconds` in `run_summary.json`;
they reconcile exactly with `elapsed_seconds` (6,891s on 2026-07-29 — the 6,942s in
`ELAPSED` additionally covers container start and teardown).

The 2026-07-29 column above is corrected. That run's `status.sh` reported
`daily_characteristics` 203s and `final_outputs` 1,582s, both wrong: it summed
every `step_timings.csv` row, so nested steps were counted twice
(`export_production` contains five children) while the 19 `roll_apply_daily` jobs
were counted not at all — they run on `ThreadPoolExecutor` threads, where the
`_ACTIVE_MONITOR` ContextVar did not propagate, so `measure_time` fell back to
plain prints and recorded no step. The two errors nearly cancelled in the total,
which made the aggregate look healthy.

Both are fixed: the fan-out copies its context into each worker and records the 19
children under a `rolling_daily_fanout` parent, `step_timings.csv` carries
`parent_step_token` and `step_depth`, and `status.sh` sums only depth-0 rows.

## Output verification

```bash
python3 scripts/ec2-benchmark/verify_output.py     # on the host, before terminating
```

Checks the parity fixes in the delivered CSVs. Expected (2026-07-26, versus
Research):

| Check | Expected | Was broken as |
|---|---|---|
| Germany universe | 761 May / 761 Jun (Research 760) | 583–585 |
| USA `me_company` ≠ `me` | 981 rows (Research 975) | 0 |
| Missing `conm` on 2026-05-31 | 0 | 343 |
| `ret_local_lead1m` populated | yes | 100% null |

Remaining nulls in `ret_local_lead1m` are correct — the continuity guard fires on
discontinuous series.

## Gotchas

- **Git Bash mangles paths.** `MSYS_NO_PATHCONV=1` is required for
  `--block-device-mappings ... /dev/xvda` and for `docker run --entrypoint
  /app/.venv/bin/python`; otherwise they become `C:/Program Files/Git/...`.
  Both scripts export it.
- **Don't inline Python in `ssm send-command`** — nested quotes break the JSON
  parameter parser. Base64-encode the script and decode on the host.
- **Check `ff.factors_monthly` freshness** before a run
  (`sql/xpressfeed_views/load_ff_factors.py`). It lags roughly a month, so the
  last month of a run uses the last-available-RF fallback; the run records what
  it used in `source_snapshot_manifest.json`.
- **Spot for tests, on demand for production.** This kit runs timing benchmarks,
  so `MARKET` defaults to `spot`; a reclaim there costs a rerun. Production runs
  pass `MARKET=ondemand`, where a reclaim would cost a delivery. A reclaim also
  voids the timing measurement outright, so re-measure rather than reporting a
  partial run.
- **The spot discount is very type-specific — check before switching.** In
  eu-central-1b (2026-07-29): `m6a.32xlarge` $1.494 spot vs $6.624 on demand
  (~77% off, 128 vCPU), but `r7i.16xlarge` only $2.604 vs $5.107 (~49% off,
  64 vCPU). m6a is both cheaper per hour *and* twice the cores, which is why it is
  the default. `m7i.16xlarge` is cheaper still but carries only 256 GiB against a
  351 GiB peak, so it OOMs. Verify with:

  ```bash
  aws ec2 describe-spot-price-history --region eu-central-1 \
      --instance-types m6a.32xlarge --product-descriptions "Linux/UNIX" \
      --start-time "$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S)" \
      --query "SpotPriceHistory[].{az:AvailabilityZone,price:SpotPrice}" --output text
  ```
- **A running spot instance cannot become on-demand.** The market type is fixed at
  launch and there is no conversion API; recovery always means a new instance.
  `stop`/`hibernate` interruption behaviours do not help here — cloud-init runs
  user-data once per instance, `jkp build` has no mid-phase resume, and hibernate
  caps at 150 GiB of RAM. On reclaim the host flushes `run_logs` and emails; the
  200+ GB of output cannot leave in 120 seconds, so nothing else is recoverable.
  On spot the host also watches the rebalance recommendation, which usually
  precedes the 2-minute notice and is the cue to relaunch on demand.
