# EC2 full-run timing benchmark

Runs the whole pipeline on a fresh EC2 host — download included, no raw reuse —
and reports wall-clock time from container start through the last production CSV.
Used to validate performance changes and to check parity fixes against
`research.dbo.characteristicsproduction`.

## Rerun

```bash
scripts/ec2-benchmark/launch.sh run-20260801
scripts/ec2-benchmark/status.sh i-0abc...          # progress, any time
```

`launch.sh` is idempotent: it reuses the bucket, IAM role/profile and security
group if they exist. It stages the `COMPUSTAT` credential as an SSM SecureString,
has the instance read it once, then deletes the parameter and revokes the grant —
so no plaintext lands in SSM command history or CloudTrail. Email arrives on
start, on completion (with elapsed time), on failure, and on spot reclaim.

Overrides (environment variables): `INSTANCE_TYPE`, `VOLUME_GB`, `VOLUME_IOPS`,
`VOLUME_MBPS`, `WORKERS`, `START_DATE`, `END_DATE`, `COUNTRIES`, `MARKET=ondemand`.

After the completion email:

```bash
aws s3 sync s3://jkp-data-runs-485357734136-eu-central-1/<run-tag>/ \
    "D:/jkp-full-run-<date>/" --exclude "production/*"        # logs only
aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0abc...
```

Terminating destroys the volume. Only the countries in `COUNTRIES` reach S3 —
every other country's CSV and all parquet output is lost, and recovering them
means a full rerun.

## Fixed infrastructure

| | |
|---|---|
| Account / region | 485357734136 / eu-central-1 |
| Image | `485357734136.dkr.ecr.eu-central-1.amazonaws.com/alphabeta:jkp-data-production` |
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

Both runs: `--start-date 2000-01-01 --end-date 2026-06-30 --production`, fresh
download, 128 vCPU / ~500 GiB RAM.

| Phase (s) | 2026-07-23 | 2026-07-26 | |
|---|---:|---:|---|
| source_download | 5,673 | **3,343** | 2→4 workers |
| security_panels | 1,329 | 1,334 | ASOF exchange join added, no cost |
| market_returns | 760 | 776 | |
| accounting_characteristics | 2,110 | 1,860 | |
| factor_models | 324 | 272 | |
| daily_characteristics | 1,333 | 1,522 | slower: corrections removed, more rows survive |
| final_outputs | 1,274 | 1,119 | |
| **total** | **12,804 (3h33m)** | **10,252 (2h51m)** | |

Config differences: 2026-07-23 ran 2 download workers on 1000 GiB gp3 at default
throughput; 2026-07-26 ran 4 workers on 750 GiB gp3 at 8000 IOPS / 1000 MB/s.

Download scaling was near-linear — 838 batches in 44.0 min versus 837 in 83.2 min,
1.60 → 3.03 MiB/s, saturation exactly 4.00×/4 workers, zero retries or timeouts
either time. Peak iowait fell 73.9% → 48.8%. Peak RAM 318 → 375 GiB and peak disk
443 → 485 GiB (more observations survive without the corrections layer), so keep
the volume at 750 GiB or above.

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
- Spot in eu-central-1b was ~$1.475/hr against ~$6.60 on-demand; a full run costs
  roughly $4–6 on spot. A reclaim voids the timing measurement.
