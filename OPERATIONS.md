# Running the pipeline on AWS

How the monthly production build is started, what each script is for, and where
its credentials come from. For the image itself see [DOCKER.md](DOCKER.md).

There is one job: the monthly production build. Everything below serves it.

---

## 1. Which script do I run?

| I want to | Run | Where |
|---|---|---|
| The monthly production build | `scripts/production-run.sh` | your machine, VPN up |
| Check whether it *could* run, change nothing | `scripts/production-run.sh --check-only` | your machine |
| Just the feed-readiness verdict | `uv run python scripts/check_source_ready.py` | your machine |
| Watch a run in flight | `scripts/aws/status.sh <instance-id>` | your machine |
| Store credentials for unattended runs | `scripts/put-production-credentials.sh` | your machine, once |
| Re-launch a month already prepared | `scripts/aws/launch.sh <run-tag>` | your machine |

`production-run.sh` is the entry point. It runs every gate, refreshes
Fama-French, captures identifiers, and *then* calls `launch.sh` for you. Calling
`launch.sh` directly skips all of that — right only when you have already
prepared that month and just need another host.

---

## 2. Where the credentials come from

This is the part with no obvious answer from reading the code, so it is spelled
out in full.

### The three variables

| Variable | What it is | Used by |
|---|---|---|
| `COMPUSTAT` | Connection URL for the private XpressFeed RDS | the pipeline, every gate, both loaders |
| `ENV_USERNAME` | WRDS username | the Fama-French refresh only |
| `ENV_PASSWORD` | WRDS password | the Fama-French refresh only |

Only `COMPUSTAT` is required. Without the WRDS pair the run still completes; it
skips the FF refresh and falls back to the last rate already in the database,
exactly as the production SAS does.

### Today: attended run, credentials start on your machine

```
your machine                          EC2 host                    container
────────────                          ────────                    ─────────
.env  (3 keys)
 │
 ├─ production-run.sh reads it directly for the gates,
 │  the FF refresh and the identifier capture
 │
 └─ launch.sh copies ONLY the COMPUSTAT line
        │
        ▼
    SSM SecureString  /jkp-data/<run-tag>/env
        │  (instance role: ssm:GetParameter + kms:Decrypt via ssm)
        ▼
                              /secure/jkp.env  (0600)
                              then DELETES the parameter
                                    │
                                    └─ docker run --env-file ──▶ os.environ["COMPUSTAT"]
```

The per-run parameter carries `COMPUSTAT` and nothing else, because in this mode
the host never needs WRDS — you already ran the FF refresh locally. The
credential's plaintext never appears in an SSM command, in CloudTrail, or in the
launcher's terminal, and there is no handshake that can stall.

### Unattended: credentials live on AWS, `.env` is only the seed

```
your machine                          EC2 host                    container
────────────                          ────────                    ─────────
.env  (3 keys)
 │
 └─ put-production-credentials.sh   ← run ONCE, or again to rotate
        │
        ▼
    SSM SecureString  /jkp-data/production/env   (all 3 keys, persistent)
        │
        │  read at every boot; the host does NOT delete it
        ▼
                              /secure/jkp.env  (0600)
                                    │
                                    ├─ docker run --env-file ──▶ FF refresh (WRDS)
                                    ├─ docker run --env-file ──▶ identifier capture
                                    └─ docker run --env-file ──▶ pipeline
```

**Answer to "where does production get them if everything sits on AWS":** from
SSM Parameter Store at `/jkp-data/production/env`. Your `.env` seeds it once and
is then irrelevant — no laptop, no VPN and no operator is in the path.

Why Parameter Store and not Secrets Manager: functionally identical here (both
KMS-encrypted, both IAM-scoped, both in CloudTrail). Secrets Manager adds
automatic rotation and resource policies; we use neither, and a WRDS password
cannot be rotated by a Lambda anyway. If rotation ever matters, switching is a
one-line change to the host's fetch plus an IAM statement.

### How the code finds them

Two different mechanisms, worth knowing when something comes back `None`:

- **The pipeline** — `get_xpressfeed_connection_info()` reads `os.environ["COMPUSTAT"]`,
  falling back to a `.env` found by walking up from the working directory.
- **The loaders** (`load_ff_factors.py`, `capture_sec_ids.py`) — `gen_views.load_env()`
  reads the repo-root `.env` if present, then lets the environment override it.
  The runtime image ships no `.env`, so in a container the environment is the
  only source.

### Guarding the persistent parameter

The instance role holds `ssm:GetParameter` and `ssm:DeleteParameter` on
`/jkp-data/*`, with an explicit `Deny` on `ssm:DeleteParameter` for
`/jkp-data/production/*`. A host deleting that parameter would take the *next*
month's run down, and nobody would notice until it failed to start.

```bash
scripts/put-production-credentials.sh --show   # names and lengths, never values
```

---

## 3. Script reference

### `scripts/production-run.sh`

```bash
scripts/production-run.sh                 # gates, prepare sources, launch
scripts/production-run.sh --check-only    # gates only, launch nothing
scripts/production-run.sh prod-20260801   # explicit run tag
scripts/production-run.sh --unattended    # on the run host; prepare, do not launch
```

Requires the aws CLI authenticated to `485357734136`, the VPN up, `.env` at the
repo root, and `uv`. Default run tag is `prod-$(date -u +%Y%m%d)`.

The gates, in order. Every fatal one must pass or nothing launches:

| # | Gate | Fatal? |
|---|---|---|
| 1 | aws CLI authenticated to the right account | yes |
| 2 | `COMPUSTAT` present | yes |
| 2b | `ENV_USERNAME`/`ENV_PASSWORD` present | no — skips the FF refresh |
| 3 | RDS reachable | yes |
| 4 | ECR image newer than the last change to what goes into it | yes |
| 5 | Compustat feed complete for the target month | yes |
| 6 | Run tag free in S3 | yes |
| 7 | No other run in flight | yes |
| 8 | Fama-French vintage | no — reports the fallback |

Then: FF refresh → identifier capture → launch.

`capture_sec_ids.py` **must** run before the build downloads its raw tables. It
appends any identifier changes since the last run to `comp.sec_id_history`,
which is what gives historical rows their point-in-time CUSIP/ISIN/SEDOL instead
of today's value. Skipping it does not fail the build — the run falls back to
the security header — but that month's identifier changes are lost for good,
since the feed only ever exposes the current value.

`--unattended` is the same gates in the same order, run on the host instead of a
laptop: credentials come from the env file the host fetched from SSM, the Python
steps run inside the pulled image, gate 5 becomes a bounded poll, gate 4 is
skipped (the script came *out of* the image it would be checking), gate 7
discounts the host itself, and there is no launch at the end — `user-data.sh`
starts the pipeline once this exits 0.

### `scripts/put-production-credentials.sh`

```bash
scripts/put-production-credentials.sh          # seed /jkp-data/production/env from .env
scripts/put-production-credentials.sh --show   # names and lengths, not values
```

Run once. Run again to rotate — the parameter is overwritten in place and the
next run picks up the new value with no other change.

### `scripts/check_source_ready.py`

```bash
uv run python scripts/check_source_ready.py                    # previous month end
uv run python scripts/check_source_ready.py 2026-07-31         # explicit target
uv run python scripts/check_source_ready.py --wait-minutes 60  # poll until ready
```

Exit 0 = ready. Four gates: daily prices past month end, FX past month end,
monthly universe ≥95% of the prior month, daily trading days ≥95% of the prior
month.

It deliberately does **not** trust `max(datadate)` on `sec_mth`: that table
stamps rows with the month-end date as they arrive, so it reports the current
month end from the 1st while holding a fraction of the universe. Trusting it
would silently build a month with most securities missing.

`--wait-minutes` is a deadline, not an attempt count — a slow query eats the
budget rather than pushing past the hour you allowed. Defaults to 0 (single
shot).

### `scripts/aws/launch.sh`

```bash
scripts/aws/launch.sh [RUN_TAG]
```

Idempotent: reuses the bucket, IAM role/profile and security group if they
exist. Blocks only on capacity; everything after that the host does itself and
reports by email. Default run tag `prod-<UTC date>`, and it refuses to start if
that tag already has objects in S3 — pass a distinct tag for a same-day re-run.

Email arrives on start, on completion (with elapsed time), on failure, and on
bootstrap failure. **A missing "started" email means the host never got that
far** — the bootstrap fails loudly rather than idling, so look for
`JKP RUN: BOOTSTRAP FAILED`, which carries the reason.

### `scripts/aws/status.sh`

```bash
scripts/aws/status.sh <instance-id>
```

Step totals sum only depth-0 rows — nested steps would be double-counted. For
authoritative phase durations read `phase_timings_seconds` in `run_summary.json`.

---

## 4. Parameters

All are environment variables read by `launch.sh`. Every default is the
production-correct value; override only for a reason.

| Variable | Default | Meaning |
|---|---|---|
| `COUNTRIES` | *(empty)* | Empty uploads every country. A space-separated list uploads only those; the six cross-country files still ship. |
| `START_DATE` | *(empty)* | Empty gives the rolling `ROLLING_INPUT_YEARS` (23) window. |
| `END_DATE` | `2026-06-30` | Last month end to build. |
| `WORKERS` | *(empty)* | Empty uses `config.DAILY_DOWNLOAD_WORKERS` (8). |
| `KEEP_INTERIM` | `1` | Keeps `interim/` and `raw/` and ships the accounting artefacts to S3. |
| `CREDENTIAL_PARAM` | *(empty)* | Empty stages a per-run secret from `.env` and the host deletes it. Set to an existing SSM path and the host keeps it. |
| `UNATTENDED` | `0` | `1` runs `production-run.sh --unattended` on the host before the pipeline. |
| `MARKET` | `ondemand` | `spot` is available for a throwaway re-run; a reclaim on a delivery loses the run. |
| `INSTANCE_TYPE` | `r7i.16xlarge` | 64 vCPU / 512 GiB. Cheaper *and* faster than `m6a.32xlarge` — see below. Do not move to `m7i.16xlarge`: 256 GiB against a ~351 GiB peak, so it OOMs. |
| `VOLUME_GB` / `VOLUME_IOPS` / `VOLUME_MBPS` | `750` / `8000` / `1000` | gp3 root volume. Peak disk was 471–485 GiB, so do not go below 750. |

`production-run.sh --unattended` additionally reads `RUN_TAG`, `IMAGE` and
`ENV_FILE` (default `/secure/jkp.env`), all set by `user-data.sh`.

### Why `r7i.16xlarge` and not something with more cores

Counter-intuitive, so it is worth stating: the 64-vCPU `r7i.16xlarge` beats the
128-vCPU `m6a.32xlarge` on both time and money.

| | on demand | vCPU | RAM | run | cost/run |
|---|---:|---:|---:|---:|---:|
| `r7i.16xlarge` | $5.107/h | 64 | 512 GiB | 1h55m | **$9.80** |
| `m6a.32xlarge` | $6.624/h | 128 | 512 GiB | ~2h30m | $16.60 |

Only `source_download` scales with core count. The middle phases —
`security_panels`, `market_returns`, `accounting_characteristics` — are lightly
threaded and run at single-digit CPU, so they are bound by per-core speed, where
Sapphire Rapids beats EPYC decisively: `security_panels` 760s against ~1,300s,
`market_returns` 449s against ~770s.

`m6a.32xlarge` was correct only while `MARKET` defaulted to `spot`, where its
~77% discount beat r7i's ~49%. On demand that inverts. **If you ever switch back
to spot, re-check both prices before assuming the type follows** — at the time of
writing spot is $1.53/h for m6a against $2.64/h for r7i, which reverses the
ranking again.

Notes on the two easiest to get wrong:

- **`START_DATE`.** 23 years is the shortest window that nulls no
  characteristic: `seas_16_20` needs 240 monthly *observations*, and its gate
  counts a security's own rows rather than calendar months, so gappy securities
  need more than 20 calendar years to reach 240. A shorter window silently nulls
  the seasonality columns (the pipeline warns). For a complete-history build —
  re-seeding a downstream store, or reissuing after a change that rewrites
  history — pass `jkp build --full-history` rather than guessing a date.
- **`KEEP_INTERIM`.** Without it, `save_full_files_and_cleanup` wipes `interim/`
  and `raw/` at the end of the run and there is nothing left to upload. The host
  probes the image for the flag first, so an older image degrades to the
  `processed/` copies rather than failing.

---

## 5. Output

`processed/production/` is the entire deliverable:

```
processed/production/
    monthly/<country>.csv          # 455-column characteristics
    daily/<country>.csv            # returns, prices, identifiers
    market_returns.csv  market_returns_daily.csv
    nyse_cutoffs.csv    return_cutoffs.csv  return_cutoffs_daily.csv
    world_ret_monthly.csv
```

The per-country CSVs carry `config.PRODUCTION_OUTPUT_YEARS` (3) of history, not
the whole panel — the loader downstream only reads rows past its own high-water
mark, so the rest was written and shipped unread. `jkp build --production-years 0`
emits everything for a re-seed. This bounds the *output* only: characteristics
are computed over the full source window either way, so the retained rows are
identical. The six cross-country files keep full history.

After the completion email:

```bash
aws s3 sync s3://jkp-data-runs-485357734136-eu-central-1/<run-tag>/ \
    "D:/jkp-run-<date>/" --exclude "production/*"        # logs only
aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0abc...
```

**Terminating destroys the volume.** By default the whole of
`processed/production/` reaches S3 first (~12 GiB with the 3-year window,
against ~95 GiB unbounded). Whatever was not uploaded — the remaining CSVs and
all parquet output — is lost, and recovering it means a full rerun.

**The sasWrds uploader needs updating to match.** Its `Folder` enum walks
`CharacteristicsProduction/` and `DailyReturnsProduction/`; the layout above uses
`monthly/` and `daily/`. `FileFactory` returns `None` for unrecognised names and
`WrdsUpdater` logs nothing, so anything unmatched is skipped silently while the
job still reports success — point `FILE_PATH` at `processed/production/` and
rename the two folder constants.

---

## 6. Fixed infrastructure

| | |
|---|---|
| Account / region | `485357734136` / `eu-central-1` |
| Image | `485357734136.dkr.ecr.eu-central-1.amazonaws.com/jkp-data:production` |
| VPC / subnet | `vpc-0712bd9cff9966754` / `subnet-0a74f7b1e230a2f12` (eu-central-1b) |
| Security group | `jkp-data-run-sg` — egress only, no inbound |
| IAM | role `jkp-data-run-role`, profile `jkp-data-run-profile` |
| S3 | `jkp-data-runs-485357734136-eu-central-1` (AES256, public access blocked) |
| SNS | `jkp-spot-run-alerts` → serhii@alpha-beta.co.il |
| Credential (unattended) | SSM SecureString `/jkp-data/production/env` |
| Instance tag | `Purpose=jkp-data-production` — the in-flight interlock filters on it |

**The subnet is not optional.** The Compustat RDS has no public endpoint; its
security group admits `10.10.0.0/16` on 5432, so the instance must sit in that
VPC. This subnet is in the RDS's AZ *and* routes outbound via the NAT gateway,
which the ECR pull and S3 uploads require. Host access is SSM only — no key
pair, no public IP, nothing inbound.

---

## 7. How the image and the scripts stay in step

`check_source_ready.py`, `production-run.sh` and `sql/` are baked into the
runtime image at `/opt/jkp`. The host lifts them out with `docker cp` rather
than cloning the repo, so the scripts that gate a build are always the same
commit as the build they gate. The image carries
`org.opencontainers.image.revision`, and the host logs it.

Gate 4 exists because a launcher newer than its image is the dangerous
combination: `launch.sh` stopped passing `--start-date` when the rolling window
landed, so an image predating that change falls back to `ACCOUNTING_START_DATE`
and downloads from 1949 instead of 23 years. The gate compares the image's push
time against the last commit touching the paths that trigger a rebuild;
`tests/unit/test_unattended_run_wiring.py` asserts those two path lists match.

A push to `compustat_migration` or `main` touching `src/`, `sql/`, either baked
script, `Dockerfile`, `.dockerignore`, `pyproject.toml`, `uv.lock` or
`.python-version` rebuilds and republishes automatically.

---

## 8. Gotchas

- **Git Bash mangles paths.** `MSYS_NO_PATHCONV=1` is required for
  `--block-device-mappings ... /dev/xvda` and for `docker run --entrypoint
  /app/.venv/bin/python`; otherwise they become `C:/Program Files/Git/...`.
  The scripts export it. The same trap bit `production-run.sh`, which must pass
  script paths *relative to the repo* because a Git Bash `/d/projects/...` path
  is not something native `python.exe` can open.
- **Don't inline Python in `ssm send-command`** — nested quotes break the JSON
  parameter parser. Base64-encode the script and decode on the host.
- **`ff.factors_monthly` lags roughly a month.** The last month of a run uses
  the last-available-RF fallback; the run records what it used in
  `source_snapshot_manifest.json`. `production-run.sh` refreshes it and reports
  the vintage, but Ken French genuinely publishes late — the fallback is not a
  bug.
- **A running spot instance cannot become on-demand.** The market type is fixed
  at launch and there is no conversion API; recovery always means a new
  instance. `stop`/`hibernate` interruption behaviours do not help — cloud-init
  runs user-data once per instance, `jkp build` has no mid-phase resume, and
  hibernate caps at 150 GiB of RAM. On reclaim the host flushes `run_logs` and
  emails; 200+ GB cannot leave in 120 seconds. This is why `MARKET` defaults to
  `ondemand`.
- **The spot discount is very type-specific**, if you do opt into it. In
  eu-central-1b: `m6a.32xlarge` was ~77% off on demand but `r7i.16xlarge` only
  ~49%. Check `describe-spot-price-history` rather than assuming.

---

## 9. What is not automated

The build is one command; nothing runs on a schedule yet. Still manual:

- **Starting the run.** No EventBridge trigger, no Launch Template, no
  self-termination. `UNATTENDED=1` and the persistent credential exist and work,
  but something still has to call `launch.sh`.
- **Terminating the instance.** The host stays up after a run, deliberately, so
  the volume can be inspected. Terminate it when you are done — a 750 GB volume
  on a stopped instance still costs ~$60/month.
- **Downloading the output.** `aws s3 sync s3://<bucket>/<run-tag>/production/ …`
- **Loading the database.** Separate flow, separate repo.
