# Running the pipeline on AWS

How the monthly production build is started, what each script is for, and where
its credentials come from. For the image itself see [DOCKER.md](DOCKER.md); for
benchmark-specific detail (instance sizing, baselines, spot behaviour) see
[scripts/ec2-benchmark/README.md](scripts/ec2-benchmark/README.md).

---

## 1. Which script do I run?

| I want to | Run | Where |
|---|---|---|
| The monthly production build | `scripts/production-run.sh` | your machine, VPN up |
| Check whether it *could* run, change nothing | `scripts/production-run.sh --check-only` | your machine |
| Just the feed-readiness verdict | `uv run python scripts/check_source_ready.py` | your machine |
| A timing benchmark on spot | `scripts/ec2-benchmark/launch.sh run-YYYYMMDD` | your machine |
| Watch a run in flight | `scripts/ec2-benchmark/status.sh <instance-id>` | your machine |
| Store credentials for unattended runs | `scripts/put-production-credentials.sh` | your machine, once |

`production-run.sh` is the only entry point you need for a production build. It
runs every gate, refreshes Fama-French, captures identifiers, and *then* calls
`launch.sh` for you. Calling `launch.sh` directly skips all of that — it is the
benchmark entry point, not the production one.

---

## 2. Where the credentials come from

This is the part that has no obvious answer from reading the code, so it is
spelled out in full.

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
the host never needs WRDS — you already ran the FF refresh locally.

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
cannot be rotated by a Lambda anyway. Parameter Store is also what the kit
already used. If rotation ever matters, switching is a one-line change to the
host's fetch plus an IAM statement.

### How the code finds them

Two different mechanisms, which is worth knowing when something is `None`:

- **The pipeline** — `get_xpressfeed_connection_info()` reads `os.environ["COMPUSTAT"]`,
  falling back to a `.env` file found by walking up from the working directory.
- **The loaders** (`load_ff_factors.py`, `capture_sec_ids.py`) — `gen_views.load_env()`
  reads the repo-root `.env` if present, then lets the environment override it.
  The runtime image ships no `.env`, so in a container the environment is the
  only source.

### Guarding the persistent parameter

The instance role holds `ssm:GetParameter` and `ssm:DeleteParameter` on
`/jkp-data/*`, with an explicit `Deny` on `ssm:DeleteParameter` for
`/jkp-data/production/*`. A host deleting that parameter would take the *next*
month's run down, and nobody would notice until it failed to start.

Inspect what is stored (names and lengths only, never values):

```bash
scripts/put-production-credentials.sh --show
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

Then: FF refresh → identifier capture → launch. The capture **must** precede the
download; skipping it loses that month's identifier changes permanently, because
the feed exposes only current values.

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
shot), so existing callers are unaffected.

### `scripts/ec2-benchmark/launch.sh`

```bash
scripts/ec2-benchmark/launch.sh [RUN_TAG]
```

Idempotent: reuses the bucket, IAM role/profile and security group if they
exist. Blocks only on spot capacity; everything after that the host does itself
and reports by email.

### `scripts/ec2-benchmark/status.sh`

```bash
scripts/ec2-benchmark/status.sh <instance-id>
```

Step totals sum only depth-0 rows — nested steps would be double-counted. For
authoritative phase durations read `phase_timings_seconds` in `run_summary.json`.

### `scripts/ec2-benchmark/verify_output.py`

An ad-hoc post-run spot check, not a gate: it hardcodes three countries and the
June 2026 month ends. Copy it to the host and edit the constants when you want
that particular check. Nothing in the pipeline calls it.

---

## 4. Parameters

All are environment variables read by `launch.sh`. Every default is the
production-correct value; override only for a reason.

| Variable | Default | Meaning |
|---|---|---|
| `MARKET` | `spot` | `ondemand` for production — a reclaim costs a delivery. `production-run.sh` sets this for you. |
| `COUNTRIES` | *(empty)* | Empty uploads every country (~95 GiB). A space-separated list uploads only those; the rest die with the volume. |
| `START_DATE` | *(empty)* | Empty gives the rolling `ROLLING_INPUT_YEARS` (23) window. Pin only to reproduce an older run — under 240 months silently nulls the seasonality columns. |
| `END_DATE` | `2026-06-30` | Last month end to build. |
| `WORKERS` | *(empty)* | Empty uses `config.DAILY_DOWNLOAD_WORKERS` (8). Set only to A/B a count. |
| `KEEP_INTERIM` | `1` | Keeps `interim/` and `raw/` and ships the accounting artefacts to S3. |
| `CREDENTIAL_PARAM` | *(empty)* | Empty stages a per-run secret from `.env` and the host deletes it. Set to an existing SSM path and the host keeps it. |
| `UNATTENDED` | `0` | `1` runs `production-run.sh --unattended` on the host before the pipeline. |
| `INSTANCE_TYPE` | `m6a.32xlarge` | 512 GiB, 128 vCPU. |
| `VOLUME_GB` / `VOLUME_IOPS` / `VOLUME_MBPS` | `750` / `8000` / `1000` | gp3 root volume. |

`production-run.sh --unattended` additionally reads `RUN_TAG`, `IMAGE` and
`ENV_FILE` (default `/secure/jkp.env`), all set by `user-data.sh`.

---

## 5. Fixed infrastructure

| | |
|---|---|
| Account / region | `485357734136` / `eu-central-1` |
| Artifact bucket | `jkp-data-runs-485357734136-eu-central-1` |
| Image | `485357734136.dkr.ecr.eu-central-1.amazonaws.com/jkp-data:production` |
| Alerts topic | `arn:aws:sns:eu-central-1:485357734136:jkp-spot-run-alerts` |
| Instance role / profile | `jkp-data-run-role` / `jkp-data-run-profile` |
| VPC / subnet | `vpc-0712bd9cff9966754` / `subnet-0a74f7b1e230a2f12` |
| Credential (unattended) | SSM SecureString `/jkp-data/production/env` |

The subnet is in the RDS's AZ and routes outbound through the NAT gateway, which
the ECR pull and the S3 uploads need. The RDS has no public endpoint, so the
instance must sit in that VPC.

---

## 6. How the image and the scripts stay in step

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

## 7. What is not automated

The build is one command; nothing runs on a schedule yet. Still manual:

- **Starting the run.** No EventBridge trigger, no Launch Template, no
  self-termination. `UNATTENDED=1` and the persistent credential exist and work,
  but something still has to call `launch.sh`.
- **Terminating the instance.** The host stays up after a run, deliberately, so
  the volume can be inspected. Terminate it when you are done — a 750 GB volume
  on a stopped instance still costs ~$60/month.
- **Downloading the output.** `aws s3 sync s3://<bucket>/<run-tag>/production/ …`
- **Loading the database.** Separate flow, separate repo.
