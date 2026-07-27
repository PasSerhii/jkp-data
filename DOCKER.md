# Docker and AWS ECR

The image runs the `jkp` CLI. It does not contain credentials, licensed source
data, or generated output. Supply credentials at runtime and mount durable
storage at `/data`.

The build is multi-stage: a builder resolves the virtualenv with `uv`, and the
runtime stage copies only the finished `/app/.venv`. Neither `uv` nor the
`src/` tree ships in the final image — the package is installed non-editable,
so its `resources/` data lives inside the virtualenv.

## Build and test locally

From the repository root in PowerShell:

```powershell
docker build --platform linux/amd64 --provenance=false --sbom=false -t jkp-data:production .
docker run --rm jkp-data:production --version
docker run --rm --entrypoint /app/.venv/bin/python jkp-data:production `
  -c "import duckdb; c=duckdb.connect(); c.execute('LOAD postgres'); print('postgres extension OK')"
```

There is one image and one name: `jkp-data:production`. Rebuilding replaces it.

Use `linux/amd64` for an Intel/AMD EC2 instance. Use `linux/arm64` instead for
an AWS Graviton instance, and build/test that platform before scheduling a job.

## Run locally

The repository `.env` is excluded from the Docker build context. It can be
passed to a container without copying it into the image:

```powershell
New-Item -ItemType Directory -Force D:\jkp-production | Out-Null

docker run --rm --name jkp-build `
  --env-file .env `
  --mount type=bind,source=D:\jkp-production,target=/data `
  jkp-data:production `
  build /data `
  --compustat-source xpressfeed `
  --bypass-crsp `
  --persistent-connection `
  --start-date 2000-01-01 `
  --end-date 2026-06-30
```

Omit `--end-date` for the normal monthly job; the application then fixes the
cutoff at the previous calendar month-end when the process starts.

## Push to Amazon ECR

The image has its own ECR repository, so the name is the same locally and
remotely: `jkp-data:production`. Each push replaces it.

```powershell
$AwsRegion = "eu-central-1"
$AwsAccountId = aws sts get-caller-identity --query Account --output text
$Registry = "$AwsAccountId.dkr.ecr.$AwsRegion.amazonaws.com"
$Image = "$Registry/jkp-data:production"

aws ecr get-login-password --region $AwsRegion |
  docker login --username AWS --password-stdin $Registry

docker tag jkp-data:production $Image
docker push $Image
```

Build for `linux/amd64` before pushing: the tag is pulled by Intel/AMD EC2
instances, and an arm64 image fails at run time, not at push time.

Rebuild and push whenever the pipeline code changes. The tag is mutable and
carries no version marker, so what it points at is only ever "the last thing
pushed" — check `git log` for what that was.

## Run on EC2

Attach an adequately sized EBS volume and prepare its mount for the container's
non-root UID (`10001`):

```bash
sudo mkdir -p /mnt/jkp-data
sudo chown 10001:10001 /mnt/jkp-data
chmod 700 /mnt/jkp-data
chmod 600 /secure/jkp.env
```

Authenticate, pull, and run:

```bash
AWS_REGION=eu-central-1
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
IMAGE="$REGISTRY/jkp-data:production"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
docker pull "$IMAGE"

docker run --rm --name jkp-monthly \
  --env-file /secure/jkp.env \
  --mount type=bind,source=/mnt/jkp-data,target=/data \
  "$IMAGE" \
  build /data \
  --compustat-source xpressfeed \
  --bypass-crsp \
  --persistent-connection \
  --start-date 2000-01-01
```

For the first full run, keep the stopped container available for inspection and
follow its progress from a second SSH session:

```bash
docker run -d --name jkp-monthly \
  --env-file /secure/jkp.env \
  --mount type=bind,source=/mnt/jkp-data,target=/data \
  "$IMAGE" \
  build /data \
  --compustat-source xpressfeed \
  --bypass-crsp \
  --persistent-connection \
  --start-date 2000-01-01 \
  --metrics-interval 60

docker logs --follow jkp-monthly
```

The build writes durable monitoring artifacts beneath
`/mnt/jkp-data/run_logs/<run-id>/`:

- `pipeline.log` records eight major phases, individual step start/end times,
  overall elapsed time, and a resource heartbeat every sampling interval.
- `resource_metrics.csv` records host and process CPU, CPU I/O wait, thread
  count, load average, RAM, process RSS, swap, disk capacity, process/system
  disk I/O, and network traffic.
- `step_timings.csv` contains one row for every completed or failed timed step.
- `run_summary.json` records status, configuration, phase durations, peak
  resources, failure details, and host sizing.

`run_logs/latest_run.txt` identifies the current run. A successful run also
updates `run_logs/timing_history.json`. The next comparable run uses those
phase durations to print an estimated remaining time. During the first run the
ETA is explicitly shown as unavailable because no defensible history exists.
The CSV and text files are flushed continuously, so the observations written
before an out-of-memory termination remain on the mounted EBS volume.

After the container exits, inspect its exit code and the final summary before
removing it:

```bash
docker inspect jkp-monthly --format '{{.State.Status}} exit={{.State.ExitCode}}'
RUN_ID=$(cat /mnt/jkp-data/run_logs/latest_run.txt)
cat "/mnt/jkp-data/run_logs/$RUN_ID/run_summary.json"
docker rm jkp-monthly
```

If a run fails after `download_raw_data_tables` completed, retain the `raw/`
directory, remove or archive the partial `interim/` and `processed/`
directories, and restart with `--reuse-raw --force`. The recovery option
validates all required source files and verifies that the `secd` and `g_secd`
part sequences match their complete security-pair universes before it skips
the database download. It fails closed when any input or daily part is
missing or empty.

The EC2 security group must be allowed to reach the XpressFeed RDS on port
5432. No VPN is required when routing and security groups permit private VPC
access.

`/secure/jkp.env` should contain `COMPUSTAT=...` and should not be stored in the
repository, image, user-data script, or ECR. A later automated deployment can
retrieve it from AWS Secrets Manager immediately before starting the job.
