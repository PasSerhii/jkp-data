# Docker and AWS ECR

The image runs the `jkp` CLI. It does not contain credentials, licensed source
data, or generated output. Supply credentials at runtime and mount durable
storage at `/data`.

## Build and test locally

From the repository root in PowerShell:

```powershell
docker build --platform linux/amd64 --provenance=false --sbom=false -t jkp-data:local .
docker run --rm jkp-data:local --version
docker run --rm --entrypoint /app/.venv/bin/python jkp-data:local `
  -c "import duckdb; c=duckdb.connect(); c.execute('LOAD postgres'); print('postgres extension OK')"
```

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
  jkp-data:local `
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

Set the deployment values in PowerShell. The repository is created only if it
does not already exist:

```powershell
$ErrorActionPreference = "Stop"

function Assert-NativeSuccess([string]$Action) {
  if ($LASTEXITCODE -ne 0) {
    throw "$Action failed with exit code $LASTEXITCODE."
  }
}

$AwsRegion = "eu-central-1"
$AwsAccountId = aws sts get-caller-identity --query Account --output text
Assert-NativeSuccess "Read AWS account identity"

$Repository = "alphabeta"
$GitSha = git rev-parse --short=12 HEAD
Assert-NativeSuccess "Read Git commit"

$Dirty = git status --porcelain
Assert-NativeSuccess "Read Git working-tree status"
if ($Dirty) {
  throw "Commit or stash the working tree before building a traceable production image."
}

$Tag = "jkp-data-$(Get-Date -Format 'yyyyMMdd-HHmmss')-$GitSha"
$Registry = "$AwsAccountId.dkr.ecr.$AwsRegion.amazonaws.com"
$Image = "${Registry}/${Repository}:${Tag}"

aws ecr describe-repositories --region $AwsRegion --repository-names $Repository 2>$null
if ($LASTEXITCODE -ne 0) {
  aws ecr create-repository --region $AwsRegion --repository-name $Repository | Out-Null
  Assert-NativeSuccess "Create ECR repository"
}

aws ecr get-login-password --region $AwsRegion |
  docker login --username AWS --password-stdin $Registry
Assert-NativeSuccess "Log Docker in to ECR"

docker build --platform linux/amd64 --provenance=false --sbom=false -t $Image .
Assert-NativeSuccess "Build Docker image"

docker push $Image
Assert-NativeSuccess "Push Docker image"

$Digest = aws ecr describe-images `
  --region $AwsRegion `
  --repository-name $Repository `
  --image-ids imageTag=$Tag `
  --query "imageDetails[0].imageDigest" `
  --output text
Assert-NativeSuccess "Read pushed image digest"

Write-Host "Pushed tag:    $Image"
Write-Host "Immutable URI: ${Registry}/${Repository}@${Digest}"
```

`alphabeta` is the ECR repository; the account-level private registry is
`$Registry`. The timestamp plus Git commit makes the tag unique and traceable,
but this repository currently permits mutable tags. Only the reported digest
URI is immutable, so production jobs should use that URI.

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
IMAGE="$REGISTRY/alphabeta:jkp-data-REPLACE_WITH_TIMESTAMP-AND-COMMIT"

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

The EC2 security group must be allowed to reach the XpressFeed RDS on port
5432. No VPN is required when routing and security groups permit private VPC
access.

`/secure/jkp.env` should contain `COMPUSTAT=...` and should not be stored in the
repository, image, user-data script, or ECR. A later automated deployment can
retrieve it from AWS Secrets Manager immediately before starting the job.
