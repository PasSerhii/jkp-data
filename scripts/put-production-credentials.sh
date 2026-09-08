#!/usr/bin/env bash
# Seed the persistent credential the unattended monthly run reads.
#
#   scripts/put-production-credentials.sh          # from .env at the repo root
#   scripts/put-production-credentials.sh --show   # print what is stored now
#
# Writes one SSM SecureString holding COMPUSTAT (the XpressFeed RDS),
# ENV_USERNAME/ENV_PASSWORD (WRDS, for the Fama-French refresh) and
# RESEARCH_UPDATE (the research MSSQL database the --db-update phase writes
# to). By default a launch stages a per-run parameter and the host deletes it
# after reading, which works only because a human runs the launcher. An
# unattended run has no such human, so this parameter persists and the host
# keeps it after reading.
#
# A key exported in the environment overrides the .env line, matching the
# precedence everywhere else in this repo. That is how RESEARCH_UPDATE gets
# its container-specific form: the image ships "ODBC Driver 18 for SQL Server"
# (bookworm has no driver 17), while a Windows workstation .env typically
# names driver 17 -- so seed with the 18 form exported:
#   RESEARCH_UPDATE='mssql+pyodbc://...?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes' \
#     scripts/put-production-credentials.sh
#
# Run again to rotate: the parameter is overwritten in place and the next run
# picks up the new value with no other change.
set -euo pipefail
export MSYS_NO_PATHCONV=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
REGION=eu-central-1
PARAM=/jkp-data/production/env
KEYS="COMPUSTAT ENV_USERNAME ENV_PASSWORD RESEARCH_UPDATE"

if [ "${1:-}" = "--show" ]; then
  # Names and lengths only. Printing the values would defeat the SecureString.
  aws ssm get-parameter --region "$REGION" --name "$PARAM" --with-decryption \
    --query Parameter.Value --output text \
    | awk -F= '{printf "  %-14s %d chars\n", $1, length($0)-length($1)-1}'
  aws ssm get-parameter --region "$REGION" --name "$PARAM" \
    --query "Parameter.{Version:Version,Modified:LastModifiedDate}" --output table
  exit 0
fi

[ -f "$REPO/.env" ] || { echo "No $REPO/.env to read" >&2; exit 1; }

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
for k in $KEYS; do
  if [ -n "${!k:-}" ]; then
    printf '%s=%s\n' "$k" "${!k}" >> "$TMP"
  else
    grep "^$k=" "$REPO/.env" >> "$TMP" || { echo "No $k= line in $REPO/.env and \$$k not set" >&2; exit 1; }
  fi
done

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) URI="file://$(cygpath -m "$TMP")" ;;
  *)                    URI="file://$TMP" ;;
esac

aws ssm put-parameter --region "$REGION" --name "$PARAM" --type SecureString \
  --description "jkp-data unattended monthly run: XpressFeed + WRDS credentials" \
  --value "$URI" --overwrite >/dev/null

echo "stored $PARAM ($(wc -l < "$TMP" | tr -d ' ') keys)"
echo
echo "Use it by pointing a launch at the parameter instead of staging one:"
echo "  CREDENTIAL_PARAM=$PARAM scripts/aws/launch.sh <tag>"
