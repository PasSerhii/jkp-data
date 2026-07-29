"""Extract the May/June 2026 inputs for the rerun2 production audit.

The script is deliberately read-only with respect to S3 and SQL Server.  It
uses S3 Select so that the 50 GiB historical CSV collection does not need to be
downloaded locally, and writes the corresponding live Research rows to
Parquet.  Connection secrets are read from .env and never logged or persisted.
"""

from __future__ import annotations

import csv
import argparse
import json
import subprocess
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = ROOT / "report_codex_27-07-2026_run_analysis_artifacts"
RAW_DIR = ARTIFACT_ROOT / "raw"
S3_DIR = RAW_DIR / "s3"
DB_DIR = RAW_DIR / "research"

BUCKET = "jkp-data-runs-485357734136-eu-central-1"
PREFIX = "run-20260727-rerun2/production"
COUNTRIES = ("CAN", "DEU", "FRA", "GBR", "HKG", "IND", "ITA", "JPN", "NOR", "USA")


def dotenv_value(name: str) -> str:
    path = ROOT / ".env"
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != name:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if value:
            return value
    raise RuntimeError(f"{name} is not set in {path}")


def run_aws(*args: str) -> None:
    subprocess.run(["aws", *args], check=True, stdout=subprocess.DEVNULL)


def get_csv_header(key: str) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="jkp-audit-header-") as temp_dir:
        sample = Path(temp_dir) / "sample"
        run_aws(
            "s3api",
            "get-object",
            "--bucket",
            BUCKET,
            "--key",
            key,
            "--range",
            "bytes=0-1048575",
            str(sample),
        )
        with sample.open("r", encoding="utf-8-sig", newline="") as handle:
            return next(csv.reader(handle))


def extract_s3_csv(kind: str, country: str, output: Path) -> None:
    key = f"{PREFIX}/{kind}/{country.lower()}.csv"
    date_column = "date" if kind == "daily" else "eom"
    lower = 20260501 if kind == "daily" else 20260531
    expression = (
        f'SELECT * FROM S3Object s WHERE CAST(s."{date_column}" AS INT) '
        f"BETWEEN {lower} AND 20260630"
    )
    input_serialization = json.dumps(
        {
            "CSV": {
                "FileHeaderInfo": "USE",
                "RecordDelimiter": "\n",
                "FieldDelimiter": ",",
                "QuoteCharacter": '"',
                "QuoteEscapeCharacter": '"',
                "AllowQuotedRecordDelimiter": True,
            },
            "CompressionType": "NONE",
        },
        separators=(",", ":"),
    )
    output_serialization = json.dumps(
        {
            "CSV": {
                "RecordDelimiter": "\n",
                "FieldDelimiter": ",",
                "QuoteCharacter": '"',
                "QuoteEscapeCharacter": '"',
                "QuoteFields": "ASNEEDED",
            }
        },
        separators=(",", ":"),
    )
    run_aws(
        "s3api",
        "select-object-content",
        "--bucket",
        BUCKET,
        "--key",
        key,
        "--expression",
        expression,
        "--expression-type",
        "SQL",
        "--input-serialization",
        input_serialization,
        "--output-serialization",
        output_serialization,
        str(output),
    )


def sql_schema(connection, table: str) -> list[dict[str, str]]:
    rows = connection.execute(
        text(
            """
            SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
            """
        ),
        {"table": table},
    ).mappings()
    return [dict(row) for row in rows]


def export_query_to_parquet(
    engine,
    *,
    table: str,
    date_column: str,
    lower: date,
    output: Path,
    database_schema: list[dict[str, str]],
) -> int:
    country_params = {f"country_{i}": value for i, value in enumerate(COUNTRIES)}
    placeholders = ", ".join(f":country_{i}" for i in range(len(COUNTRIES)))
    query = text(
        f"""
        SELECT *
        FROM dbo.{table}
        WHERE {date_column} BETWEEN :lower AND :upper
          AND excntry IN ({placeholders})
        """
    )
    params = {
        "lower": lower,
        "upper": date(2026, 6, 30),
        **country_params,
    }
    arrow_types = {
        "varchar": pa.large_string(),
        "float": pa.float64(),
        "bigint": pa.int64(),
        "int": pa.int64(),
        "date": pa.date32(),
    }
    arrow_schema = pa.schema(
        [pa.field(column["name"], arrow_types[column["data_type"]]) for column in database_schema]
    )
    writer: pq.ParquetWriter | None = None
    row_count = 0
    try:
        for frame in pd.read_sql_query(query, engine, params=params, chunksize=100_000):
            arrow_table = pa.Table.from_pandas(
                frame, schema=arrow_schema, preserve_index=False, safe=False
            )
            if writer is None:
                writer = pq.ParquetWriter(output, arrow_table.schema, compression="zstd")
            writer.write_table(arrow_table)
            row_count += len(frame)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise RuntimeError(f"The query for dbo.{table} returned no rows")
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="Reuse existing completed S3 extracts (for recovery after interruption).",
    )
    args = parser.parse_args()
    S3_DIR.mkdir(parents=True, exist_ok=True)
    DB_DIR.mkdir(parents=True, exist_ok=True)

    headers: dict[str, list[str]] = {}
    s3_files: dict[str, dict[str, dict[str, object]]] = {"daily": {}, "monthly": {}}
    for kind in ("daily", "monthly"):
        expected_header: list[str] | None = None
        for country in COUNTRIES:
            key = f"{PREFIX}/{kind}/{country.lower()}.csv"
            header = get_csv_header(key)
            if expected_header is None:
                expected_header = header
            elif header != expected_header:
                raise RuntimeError(f"S3 schema mismatch for {key}")
            output = S3_DIR / f"{kind}_{country.lower()}.csv"
            if not (args.resume_existing and output.is_file()):
                extract_s3_csv(kind, country, output)
            with output.open("r", encoding="utf-8", newline="") as handle:
                row_count = sum(1 for _ in handle)
            s3_files[kind][country] = {
                "key": key,
                "file": str(output.relative_to(ROOT)),
                "bytes": output.stat().st_size,
                "rows": row_count,
            }
        assert expected_header is not None
        headers[kind] = expected_header

    research_url = make_url(dotenv_value("RESEARCH"))
    engine = create_engine(research_url, pool_pre_ping=True)
    extraction_started = datetime.now(UTC)
    with engine.connect() as connection:
        server_utc = connection.execute(text("SELECT GETUTCDATE() AS utc_now")).scalar_one()
        schemas = {
            "daily": sql_schema(connection, "dailyreturnsproduction"),
            "monthly": sql_schema(connection, "characteristicsproduction"),
        }
    db_rows = {
        "daily": export_query_to_parquet(
            engine,
            table="dailyreturnsproduction",
            date_column="date",
            lower=date(2026, 5, 1),
            output=DB_DIR / "dailyreturnsproduction.parquet",
            database_schema=schemas["daily"],
        ),
        "monthly": export_query_to_parquet(
            engine,
            table="characteristicsproduction",
            date_column="eom",
            lower=date(2026, 5, 31),
            output=DB_DIR / "characteristicsproduction.parquet",
            database_schema=schemas["monthly"],
        ),
    }
    engine.dispose()

    metadata = {
        "created_utc": datetime.now(UTC).isoformat(),
        "database_extraction_started_utc": extraction_started.isoformat(),
        "database_server_utc_at_start": server_utc.isoformat(),
        "scope": {
            "countries": list(COUNTRIES),
            "daily_start": "2026-05-01",
            "daily_end": "2026-06-30",
            "monthly_eom": ["2026-05-31", "2026-06-30"],
            "s3_bucket": BUCKET,
            "s3_prefix": PREFIX,
            "research_tables": [
                "research.dbo.dailyreturnsproduction",
                "research.dbo.characteristicsproduction",
            ],
        },
        "s3_headers": headers,
        "s3_files": s3_files,
        "research_schemas": schemas,
        "research_rows": db_rows,
        "research_files": {
            "daily": str((DB_DIR / "dailyreturnsproduction.parquet").relative_to(ROOT)),
            "monthly": str((DB_DIR / "characteristicsproduction.parquet").relative_to(ROOT)),
        },
    }
    (RAW_DIR / "extraction_metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps({"s3_rows": s3_files, "research_rows": db_rows}, indent=2))


if __name__ == "__main__":
    main()
