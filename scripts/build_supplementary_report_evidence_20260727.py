"""Build compact evidence tables used by the rerun comparison report."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import analyze_run_comparison_20260727 as comparison  # noqa: E402


TABLES = ROOT / "report_codex_27-07-2026_run_analysis_artifacts" / "tables"
FROZEN_ACCOUNTING = Path(r"D:\jkp-full-run-20260727\interim\accounting_data")
BASE_ACCOUNTING = ("assets", "book_equity", "sales", "net_income")
TRACE_GVKEYS = ("051423", "051863", "226824", "282615", "288567", "321134")
SCORE_COLUMNS = (
    "f_score", "o_score", "z_score", "mispricing_mgmt", "mispricing_perf",
    "qmj", "qmj_prof", "qmj_growth", "qmj_safety",
)


def main() -> None:
    metadata = json.loads(comparison.METADATA_PATH.read_text(encoding="utf-8"))
    s3, research, _ = comparison.load_inputs("monthly", metadata)
    keys = ["excntry", "eom", "id"]
    joined = s3.select(keys + ["conm", "gvkey"] + list(BASE_ACCOUNTING)).join(
        research.select(keys + list(BASE_ACCOUNTING)),
        on=keys,
        how="inner",
        suffix="_research",
    )

    s3_all_null = pl.all_horizontal([pl.col(c).is_null() for c in BASE_ACCOUNTING])
    research_any_value = pl.any_horizontal(
        [pl.col(f"{c}_research").is_not_null() for c in BASE_ACCOUNTING]
    )
    missing = joined.filter(s3_all_null & research_any_value).sort(keys)
    missing.write_csv(TABLES / "accounting_all_base_null_s3_records.csv")

    research_all_null = pl.all_horizontal(
        [pl.col(f"{c}_research").is_null() for c in BASE_ACCOUNTING]
    )
    s3_any_value = pl.any_horizontal([pl.col(c).is_not_null() for c in BASE_ACCOUNTING])
    opposite = joined.filter(research_all_null & s3_any_value).sort(keys)
    opposite.write_csv(TABLES / "accounting_all_base_null_research_records.csv")

    missing_gvkeys = {
        str(value).zfill(6) for value in missing.get_column("gvkey").drop_nulls().to_list()
    }
    trace_gvkeys = sorted(missing_gvkeys | set(TRACE_GVKEYS))
    frames = []
    for frequency in ("annual", "quarterly"):
        path = FROZEN_ACCOUNTING / f"{frequency}.parquet"
        schema = pl.scan_parquet(path).collect_schema()
        fields = [
            name for name in (
                "gvkey", "curcd", "datadate", "availability_date", "source",
                "at", "ceq", "seq", "sale", "revt", "ni", "ib",
                "ni_qtr", "sale_qtr",
            ) if name in schema
        ]
        frame = (
            pl.scan_parquet(path)
            .filter(
                pl.col("gvkey").cast(pl.String).str.zfill(6).is_in(trace_gvkeys)
                & (pl.col("datadate") >= pl.date(2023, 1, 1))
            )
            .select(fields)
            .with_columns(pl.lit(frequency).alias("frequency"))
            .collect()
        )
        frames.append(frame)
    pl.concat(frames, how="diagonal_relaxed").sort(
        ["gvkey", "frequency", "datadate", "availability_date"],
        descending=[False, False, True, True],
        nulls_last=True,
    ).write_csv(TABLES / "frozen_accounting_lineage_records.csv")

    correlations = pl.read_csv(TABLES / "monthly_numeric_column_comparison.csv")
    correlations.filter(pl.col("column").is_in(SCORE_COLUMNS)).sort("column").write_csv(
        TABLES / "factor_score_comparison_summary.csv"
    )

    summary = {
        "s3_all_four_base_accounting_null_research_has_value_rows": missing.height,
        "s3_all_four_base_accounting_null_research_has_value_distinct_ids": missing[
            "id"
        ].n_unique(),
        "all_such_rows_are_may": bool((missing["eom"] == 20260531).all()),
        "research_all_four_base_accounting_null_s3_has_value_rows": opposite.height,
        "research_all_four_base_accounting_null_s3_has_value_distinct_ids": opposite[
            "id"
        ].n_unique(),
    }
    (TABLES / "supplementary_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
