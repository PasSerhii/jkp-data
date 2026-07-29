"""Compare rerun2 May/June 2026 production CSVs with Research production tables."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import polars as pl


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = ROOT / "report_codex_27-07-2026_run_analysis_artifacts"
RAW_DIR = ARTIFACT_ROOT / "raw"
TABLE_DIR = ARTIFACT_ROOT / "tables"
METADATA_PATH = RAW_DIR / "extraction_metadata.json"

ABS_TOL = 1e-8
REL_TOL = 1e-6
TOP_N = 25

MONTHLY_IDENTIFIER_FIELDS = (
    "conm",
    "date",
    "size_grp",
    "obs_main",
    "exch_main",
    "primary_sec",
    "gvkey",
    "iid",
    "curcd",
    "common",
    "comp_tpci",
    "comp_exchg",
    "gics",
    "sic",
    "naics",
    "ff49",
    "sedol",
    "cusip",
    "isin_orig",
    "isin",
)
DAILY_IDENTIFIER_FIELDS = ("sedol", "cusip", "isin")


def pl_type(database_type: str, *, s3: bool) -> pl.DataType:
    if database_type == "varchar":
        return pl.String
    if database_type == "float":
        return pl.Float64
    if database_type in {"bigint", "int"}:
        return pl.Int64
    if database_type == "date":
        return pl.Int64 if s3 else pl.Date
    raise ValueError(database_type)


def load_inputs(kind: str, metadata: dict) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, str]]:
    header = metadata["s3_headers"][kind]
    db_types = {item["name"]: item["data_type"] for item in metadata["research_schemas"][kind]}
    if set(header) != set(db_types):
        raise RuntimeError(f"{kind} schemas do not align")
    schema = {name: pl_type(db_types[name], s3=True) for name in header}
    frames = []
    for country in metadata["scope"]["countries"]:
        path = RAW_DIR / "s3" / f"{kind}_{country.lower()}.csv"
        frames.append(
            pl.read_csv(
                path,
                has_header=False,
                new_columns=header,
                schema_overrides=schema,
                null_values=[""],
                infer_schema_length=0,
            )
        )
    s3 = pl.concat(frames, how="vertical", rechunk=True)
    research_path = (
        RAW_DIR
        / "research"
        / ("dailyreturnsproduction.parquet" if kind == "daily" else "characteristicsproduction.parquet")
    )
    research = pl.read_parquet(research_path)
    date_columns = [name for name, dtype in db_types.items() if dtype == "date"]
    research = research.with_columns(
        [pl.col(name).dt.strftime("%Y%m%d").cast(pl.Int64).alias(name) for name in date_columns]
    )
    for frame_name, frame in (("S3", s3), ("Research", research)):
        actual = set(frame.columns)
        if actual != set(header):
            raise RuntimeError(f"{kind} {frame_name} columns differ from expected schema")
    return s3, research.select(header), db_types


def normalized_string_expr(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.String).str.strip_chars().str.to_uppercase()


def prepare(frame: pl.DataFrame, kind: str) -> pl.DataFrame:
    date_columns = ["date"] if kind == "daily" else ["date", "eom"]
    exprs: list[pl.Expr] = [normalized_string_expr("excntry").alias("excntry")]
    exprs.extend(pl.col(name).cast(pl.Int64).alias(name) for name in date_columns)
    return frame.with_columns(exprs)


def month_expr(kind: str) -> pl.Expr:
    date_name = "date" if kind == "daily" else "eom"
    return (pl.col(date_name) // 100).cast(pl.Int64).alias("month")


def write(frame: pl.DataFrame, name: str) -> None:
    frame.write_csv(TABLE_DIR / name)


def key_columns(kind: str) -> list[str]:
    return ["excntry", "date" if kind == "daily" else "eom", "id"]


def duplicate_records(frame: pl.DataFrame, source: str, kind: str) -> pl.DataFrame:
    keys = key_columns(kind)
    counts = frame.group_by(keys).len(name="duplicate_count").filter(pl.col("duplicate_count") > 1)
    if counts.is_empty():
        return pl.DataFrame(
            schema={"frequency": pl.String, "source": pl.String, **{k: frame.schema[k] for k in keys}, "duplicate_count": pl.UInt32}
        )
    return counts.with_columns(
        pl.lit(kind).alias("frequency"), pl.lit(source).alias("source")
    ).select("frequency", "source", *keys, "duplicate_count")


def membership_sets(
    s3: pl.DataFrame, research: pl.DataFrame, kind: str
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    keys = key_columns(kind)
    s3_keys = s3.select(keys).unique()
    research_keys = research.select(keys).unique()
    common = s3_keys.join(research_keys, on=keys, how="inner")
    s3_only = s3_keys.join(research_keys, on=keys, how="anti")
    research_only = research_keys.join(s3_keys, on=keys, how="anti")
    return common, s3_only, research_only


def universe_summary(
    s3: pl.DataFrame,
    research: pl.DataFrame,
    common: pl.DataFrame,
    s3_only: pl.DataFrame,
    research_only: pl.DataFrame,
    kind: str,
) -> pl.DataFrame:
    def counts(frame: pl.DataFrame, name: str) -> pl.DataFrame:
        return (
            frame.with_columns(month_expr(kind))
            .group_by("month", "excntry")
            .len(name=name)
        )

    result = counts(s3, "s3_rows")
    for frame, name in (
        (research, "research_rows"),
        (common, "common_rows"),
        (s3_only, "s3_only_rows"),
        (research_only, "research_only_rows"),
    ):
        result = result.join(counts(frame, name), on=["month", "excntry"], how="full", coalesce=True)
    return result.fill_null(0).sort("month", "excntry")


def universe_discrepancies(
    s3: pl.DataFrame,
    research: pl.DataFrame,
    s3_only: pl.DataFrame,
    research_only: pl.DataFrame,
    kind: str,
) -> pl.DataFrame:
    keys = key_columns(kind)
    if kind == "daily":
        context = ["sedol", "cusip", "isin"]
    else:
        context = [
            "conm",
            "date",
            "gvkey",
            "iid",
            "sedol",
            "cusip",
            "isin_orig",
            "isin",
            "size_grp",
            "curcd",
            "gics",
            "sic",
            "naics",
            "ff49",
        ]
    left = s3_only.join(s3, on=keys, how="left").select(*keys, *context).with_columns(pl.lit("S3-only").alias("side"))
    right = (
        research_only.join(research, on=keys, how="left")
        .select(*keys, *context)
        .with_columns(pl.lit("Research-only").alias("side"))
    )
    return pl.concat([left, right], how="vertical").select("side", *keys, *context).sort([*keys, "side"])


def month_over_month_changes(frame: pl.DataFrame, source: str, kind: str) -> pl.DataFrame:
    period_col = "date" if kind == "daily" else "eom"
    active = frame.with_columns((pl.col(period_col) // 100).alias("month")).select("excntry", "month", "id").unique()
    may = active.filter(pl.col("month") == 202605).select("excntry", "id")
    june = active.filter(pl.col("month") == 202606).select("excntry", "id")
    added = june.join(may, on=["excntry", "id"], how="anti").with_columns(
        pl.lit("added_in_June").alias("change")
    )
    removed = may.join(june, on=["excntry", "id"], how="anti").with_columns(
        pl.lit("removed_after_May").alias("change")
    )
    result = pl.concat([added, removed], how="vertical")
    return result.with_columns(
        pl.lit(source).alias("source"), pl.lit(kind).alias("frequency")
    ).select("frequency", "source", "excntry", "id", "change").sort("excntry", "change", "id")


def canonical_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip().upper()
        return stripped if stripped else None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def identifier_mismatches(joined: pl.DataFrame, kind: str) -> pl.DataFrame:
    keys = key_columns(kind)
    fields = DAILY_IDENTIFIER_FIELDS if kind == "daily" else MONTHLY_IDENTIFIER_FIELDS
    context = [] if kind == "daily" else ["conm", "conm_research"]
    outputs: list[pl.DataFrame] = []
    for field in fields:
        left_name = field
        right_name = f"{field}_research"
        left = joined.get_column(left_name).to_list()
        right = joined.get_column(right_name).to_list()
        mask = [canonical_value(a) != canonical_value(b) for a, b in zip(left, right, strict=True)]
        if not any(mask):
            continue
        mismatch = (
            joined.filter(pl.Series(mask))
            .select(*keys, *context, pl.col(left_name).cast(pl.String).alias("s3_value"), pl.col(right_name).cast(pl.String).alias("research_value"))
            .with_columns(pl.lit(field).alias("column"))
            .select(*keys, *context, "column", "s3_value", "research_value")
        )
        outputs.append(mismatch)
    if not outputs:
        return pl.DataFrame()
    return pl.concat(outputs, how="vertical").sort("column", *keys)


def identifier_mismatch_summary(mismatches: pl.DataFrame, kind: str) -> pl.DataFrame:
    if mismatches.is_empty():
        return pl.DataFrame()
    return (
        mismatches.with_columns(
            pl.when(pl.col("s3_value").is_null() & pl.col("research_value").is_not_null())
            .then(pl.lit("S3_null"))
            .when(pl.col("s3_value").is_not_null() & pl.col("research_value").is_null())
            .then(pl.lit("Research_null"))
            .otherwise(pl.lit("different_values"))
            .alias("mismatch_type")
        )
        .group_by("column", "excntry", "mismatch_type")
        .agg(pl.len().alias("cells"), pl.col("id").n_unique().alias("distinct_ids"))
        .with_columns(pl.lit(kind).alias("frequency"))
        .select("frequency", "column", "excntry", "mismatch_type", "cells", "distinct_ids")
        .sort("column", "excntry", "mismatch_type")
    )


def one_sided_security_runs(discrepancies: pl.DataFrame, kind: str) -> pl.DataFrame:
    date_col = "date" if kind == "daily" else "eom"
    aggregations: list[pl.Expr] = [
        pl.len().alias("rows"),
        pl.col(date_col).min().alias("first_date"),
        pl.col(date_col).max().alias("last_date"),
    ]
    for column in ("conm", "gvkey", "iid", "sedol", "cusip", "isin"):
        if column in discrepancies.columns:
            aggregations.append(pl.col(column).drop_nulls().first().alias(column))
    return (
        discrepancies.group_by("side", "excntry", "id")
        .agg(aggregations)
        .with_columns(pl.lit(kind).alias("frequency"))
        .select("frequency", "side", "excntry", "id", *[c for c in ["conm", "gvkey", "iid", "sedol", "cusip", "isin"] if c in discrepancies.columns], "rows", "first_date", "last_date")
        .sort("rows", descending=True)
    )


def implied_rf_summary(joined: pl.DataFrame, kind: str) -> pl.DataFrame:
    if kind == "daily":
        s3_rf = pl.col("ret_dollar") - pl.col("ret_exc_dollar")
        research_rf = pl.col("ret_dollar_research") - pl.col("ret_exc_dollar_research")
        scale = 21.0
    else:
        s3_rf = pl.col("ret") - pl.col("ret_exc")
        research_rf = pl.col("ret_research") - pl.col("ret_exc_research")
        scale = 1.0
    date_col = "date" if kind == "daily" else "eom"
    values = joined.select(
        "excntry",
        (pl.col(date_col) // 100).alias("month"),
        (s3_rf * scale).alias("s3_monthly_equivalent_rf"),
        (research_rf * scale).alias("research_monthly_equivalent_rf"),
    )
    return (
        values.group_by("month", "excntry")
        .agg(
            pl.col("s3_monthly_equivalent_rf").drop_nulls().median().alias("s3_median_rf"),
            pl.col("research_monthly_equivalent_rf").drop_nulls().median().alias("research_median_rf"),
            pl.col("s3_monthly_equivalent_rf").drop_nulls().min().alias("s3_min_rf"),
            pl.col("s3_monthly_equivalent_rf").drop_nulls().max().alias("s3_max_rf"),
            pl.col("research_monthly_equivalent_rf").drop_nulls().min().alias("research_min_rf"),
            pl.col("research_monthly_equivalent_rf").drop_nulls().max().alias("research_max_rf"),
        )
        .with_columns((pl.col("s3_median_rf") - pl.col("research_median_rf")).alias("median_difference"))
        .sort("month", "excntry")
    )


def market_identifier_collisions(frame: pl.DataFrame, source: str, kind: str) -> pl.DataFrame:
    date_col = "date" if kind == "daily" else "eom"
    fields = ("sedol", "cusip", "isin") if kind == "daily" else ("gvkey", "sedol", "cusip", "isin_orig", "isin")
    outputs: list[pl.DataFrame] = []
    for field in fields:
        values = frame.with_columns(pl.col(field).cast(pl.String).str.strip_chars().str.to_uppercase().alias("identifier"))
        values = values.filter(pl.col("identifier").is_not_null() & (pl.col("identifier") != ""))
        collisions = (
            values.group_by("excntry", date_col, "identifier")
            .agg(
                pl.col("id").n_unique().alias("distinct_ids"),
                pl.col("id").unique().sort().alias("id_list"),
            )
            .filter(pl.col("distinct_ids") > 1)
            .with_columns(
                pl.col("id_list")
                .list.eval(pl.element().cast(pl.String))
                .list.join("|")
                .alias("ids")
            )
            .drop("id_list")
        )
        if collisions.is_empty():
            continue
        outputs.append(
            collisions.with_columns(
                pl.lit(kind).alias("frequency"), pl.lit(source).alias("source"), pl.lit(field).alias("identifier_field")
            ).select("frequency", "source", "excntry", date_col, "identifier_field", "identifier", "distinct_ids", "ids")
        )
    return pl.concat(outputs, how="vertical") if outputs else pl.DataFrame()


def remap_candidates(
    s3_discrepancies: pl.DataFrame,
    research_discrepancies: pl.DataFrame,
    kind: str,
) -> pl.DataFrame:
    date_col = "date" if kind == "daily" else "eom"
    fields = ("sedol", "cusip", "isin") if kind == "daily" else ("sedol", "cusip", "isin_orig", "isin")
    outputs: list[pl.DataFrame] = []
    for field in fields:
        left = s3_discrepancies.with_columns(
            pl.col(field).cast(pl.String).str.strip_chars().str.to_uppercase().alias("match_value")
        ).filter(pl.col("match_value").is_not_null() & (pl.col("match_value") != ""))
        right = research_discrepancies.with_columns(
            pl.col(field).cast(pl.String).str.strip_chars().str.to_uppercase().alias("match_value")
        ).filter(pl.col("match_value").is_not_null() & (pl.col("match_value") != ""))
        matches = left.select("excntry", date_col, pl.col("id").alias("s3_id"), "match_value").join(
            right.select("excntry", date_col, pl.col("id").alias("research_id"), "match_value"),
            on=["excntry", date_col, "match_value"],
            how="inner",
        ).filter(pl.col("s3_id") != pl.col("research_id"))
        if not matches.is_empty():
            outputs.append(matches.with_columns(pl.lit(field).alias("matched_identifier")))
    if kind == "monthly":
        left = s3_discrepancies.with_columns(
            pl.concat_str(pl.col("gvkey").cast(pl.String), pl.col("iid").cast(pl.String), separator="|").alias("match_value")
        )
        right = research_discrepancies.with_columns(
            pl.concat_str(pl.col("gvkey").cast(pl.String), pl.col("iid").cast(pl.String), separator="|").alias("match_value")
        )
        matches = left.select("excntry", date_col, pl.col("id").alias("s3_id"), "match_value").join(
            right.select("excntry", date_col, pl.col("id").alias("research_id"), "match_value"),
            on=["excntry", date_col, "match_value"], how="inner"
        ).filter(pl.col("s3_id") != pl.col("research_id"))
        if not matches.is_empty():
            outputs.append(matches.with_columns(pl.lit("gvkey+iid").alias("matched_identifier")))
    return pl.concat(outputs, how="vertical").unique().sort("excntry", date_col) if outputs else pl.DataFrame()


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 2:
        return float("nan")
    ar = pd.Series(a).rank(method="average").to_numpy()
    br = pd.Series(b).rank(method="average").to_numpy()
    if np.std(ar) == 0 or np.std(br) == 0:
        return float("nan")
    return float(np.corrcoef(ar, br)[0, 1])


def correlations(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    if len(a) < 2 or np.std(a) == 0 or np.std(b) == 0:
        return float("nan"), float("nan")
    return float(np.corrcoef(a, b)[0, 1]), spearman(a, b)


def append_csv(frame: pl.DataFrame, path: Path, first: bool) -> bool:
    if frame.is_empty():
        return first
    with path.open("wb" if first else "ab") as handle:
        frame.write_csv(handle, include_header=first)
    return False


def numeric_comparison(
    joined: pl.DataFrame,
    kind: str,
    numeric_columns: Iterable[str],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, int]]:
    keys = key_columns(kind)
    material_path = TABLE_DIR / f"{kind}_material_numeric_discrepancy_records.csv"
    top_path = TABLE_DIR / f"{kind}_numeric_top_differences.csv"
    if material_path.exists():
        material_path.unlink()
    if top_path.exists():
        top_path.unlink()
    material_first = True
    top_first = True
    stats: list[dict[str, object]] = []
    country_stats: list[dict[str, object]] = []
    month_stats: list[dict[str, object]] = []
    country_values = joined.get_column("excntry").to_numpy()
    date_col = "date" if kind == "daily" else "eom"
    month_values = (joined.get_column(date_col).to_numpy() // 100).astype(np.int64)
    context = [] if kind == "daily" else ["conm", "conm_research"]
    total_material_values = 0
    total_null_mismatches = 0

    for column in numeric_columns:
        right_column = f"{column}_research"
        a_null = joined.get_column(column).is_null().to_numpy()
        b_null = joined.get_column(right_column).is_null().to_numpy()
        a = joined.get_column(column).cast(pl.Float64).fill_null(float("nan")).to_numpy()
        b = joined.get_column(right_column).cast(pl.Float64).fill_null(float("nan")).to_numpy()
        finite = (~a_null) & (~b_null) & np.isfinite(a) & np.isfinite(b)
        null_mismatch = a_null ^ b_null
        abs_diff = np.full(len(a), np.nan)
        tolerance = np.full(len(a), np.nan)
        abs_diff[finite] = np.abs(a[finite] - b[finite])
        tolerance[finite] = np.maximum(
            ABS_TOL, REL_TOL * np.maximum(np.abs(a[finite]), np.abs(b[finite]))
        )
        value_material = finite & (abs_diff > tolerance)
        material = value_material | null_mismatch
        total_material_values += int(value_material.sum())
        total_null_mismatches += int(null_mismatch.sum())
        pair_a = a[finite]
        pair_b = b[finite]
        pearson, rank_corr = correlations(pair_a, pair_b)
        diffs = abs_diff[finite]
        stats.append(
            {
                "column": column,
                "finite_pairs": int(finite.sum()),
                "exact_equal_pairs": int(np.sum(diffs == 0)),
                "pearson": pearson,
                "spearman": rank_corr,
                "mae": float(np.mean(diffs)) if len(diffs) else float("nan"),
                "median_abs_diff": float(np.median(diffs)) if len(diffs) else float("nan"),
                "p95_abs_diff": float(np.quantile(diffs, 0.95)) if len(diffs) else float("nan"),
                "p99_abs_diff": float(np.quantile(diffs, 0.99)) if len(diffs) else float("nan"),
                "max_abs_diff": float(np.max(diffs)) if len(diffs) else float("nan"),
                "material_value_differences": int(value_material.sum()),
                "null_mismatches": int(null_mismatch.sum()),
                "s3_nulls": int(a_null.sum()),
                "research_nulls": int(b_null.sum()),
            }
        )

        for grouping_name, grouping_values, accumulator in (
            ("country", country_values, country_stats),
            ("month", month_values, month_stats),
        ):
            for group_value in np.unique(grouping_values):
                group_pair = finite & (grouping_values == group_value)
                group_a, group_b = a[group_pair], b[group_pair]
                group_pearson, group_spearman = correlations(group_a, group_b)
                group_diff = np.abs(group_a - group_b)
                group_material = value_material & (grouping_values == group_value)
                group_null = null_mismatch & (grouping_values == group_value)
                accumulator.append(
                    {
                        grouping_name: group_value,
                        "column": column,
                        "finite_pairs": int(group_pair.sum()),
                        "pearson": group_pearson,
                        "spearman": group_spearman,
                        "mae": float(np.mean(group_diff)) if len(group_diff) else float("nan"),
                        "max_abs_diff": float(np.max(group_diff)) if len(group_diff) else float("nan"),
                        "material_value_differences": int(group_material.sum()),
                        "null_mismatches": int(group_null.sum()),
                    }
                )

        if material.any():
            indices = np.flatnonzero(material)
            record = (
                joined[indices]
                .select(
                    *keys,
                    *context,
                    pl.col(column).cast(pl.Float64).alias("s3_value"),
                    pl.col(right_column).cast(pl.Float64).alias("research_value"),
                )
                .with_columns(
                    pl.lit(column).alias("column"),
                    pl.Series("abs_diff", abs_diff[indices]),
                    pl.Series("tolerance", tolerance[indices]),
                    pl.Series(
                        "mismatch_type",
                        np.where(null_mismatch[indices], "null_mismatch", "value_difference"),
                    ),
                )
                .with_columns((pl.col("abs_diff") / pl.col("tolerance")).alias("scaled_difference"))
                .select(*keys, *context, "column", "mismatch_type", "s3_value", "research_value", "abs_diff", "tolerance", "scaled_difference")
            )
            material_first = append_csv(record, material_path, material_first)

            value_indices = np.flatnonzero(value_material)
            if len(value_indices):
                chosen = value_indices[np.argsort(abs_diff[value_indices])[-TOP_N:][::-1]]
            else:
                chosen = np.array([], dtype=np.int64)
            null_indices = np.flatnonzero(null_mismatch)[: min(TOP_N, int(null_mismatch.sum()))]
            top_indices = np.concatenate([chosen, null_indices])
            if len(top_indices):
                top = (
                    joined[top_indices]
                    .select(
                        *keys,
                        *context,
                        pl.col(column).cast(pl.Float64).alias("s3_value"),
                        pl.col(right_column).cast(pl.Float64).alias("research_value"),
                    )
                    .with_columns(
                        pl.lit(column).alias("column"),
                        pl.Series("abs_diff", abs_diff[top_indices]),
                        pl.Series("tolerance", tolerance[top_indices]),
                        pl.Series(
                            "mismatch_type",
                            np.where(null_mismatch[top_indices], "null_mismatch", "value_difference"),
                        ),
                    )
                    .with_columns((pl.col("abs_diff") / pl.col("tolerance")).alias("scaled_difference"))
                    .select(*keys, *context, "column", "mismatch_type", "s3_value", "research_value", "abs_diff", "tolerance", "scaled_difference")
                )
                top_first = append_csv(top, top_path, top_first)

    summary = pl.DataFrame(stats).sort("pearson", descending=False, nulls_last=False)
    by_country = pl.DataFrame(country_stats).sort("column", "country")
    by_month = pl.DataFrame(month_stats).sort("column", "month")
    totals = {
        "material_value_differences": total_material_values,
        "null_mismatches": total_null_mismatches,
    }
    return summary, by_country, by_month, totals


def joined_common(s3: pl.DataFrame, research: pl.DataFrame, common: pl.DataFrame, kind: str) -> pl.DataFrame:
    keys = key_columns(kind)
    s3_common = common.join(s3, on=keys, how="left")
    research_common = common.join(research, on=keys, how="left")
    return s3_common.join(research_common, on=keys, how="inner", suffix="_research")


def date_coverage(
    s3: pl.DataFrame,
    research: pl.DataFrame,
    common: pl.DataFrame,
    s3_only: pl.DataFrame,
    research_only: pl.DataFrame,
) -> pl.DataFrame:
    def grouped(frame: pl.DataFrame, name: str) -> pl.DataFrame:
        return frame.group_by("excntry", "date").len(name=name)

    result = grouped(s3, "s3_rows")
    for frame, name in (
        (research, "research_rows"),
        (common, "common_rows"),
        (s3_only, "s3_only_rows"),
        (research_only, "research_only_rows"),
    ):
        result = result.join(grouped(frame, name), on=["excntry", "date"], how="full", coalesce=True)
    return result.fill_null(0).sort("excntry", "date")


def main() -> None:
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    all_duplicates: list[pl.DataFrame] = []
    all_changes: list[pl.DataFrame] = []
    all_collisions: list[pl.DataFrame] = []
    analysis_summary: dict[str, object] = {
        "thresholds": {"absolute": ABS_TOL, "relative": REL_TOL},
        "frequencies": {},
    }

    for kind in ("daily", "monthly"):
        print(f"Loading {kind} inputs", flush=True)
        s3, research, db_types = load_inputs(kind, metadata)
        s3, research = prepare(s3, kind), prepare(research, kind)
        duplicates = pl.concat(
            [duplicate_records(s3, "S3", kind), duplicate_records(research, "Research", kind)],
            how="diagonal_relaxed",
        )
        if not duplicates.is_empty():
            all_duplicates.append(duplicates)
            raise RuntimeError(f"Duplicate {kind} keys found; see duplicate_keys.csv")

        common, s3_only, research_only = membership_sets(s3, research, kind)
        summary = universe_summary(s3, research, common, s3_only, research_only, kind)
        write(summary, f"{kind}_universe_summary.csv")
        discrepancies = universe_discrepancies(s3, research, s3_only, research_only, kind)
        write(discrepancies, f"{kind}_universe_discrepancies.csv")
        write(one_sided_security_runs(discrepancies, kind), f"{kind}_one_sided_security_runs.csv")
        if kind == "daily":
            write(date_coverage(s3, research, common, s3_only, research_only), "daily_date_coverage.csv")

        all_changes.extend(
            [month_over_month_changes(s3, "S3", kind), month_over_month_changes(research, "Research", kind)]
        )
        for source, frame in (("S3", s3), ("Research", research)):
            collision = market_identifier_collisions(frame, source, kind)
            if not collision.is_empty():
                all_collisions.append(collision)

        s3_disc = discrepancies.filter(pl.col("side") == "S3-only")
        research_disc = discrepancies.filter(pl.col("side") == "Research-only")
        remaps = remap_candidates(s3_disc, research_disc, kind)
        write(remaps, f"{kind}_identifier_remap_candidates.csv")

        print(f"Joining {kind} common keys", flush=True)
        joined = joined_common(s3, research, common, kind)
        id_mismatches = identifier_mismatches(joined, kind)
        write(id_mismatches, f"{kind}_identifier_and_classification_mismatches.csv")
        write(identifier_mismatch_summary(id_mismatches, kind), f"{kind}_identifier_mismatch_summary.csv")
        write(implied_rf_summary(joined, kind), f"{kind}_implied_rf_summary.csv")

        keys = set(key_columns(kind))
        numeric_columns = [
            name
            for name, dtype in db_types.items()
            if dtype in {"float", "bigint", "int"} and name not in keys
        ]
        print(f"Comparing {len(numeric_columns)} {kind} numeric columns", flush=True)
        numeric, by_country, by_month, totals = numeric_comparison(joined, kind, numeric_columns)
        write(numeric, f"{kind}_numeric_column_comparison.csv")
        write(by_country, f"{kind}_numeric_comparison_by_country.csv")
        write(by_month, f"{kind}_numeric_comparison_by_month.csv")

        frequency_summary = {
            "s3_rows": s3.height,
            "research_rows": research.height,
            "common_keys": common.height,
            "s3_only_keys": s3_only.height,
            "research_only_keys": research_only.height,
            "duplicate_keys": duplicates.height,
            "identifier_mismatch_cells": id_mismatches.height,
            "identifier_remap_candidates": remaps.height,
            "numeric_columns": len(numeric_columns),
            **totals,
        }
        analysis_summary["frequencies"][kind] = frequency_summary
        print(json.dumps({kind: frequency_summary}, indent=2), flush=True)

    duplicate_output = pl.concat(all_duplicates, how="diagonal_relaxed") if all_duplicates else pl.DataFrame(
        schema={"frequency": pl.String, "source": pl.String, "excntry": pl.String, "date": pl.Int64, "id": pl.Int64, "duplicate_count": pl.UInt32}
    )
    write(duplicate_output, "duplicate_keys.csv")
    write(pl.concat(all_changes, how="diagonal_relaxed"), "month_over_month_universe_changes.csv")
    collisions = pl.concat(all_collisions, how="diagonal_relaxed") if all_collisions else pl.DataFrame()
    write(collisions, "identifier_collisions.csv")
    (ARTIFACT_ROOT / "analysis_summary.json").write_text(
        json.dumps(analysis_summary, indent=2, default=str) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
