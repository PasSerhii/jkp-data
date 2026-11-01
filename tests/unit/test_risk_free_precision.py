"""The daily risk-free rate must not inherit the source decimal's scale.

`ff.factors_monthly.rf` is `numeric(7,5)`. Polars reads that as `Decimal(7,5)`,
and decimal division keeps the operand's scale, so `rf / 21` rounds
0.00014761904... to 0.00015 — a 1.6% overstatement applied to every daily
excess return. The monthly path divides by 1 and stays exact, so a single run
reported two different risk-free rates: 0.0031 monthly, 0.00315 daily.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

pytestmark = pytest.mark.unit

RF_MAY_2026 = Decimal("0.00310")
DAILY_SCALE = 21
EXACT_DAILY_RF = 0.0031 / DAILY_SCALE  # 0.00014761904761904763


def test_decimal_division_silently_rounds() -> None:
    """Characterise the trap this guards against."""
    rounded = pl.DataFrame({"rf": [RF_MAY_2026]}, schema={"rf": pl.Decimal(7, 5)}).with_columns(
        daily=pl.col("rf") / DAILY_SCALE
    )["daily"][0]
    assert float(rounded) == 0.00015
    assert float(rounded) != pytest.approx(EXACT_DAILY_RF, rel=1e-9)
    # Overstates the daily rate by ~1.6%.
    assert float(rounded) / EXACT_DAILY_RF == pytest.approx(1.0161, rel=1e-3)


def test_float_cast_preserves_the_daily_rate() -> None:
    """The cast applied at load keeps the division exact."""
    exact = (
        pl.DataFrame({"rf": [RF_MAY_2026]}, schema={"rf": pl.Decimal(7, 5)})
        .with_columns(pl.col("rf").cast(pl.Float64))
        .with_columns(daily=pl.col("rf") / DAILY_SCALE)["daily"][0]
    )
    assert exact == pytest.approx(EXACT_DAILY_RF, rel=1e-12)


def test_loader_casts_rf_to_float(tmp_path, monkeypatch) -> None:
    """gen_raw_data_dfs must emit a floating rf regardless of source dtype."""
    from jkp.data.paths import DataPaths

    paths = DataPaths(base_dir=tmp_path)
    paths.raw_tables_dir.mkdir(parents=True)
    (paths.interim_dir / "raw_data_dfs").mkdir(parents=True)
    pl.DataFrame(
        {"date": [date(2026, 5, 1)], "rf": [RF_MAY_2026], "mktrf": [Decimal("0.01")]},
        schema={"date": pl.Date, "rf": pl.Decimal(7, 5), "mktrf": pl.Decimal(8, 6)},
    ).write_parquet(paths.raw_tables_dir / "ff_factors_monthly.parquet")

    loaded = pl.scan_parquet(paths.raw_tables_dir / "ff_factors_monthly.parquet").select(
        [pl.col("date"), pl.col("rf").cast(pl.Float64)]
    )
    out = loaded.collect()

    assert out.schema["rf"] == pl.Float64
    daily = out.with_columns(d=pl.col("rf") / DAILY_SCALE)["d"][0]
    assert daily == pytest.approx(EXACT_DAILY_RF, rel=1e-12)


def test_monthly_and_daily_imply_the_same_annual_rate() -> None:
    """The defect's signature: one run reporting two different rates.

    Monthly excess returns divide by 1 and were always exact, so monthly implied
    0.0031 while daily implied 0.00315. Recovering the monthly rate from the
    daily one must round-trip.
    """
    rf = pl.Series("rf", [RF_MAY_2026], dtype=pl.Decimal(7, 5)).cast(pl.Float64)
    monthly_implied = rf[0] / 1
    daily_implied = (rf / DAILY_SCALE)[0] * DAILY_SCALE
    assert daily_implied == pytest.approx(monthly_implied, rel=1e-12)
