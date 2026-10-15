"""The rolling source window: end_date minus config.ROLLING_INPUT_YEARS.

A default run carries a constant span instead of one that grows a year every
year. The span has to clear the longest lookback in the pipeline -- seas_16_20
needs 240 monthly observations -- with margin, because that gate counts a
security's own rows rather than calendar months.
"""

from __future__ import annotations

from datetime import date

import pytest

from jkp.data.config import MAX_LOOKBACK_MONTHS, ROLLING_INPUT_YEARS
from jkp.data.main import _rolling_start_date

pytestmark = pytest.mark.unit


def _months_between(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


class TestRollingStartDate:
    @pytest.mark.parametrize(
        "end",
        [
            date(2026, 6, 30),
            date(2026, 1, 31),
            date(2026, 12, 31),
            date(2024, 2, 29),  # leap day must not blow up the year subtraction
            date(2025, 2, 28),
        ],
    )
    def test_window_is_exactly_the_configured_span(self, end: date) -> None:
        start = _rolling_start_date(end)
        assert start.year == end.year - ROLLING_INPUT_YEARS
        assert start.month == end.month
        assert start.day == 1

    @pytest.mark.parametrize("end", [date(2026, 6, 30), date(2024, 2, 29), date(2030, 11, 30)])
    def test_window_clears_the_longest_lookback(self, end: date) -> None:
        """The whole point: seas_16_20 must not be nulled by the window itself."""
        assert _months_between(_rolling_start_date(end), end) >= MAX_LOOKBACK_MONTHS

    def test_configured_span_carries_margin_over_the_bare_lookback(self) -> None:
        """20 years exactly would fail any security with missing months."""
        assert ROLLING_INPUT_YEARS * 12 > MAX_LOOKBACK_MONTHS
        assert ROLLING_INPUT_YEARS >= 23

    def test_anchoring_to_the_first_never_shortens_the_span(self) -> None:
        """Day-of-month anchoring must round the window up, never down."""
        for end in (date(2026, 6, 1), date(2026, 6, 15), date(2026, 6, 30)):
            start = _rolling_start_date(end)
            assert start <= date(end.year - ROLLING_INPUT_YEARS, end.month, end.day)

    def test_span_is_stable_as_the_end_date_advances(self) -> None:
        """A fixed start date grows a year every year; this must not."""
        spans = {
            _months_between(_rolling_start_date(date(y, 6, 30)), date(y, 6, 30))
            for y in (2026, 2030, 2040)
        }
        assert spans == {ROLLING_INPUT_YEARS * 12}
