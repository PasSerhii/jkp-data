# Factor calculation audit: Python vs documentation vs production SAS

Date: 2026-07-26

## Executive summary

This audit compared the current Python implementation in `src/jkp/data/aux_functions.py`, the formulas in `documentation/documentation.tex` (the source of the PDF), and the production SAS programs in `D:/WRDS/current code in production`.

The calculation universe consists of 315 accounting characteristics, 46 monthly market characteristics, the configured rolling-daily characteristics, and the combined/composite characteristics. The review found four executable defects or formerly executable defects and five documentation defects. It did **not** find another current Python coefficient/sign error like the former O-score working-capital error.

The highest-priority open executable bug is in production SAS: the five-year ratio-change macro fails to expand its `horizon` parameter. The clearest shared code/documentation discrepancy is `nix_sale`, which uses raw `ni` instead of the standardized `nix_x` specified by the documentation and used by the other NIX ratios. That case needs a maintainer decision because using raw `ni` also preserves legacy SAS parity.

## Scope and evidence

| Area | Inventory/evidence | Audit result |
|---|---:|---|
| Accounting characteristics | 315 names in SAS `acc_chars` | Formula families, special formulas, guards, lags, and scaling reviewed |
| Monthly market characteristics | 46 names in SAS `monthly_chars` | Dividend, issuance, momentum, and seasonality formulas agree materially |
| Rolling daily characteristics | Python `ROLLING_DAILY_SPECS` and SAS `roll_apply_daily` | Core formulas and minimum-window conventions agree materially |
| Composite characteristics | Mispricing and QMJ | Inputs, direction handling, and aggregation agree materially, subject to the SAS five-year-growth bug below |
| Historical SAS/Python comparison | 402 characteristics in `sas_vs_py_summ_stats.parquet` | All 402 have Spearman correlation above 0.994; this is supporting evidence, not proof of current parity |
| Focused Python tests | 236 tests | All passed |

The historical comparison has an important limitation: correlations are calculated on overlapping non-missing observations. It can therefore hide coverage bugs such as a formula being populated in SAS but null in Python.

## Confirmed findings

### F1 — Production SAS fails to screen the first 60 months of five-year ratio changes

**Severity:** High  
**Status:** Open in production SAS; Python is correct

Production SAS contains:

```sas
if count <= horizon then
    &name. = .;
```

`horizon` is a macro argument, so this must be `&horizon.`. As written, SAS treats `horizon` as an uninitialized data-step variable. The condition is therefore false for positive `count`, and the intended early-history guard never runs.

Affected characteristics:

- `gpoa_ch5`
- `roe_ch5`
- `roa_ch5`
- `cfoa_ch5`
- `gmar_ch5`

Because SAS `lag60()` queues are not reset at firm boundaries, early observations can use a prior firm's value. These five variables feed `qmj_growth`, which then feeds `qmj`.

Evidence:

- Production SAS: `char_macros.sas:349-356`
- Python correctly compares `count` with the numeric function argument and nulls the early rows: `aux_functions.py:6558-6582`
- Documentation defines these as within-firm five-year changes.

Recommended SAS fix:

```sas
if count <= &horizon. then
    &name. = .;
```

Add a two-firm regression test where the second firm has fewer than 60 monthly rows; all five values must remain missing for that firm.

### F2 — Production SAS misspells the `capex_abn` helper and fails to exclude nonpositive sales

**Severity:** Medium  
**Status:** Open in production SAS; Python is correct

Production SAS creates `__capex_sale` but nulls `__capx_sale` (missing `e`):

```sas
__capex_sale = capx / sale_x;
if sale_x <= 0 then
    __capx_sale = .;
```

The screen has no effect on the value subsequently used by `capex_abn`. Firms with negative sales can therefore receive a production-SAS value contrary to the intended positive-denominator convention. Division by zero still becomes missing in SAS, so the practical error is primarily negative sales.

Evidence:

- Production SAS: `accounting_chars.sas:948-954`
- Python uses `safe_div(..., mode=3)`, which requires `sale_x > 0`: `aux_functions.py:6960-6992`

Recommended SAS fix: change `__capx_sale` to `__capex_sale` and add negative-, zero-, and positive-sales test cases.

### F3 — `nix_sale` uses raw `ni` instead of documented standardized `nix_x`

**Severity:** Low-to-medium; missing-value coverage only  
**Status:** Candidate shared bug; confirm intended legacy behavior before changing

The documentation defines:

```text
nix_sale = NIX* / SALE*
```

`NIX*` is the standardized, coverage-expanded `nix_x` helper. Nevertheless:

- Production SAS computes `nix_sale = ni / sale_x` (`accounting_chars.sas:715`).
- Python computes `safe_div("ni", "sale_x", "nix_sale")` (`aux_functions.py:7150`).

Where raw `ni` exists, `nix_x` takes that value first, so the two formulas are numerically identical. The difference occurs only where `ni` is missing: `nix_x` is constructed as `coalesce(ni, ni_x + ..., ...)`, so the standardized helper can recover observations that raw `ni` cannot. Other NIX characteristics (`nix_be`, `nix_me`, O-score inputs) consistently use `nix_x`.

For example, if `ni` is missing, `ni_x=10`, `xido_x=2`, and `sale_x=100`, then `nix_x=12`. The documented formula yields `0.12`; current Python and SAS yield missing.

`nix_sale` is present in the full stock-level output but is not in the default 153-characteristic portfolio set (`PORTFOLIO_CHARS`). The direct impact is therefore stock-level coverage, not default factor returns.

If maintainers confirm that the standardized definition is authoritative, fix both implementations as follows:

```text
nix_sale = nix_x / sale_x
```

Otherwise, document that `nix_sale` intentionally uses raw Compustat `ni` for legacy SAS continuity. In either case, add a test for a row where `ni` is missing but `nix_x` is recoverable.

### F4 — Historical Python O-score working-capital sign bug is fixed in the current checkout

**Severity:** High when present  
**Status:** Closed in Python; documentation intercept remains wrong (F5)

The pre-fix Python code subtracted `1.43 * WCTA`. Commit `270eef6` changed it to addition. Current parity is:

```text
-1.32 - 0.407*LAT + 6.03*LEV + 1.43*WC + 0.076*CACL
-1.72*NEG_EQ - 2.37*ROE - 1.83*FFO + 0.285*NEG_EARN - 0.52*NICH
```

Current Python (`aux_functions.py:6404-6413`) and production SAS (`char_macros.sas:503-505`) agree. The focused regression test also explicitly protects the `+1.43` sign.

No further action is required for current Python. Do not change the intercept to `-1.37`; fix the documentation instead.

## Documentation defects

### F5 — O-score intercept is `-1.37` in the documentation but `-1.32` in both codebases

**Severity:** Medium  
**Status:** Open documentation bug

The TeX/PDF formula at `documentation.tex:2444` says `-1.37`. Current Python and production SAS both use `-1.32`, and `-1.32` is the production convention guarded by tests. Change the documentation to `-1.32`.

### F6 — `lnoa_gr1a` has numerator and denominator errors relative to the cited HXZ definition

**Severity:** High: the numerator error can change cross-sectional ranks; the denominator error changes raw magnitudes  
**Status:** Confirmed shared Python/SAS formula bug plus documentation bug

- Hou, Xue, and Zhang's replication appendix defines the numerator as the annual change in `PPENT`, plus the annual change in `INTAN`, plus the annual change in `AO`, minus the annual change in `LO`, **plus current `DP`**.
- Python and both the original and production SAS first construct `lnoa_x = PPENT + INTAN + AO - LO + DP`, then difference `lnoa_x`. This produces `ΔPPENT + ΔINTAN + ΔAO - ΔLO + ΔDP`, incorrectly subtracting prior-year `DP`.
- The factor name, Python docstring, SAS comment, and HXZ appendix say “scaled by average assets.” Python and SAS instead divide by `AT_t + AT_{t-12}`, omitting `/ 2`.
- Documentation divides by `AT_t - AT_{t-12}`. This is not an average-assets denominator: it can be zero, can change sign, and is not a constant rescaling of the intended signal.

The cited HXZ formula is:

```text
[(PPENT_t - PPENT_{t-12})
 + (INTAN_t - INTAN_{t-12})
 + (AO_t - AO_{t-12})
 - (LO_t - LO_{t-12})
 + DP_t]
/ ((AT_t + AT_{t-12}) / 2)
```

The current executable numerator is instead:

```text
ΔPPENT + ΔINTAN + ΔAO - ΔLO + (DP_t - DP_{t-12})
```

The missing `/ 2` alone is a positive constant rescaling and therefore would not alter coverage, ranks, or rank-portfolio membership. The numerator error is not a constant rescaling: relative to HXZ, the current numerator is lower by `DP_{t-12}`. It can therefore change raw values, cross-sectional ordering, portfolio assignments, and factor returns.

Correct Python and SAS together by computing the component changes and adding current `DP`, then dividing by `(AT_t + AT_{t-12}) / 2`. Correct the documentation to show that expanded formula. If maintainers intentionally choose legacy continuity instead, the published definition must explicitly disclose both deviations; the present “average assets” description is not accurate.

Evidence: HXZ, *Replicating Anomalies*, Appendix A.3.6; `documentation.tex:1064` and `1784`; production and original SAS `accounting_chars.sas:233` and `893-904`; Python `aux_functions.py:5737`, `5975-5994`, `7680`, and `9367`. Historical SAS/Python comparison results show only that the two executable implementations match one another; they do not validate the formula against HXZ.

### F7 — `noa_at` documentation uses current assets while both codebases use lagged assets

**Severity:** Medium documentation ambiguity  
**Status:** Open documentation bug unless the project intends to redefine the executable series

Documentation says `NOA_t / AT_t`; SAS and Python use `NOA_t / AT_{t-12}` with a 12-month availability guard. The production behavior is deliberate and the Python port matches it. Update the formula and label to state “lagged total assets,” or explicitly approve a breaking formula change in both implementations.

Evidence: `documentation.tex:1517`, SAS `accounting_chars.sas:765-768`, Python `aux_functions.py:7199` through `safe_div` mode 4.

### F8 — Earnings-surprise and revenue-surprise descriptions are swapped

**Severity:** Low  
**Status:** Open documentation bug

The formulas themselves are attached to the correct variable names:

- `saleq_su = SUR(SALE_QTR*)` is revenue surprise, not earnings surprise.
- `niq_su = SUR(NI_QTR*)` is earnings surprise, not revenue surprise.

Correct the labels at `documentation.tex:1816-1818` (including the “Surprrise” typo). Python and SAS calculations agree.

### F9 — Turnover-volatility documentation places the `1,000,000` scale on the wrong side

**Severity:** Low  
**Status:** Open documentation bug

Both codebases calculate daily turnover as:

```text
TVOL / (SHARES * 1,000,000)
```

and then calculate `std(daily_turnover) / mean(daily_turnover)`. The documentation prints `(TVOL / SHARES) * 1,000,000` in the numerator while referring to the correctly scaled `TURNOVER` in the denominator, which would inflate the stated coefficient of variation by `10^12`. Correct the parentheses at `documentation.tex:2290`.

## Formula families with no material mismatch found

The following groups agree between current Python and production SAS after accounting for ordinary SAS/Polars null and floating-point behavior:

- One- and three-year level growth.
- One- and three-year changes scaled by current assets.
- Profit margins, returns on assets/equity/book enterprise value, issuance, payout, leverage, solvency, and most liquidity/efficiency ratios.
- Piotroski F-score.
- Altman Z-score coefficients and signs.
- Kaplan-Zingales index coefficients and signs.
- Intrinsic value and `ival_me` scaling.
- Monthly dividend yield, change in shares, net equity payout, momentum/reversal, and seasonality.
- CAPM/FF3/HXZ regression outputs, downside beta, Dimson beta, Amihud, price-to-high, zero trades, turnover, dollar volume, and market correlation.
- Mispricing composites and QMJ input direction handling, except for the upstream production-SAS issue in F1.

This statement means no material formula mismatch was found in the reviewed source. It is not a claim of bit-for-bit output equality under different input snapshots, duplicate resolution, or floating-point libraries.

## Validation performed

The focused suite completed successfully:

```text
236 passed
```

Test files:

- `tests/unit/test_accounting_formulas.py`
- `tests/unit/test_scaling_ratios.py`
- `tests/unit/test_rolling_daily_metrics.py`
- `tests/unit/test_quality_minus_junk.py`

The historical 402-characteristic comparison gives:

- Minimum Spearman correlation: 0.9944.
- Median Pearson correlation: effectively 1.0.
- The known low-Pearson outlier cases are residual momentum (outlier sensitivity), `sale_emp`, and `bidaskhl_21d`, as already described in the migration release notes.

## Recommended remediation order

1. Fix production SAS F1 (`&horizon.`) and F2 (`__capex_sale`).
2. Ask maintainers to resolve the `nix_sale` legacy-parity versus standardized-definition ambiguity; if `nix_x` is authoritative, change Python and SAS together and add a coverage regression test.
3. Correct documentation F5, F7, F8, and F9.
4. Decide the canonical `lnoa_gr1a` denominator, then update documentation and, if appropriate, both executable implementations together.
5. Preserve the current O-score regression test and add cross-firm boundary tests for all long-horizon lag formulas.
