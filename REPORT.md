# May/June 2026 production CSV versus Research audit

## Scope

This report is intentionally limited to the two database objects specified for this test:

- saved S3 `production/monthly/{usa,deu,fra,ita,jpn}.csv` versus `research.dbo.characteristicsproduction`;
- saved S3 `production/daily/{usa,deu,fra,ita,jpn}.csv` versus `research.dbo.dailyreturnsproduction`.

The window is May 1 through June 30, 2026. Monthly rows were joined on `(eom, id)` and daily rows on `(excntry, date, id)`. Identifiers were trimmed and upper-cased before comparison. Numeric materiality is `abs(S3 - Research) > max(1e-8, 1e-6 × max(abs(S3), abs(Research)))`; asymmetric nulls are always material. Pearson correlations use finite paired values only.

No downstream score table is in scope. Neither of the two relevant tables contains final strategy-score columns, so final scores and selected portfolios are **not testable from this scope**. No score replay or comparison to another database table is included.

## Key findings

1. **No duplicate keys in either source.** Monthly has 21,802 S3 rows and 21,779 Research rows, with 21,774 common keys, 28 S3-only, and 5 Research-only. Daily has 456,774 S3 rows and 456,291 Research rows, with 456,176 common keys, 598 S3-only, and 115 Research-only.
2. **The universe gaps are related across frequencies.** The 713 daily one-sided observations form 76 security runs; every one of the 29 unique securities in the monthly one-sided set also appears in the daily one-sided set. No one-sided daily records match each other on date and ISIN/CUSIP/SEDOL under different internal IDs. This points to changed source/filter coverage, not duplicate or broken ID mapping.
3. **Stable internal keys, stale market attributes.** On common monthly keys, `id`, `gvkey`, `iid`, `eom`, country, currency, and security type match exactly. Market identifiers and descriptive/classification fields differ. Daily has 3,143 CUSIP-discrepant observations across 117 IDs and 168 ISIN/SEDOL-discrepant observations across 10 IDs, while retaining the same internal ID.
4. **Risk-free inputs differ systematically.** Monthly `ret - ret_exc` is 0.0031 in S3 and 0.0029 in Research in both months. Daily `ret_dollar - ret_exc_dollar` is 0.0001500000 in S3 and approximately 0.0001380952 in Research. Consequently, all 439,165 finite paired daily `ret_exc_dollar` observations differ by about `1.19048e-5`, despite a 0.999997 correlation.
5. **Daily prices/returns otherwise agree almost exactly.** Excluding `ret_exc_dollar`, only 159 paired numeric cells exceed the materiality threshold: 57 `ret`, 57 `ret_dollar`, and 45 OHLC cells. Price differences are confined to one German ID (`333909001`) from May 1–20. Several remaining return differences cluster on Japanese dates May 28 and June 29, consistent with refreshed distributions/corporate-action return cells or a revised return calculation.
6. **Monthly characteristics show broad downstream propagation plus isolated extreme stale values.** Of 437 comparable numeric columns, 326 have correlation at least 0.99; 51 are below 0.50 or undefined. Forty-one columns have correlation below 0.90 even though their P95 absolute difference is below `1e-6`, meaning isolated extreme Research values dominate Pearson correlation.
7. **The run used a different source snapshot from Research.** The July 26 run succeeded with `bypass_crsp=true`, `compustat_source=xpressfeed`, `reuse_raw=false`, and a June 30 end date. Its recorded FF snapshot ends May 1 with latest monthly RF 0.0031. Research's observed 0.0029 RF and stale identifiers/return histories are therefore consistent with a prior or independently loaded snapshot.

## 1. Monthly universe and composition

| Month | Country | S3 | Research | Common | S3-only | Research-only |
|---|---:|---:|---:|---:|---:|---:|
| May | USA | 5,131 | 5,124 | 5,119 | 12 | 5 |
| May | DEU | 761 | 760 | 760 | 1 | 0 |
| May | FRA | 688 | 687 | 687 | 1 | 0 |
| May | ITA | 403 | 401 | 401 | 2 | 0 |
| May | JPN | 3,919 | 3,916 | 3,916 | 3 | 0 |
| June | USA | 5,142 | 5,141 | 5,141 | 1 | 0 |
| June | DEU | 761 | 760 | 760 | 1 | 0 |
| June | FRA | 689 | 687 | 687 | 2 | 0 |
| June | ITA | 403 | 400 | 400 | 3 | 0 |
| June | JPN | 3,905 | 3,903 | 3,903 | 2 | 0 |

All 33 one-sided security-month records are in [universe_discrepancies.csv](universe_discrepancies.csv). The five Research-only rows are May USA: Eloxx Pharmaceuticals (`103285801`), Trulieve (`103421401`), Glass House Brands (`103921601`), StablecoinX (`103989302`), and Bitzero (`107515401`). Recurring S3-only rows include Smart Logistics Global, SDM SE, Next RE, and Enertronica Santerno.

Month-over-month active-security changes:

| Source | USA add/remove | DEU | FRA | ITA | JPN |
|---|---:|---:|---:|---:|---:|
| S3 monthly | 45 / 34 | 5 / 5 | 2 / 1 | 2 / 2 | 8 / 22 |
| Research monthly | 40 / 23 | 5 / 5 | 2 / 2 | 2 / 3 | 8 / 21 |
| S3 daily active IDs | 45 / 31 | 6 / 6 | 2 / 1 | 2 / 2 | 8 / 22 |
| Research daily active IDs | 40 / 32 | 6 / 6 | 2 / 2 | 2 / 5 | 8 / 21 |

The full additions/removals are in [month_over_month_universe_changes.csv](month_over_month_universe_changes.csv) and [daily_month_over_month_universe_changes.csv](daily_month_over_month_universe_changes.csv). Daily and monthly counts differ slightly because a security can have at least one daily observation without surviving the month-end monthly filter.

## 2. Daily universe coverage

| Month | Country | S3 rows | Research rows | Common | S3-only | Research-only |
|---|---:|---:|---:|---:|---:|---:|
| May | USA | 102,017 | 101,896 | 101,842 | 175 | 54 |
| May | DEU | 15,954 | 15,919 | 15,919 | 35 | 0 |
| May | FRA | 14,434 | 14,415 | 14,415 | 19 | 0 |
| May | ITA | 8,462 | 8,427 | 8,427 | 35 | 0 |
| May | JPN | 82,191 | 82,136 | 82,136 | 55 | 0 |
| June | USA | 107,459 | 107,379 | 107,322 | 137 | 57 |
| June | DEU | 16,714 | 16,692 | 16,692 | 22 | 0 |
| June | FRA | 15,104 | 15,085 | 15,085 | 19 | 0 |
| June | ITA | 8,767 | 8,746 | 8,746 | 21 | 0 |
| June | JPN | 85,672 | 85,596 | 85,592 | 80 | 4 |

The largest one-sided runs are:

| Side | Country | ID | Observations | Date span | Identifier |
|---|---|---:|---:|---|---|
| S3-only | DEU | 335087601 | 43 | May 1–Jun 30 | `DE000A3CM708` |
| S3-only | USA | 105077301 | 41 | May 1–Jun 30 | CUSIP `G82195101` |
| Research-only | USA | 103921601 | 26 | May 21–Jun 29 | CUSIP `377130406` |
| Research-only | USA | 103989302 | 24 | May 21–Jun 25 | CUSIP `85238K100` |
| S3-only | USA | 103595801 | 23 | May 21–Jun 24 | CUSIP `007025869` |
| S3-only | USA | 103926202 | 22 | May 21–Jun 23 | CUSIP `251936209` |
| S3-only | USA | 104346401 | 22 | May 21–Jun 23 | CUSIP `G6781A128` |
| S3-only | FRA | 323873601 | 21 | May 26–Jun 23 | `FR0000064404` |

All 713 records are in [daily_universe_discrepancies.csv](daily_universe_discrepancies.csv); the 76 condensed security runs are in [daily_one_sided_security_runs.csv](daily_one_sided_security_runs.csv); daily country/date counts are in [daily_date_coverage.csv](daily_date_coverage.csv). The gaps are spread across dates rather than caused by an entirely missing trading day.

## 3. Identifiers and classifications

### Monthly common keys

| Field | Discrepant rows | Unique IDs | Diagnosis |
|---|---:|---:|---|
| `id`, `gvkey`, `iid`, `eom`, country, currency, type | 0 | 0 | Stable key mapping |
| `date` | 1 | 1 | BNP Paribas Funds – Global: S3 Jun 30 vs Research Jun 29 |
| `conm` | 96 | 52 | Current versus stale/former names |
| `isin` | 118 | 59 | Current versus retained identifier |
| `cusip` | 112 | 56 | Mostly US issue/reorganization changes |
| `isin_orig`, `sedol` | 6 each | 3 each | Three non-US issue identifier changes |
| `gics` | 13 | 12 | Nine Research-only values, four S3-only values |
| `sic`, `naics` | 91 each | 91 | S3 populated while Research is null, all in May |
| `ff49` | 89 | 89 | Propagates SIC/NAICS differences |
| `size_grp` | 23 | 22 | Breakpoint or market-equity snapshot difference |

Examples include ExxonMobil (`US30233Q1085` versus `US30231G1022`), T3 Defense (`US67054R3021` versus `US67054R2031`), and Genprex (`US3724464017` versus `US3724463027`). Every affected common-key record is in [canonical_identifier_and_classification_mismatch_records.csv](canonical_identifier_and_classification_mismatch_records.csv).

### Daily common keys

| Field | Value mismatches | S3 value / Research null | S3 null / Research value | Affected IDs |
|---|---:|---:|---:|---:|
| `cusip` | 3,143 | 0 | 0 | 117 |
| `isin` | 164 | 0 | 4 | 10 |
| `sedol` | 164 | 0 | 4 | 10 |

Examples include ID `100450301`, CUSIP `30233Q108` versus `30231G102` on all 41 dates, and ID `331610201`, ISIN `IT0005717571` versus `IT0005378325` on 43 dates. All records are in [daily_identifier_mismatch_records.csv](daily_identifier_mismatch_records.csv). [daily_identifier_remapped_keys.csv](daily_identifier_remapped_keys.csv) is empty: none of the one-sided records can be paired to a different internal ID by same-date market identifier. The likely issue is identifier staleness/point-in-time semantics, not an ID collision.

## 4. Monthly inputs and calculated characteristics

The monthly file contains both source-like inputs and calculated characteristics. Because the table does not store a field-level lineage/type flag, this report compares every common numeric column and diagnoses differences from column behavior and the run implementation; it does not impose an external score model.

### Correlation distribution

| Correlation band | Columns |
|---|---:|
| ≥ 0.999 | 219 |
| 0.99–0.999 | 107 |
| 0.90–0.99 | 56 |
| 0.50–0.90 | 18 |
| < 0.50 | 33 |
| Undefined | 4 |

There are 437 comparable numeric columns and 433 defined correlations. [numeric_column_comparison.csv](numeric_column_comparison.csv) contains pair counts, asymmetric nulls, correlations, MAE, median/P95/max differences, and scaled maxima. Under the stated materiality rule, [monthly_material_numeric_discrepancy_records.csv](monthly_material_numeric_discrepancy_records.csv) contains all 705,210 affected cells: 686,001 value differences and 19,209 null mismatches.

### Systematic calculated-column differences

- `ret_exc`: all 21,675 finite paired rows are different. S3 uses `ret - ret_exc = 0.0031`; Research uses 0.0029. This explains the level shift directly.
- Daily-derived/model columns with differences on most finite records include `beta_252d`, `beta_dimson_21d`, `betadown_252d`, `ivol_ff3_21d`, `iskew_ff3_21d`, `resff3_6_1`, `resff3_12_1`, `beta_60m`, and QMJ/mispricing composites. These propagate the RF, daily return, universe, and historical-window changes.
- A low Pearson correlation does not necessarily mean broad disagreement: 41 sub-0.90 columns have P95 absolute differences below `1e-6` and are dominated by one or a few extreme Research values.

### Material outliers

| ID / month | Column | S3 | Research | Difference / interpretation |
|---|---|---:|---:|---|
| Eloxx `103285801`, Jun | `ret_18_1` | -0.999994 | 369,999.0 | stale/corporate-action return history |
| Eloxx `103285801`, Jun | `ret_12_0` | 14.6166 | 12,617.1817 | same return-index problem |
| Boostheat `333403601`, Jun | `ret_6_1` | -0.2580 | 4,057.6495 | stale adjusted-return history |
| Aduro `134985401`, Jun | `ret_60_12` | 3.3010 | 274.6923 | long-window corporate-action history |
| Adomos `327569001`, May | `rvol_252d` | 0.07396 | 825.69665 | implausible stale daily-return window |
| Adomos `327569001`, May | `ivol_capm_252d` | 0.07324 | 825.23921 | propagated from same window |
| Zenergy `332108904`, May | `beta_dimson_21d` | -4.4346 | 2,001.8286 | degenerate/extreme rolling window |
| Zenergy `332108904`, May | `rmax1_21d` | 1.0038 | 122.5168 | same daily-return history |
| Global Interactive `104195801`, May | `rec_days` | 1,691,227.5 | 912.5 | accounting denominator/timing difference |
| Concorde `105053401`, May | `ival_me` | 4,558.956 | 140.155 | accounting/market-equity snapshot difference |

The exact largest record differences per column are in [numeric_top_differences.csv](numeric_top_differences.csv). Coherent accounting-input changes include Sunbelt (many S3 values where Research is null), Driven Brands (book equity 865.739 versus 780.756; net income 71.095 versus 132.073), Sony (assets 244,126.907 versus 101,315.298), Société Générale (many S3 nulls versus Research values), and OVS (sales 1,827.666 versus 2,074.449). These are consistent with different accounting observation/publication-date selection or a refreshed XpressFeed snapshot. Detailed affected columns are in [investigated_security_differences.csv](investigated_security_differences.csv).

## 5. Daily numeric comparison

| Column | Finite pairs | Correlation | MAE | P95 abs diff | Max abs diff | Material values | Null mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ret` | 439,165 | 0.9999967 | 1.15e-6 | 4.44e-10 | 0.06017 | 57 | 15 |
| `prc_open` | 427,036 | 1.0000000 | 2.81e-6 | 0 | 0.25 | 11 | 3 |
| `prc_high` | 430,415 | 1.0000000 | 6.85e-6 | 0 | 0.60 | 13 | 1 |
| `prc_low` | 430,415 | 1.0000000 | 1.63e-6 | 0 | 0.15 | 9 | 1 |
| `prc_close` | 456,176 | 1.0000000 | 5.04e-6 | 0 | 0.60 | 12 | 0 |
| `ret_dollar` | 439,165 | 0.9999967 | 1.15e-6 | 4.50e-10 | 0.06017 | 57 | 15 |
| `ret_exc_dollar` | 439,165 | 0.9999967 | 1.30e-5 | 1.19e-5 | 0.06018 | 439,165 | 15 |

All 439,374 material/null-mismatch cells are in [daily_material_numeric_discrepancy_records.csv](daily_material_numeric_discrepancy_records.csv); the largest 100 per column are in [daily_numeric_top_differences.csv](daily_numeric_top_differences.csv).

Specific non-RF discrepancies:

- DEU ID `333909001` has 13 material return differences and all 45 OHLC value differences from May 1–20. Maximum return difference is -0.06017 on May 8; S3 close is 8.00/8.05 around the period where Research carries 7.45–8.00. Values converge after May 20. This is a refreshed historical quote/return segment, not a general calculation failure.
- USA ID `131672601`, June 12: identical price data, but S3 `ret=0.031105` and Research `ret=0`. This is a stale/recomputed return cell.
- FRA ID `320483901`, June 23: identical close 65.5, but S3 `ret=0.003759` and Research `ret=-0.015038`; June 2 also differs. The security is intermittently untraded, so the pattern is consistent with different previous-price/fill handling.
- Japanese differences are mostly isolated to May 28 (many IDs) and June 29 (several IDs), a date cluster typical of refreshed distribution/corporate-action adjustments rather than random numeric drift.

The observed daily RF differences are in [daily_implied_rf_summary.csv](daily_implied_rf_summary.csv). The code path documents monthly RF divided by 21 for daily excess returns; the observed S3 daily value implies 0.00315 before division, close to the run-manifest 0.0031 after source precision/rounding, while Research implies exactly 0.0029.

## 6. Likely causes and recommended fixes

1. **Make the CSV and Research loads atomic and versioned.** Store run ID, code commit, input snapshot hashes/max dates, and load timestamp with both tables. The current evidence is consistent with S3 from the July 26 fresh XpressFeed run and Research from an older/independent snapshot.
2. **Align and persist the RF input at full precision.** The monthly 0.0031 versus 0.0029 difference propagates to every excess return and many residual/beta/volatility characteristics. Record the actual monthly RF value used before the daily `/21` conversion and rebuild both tables from the same value.
3. **Reconcile filtering at both daily and month-end stages.** Start with the 76 one-sided daily security runs and the 29 overlapping month-end securities. Persist rejection reasons (`primary`, exchange, common-security, observation-date, and source-availability flags) so each one-sided row has a deterministic explanation.
4. **Audit corporate-action and previous-price history for named records.** Check quote/return inputs for DEU `333909001`, USA `131672601`, FRA `320483901`, the May 28/June 29 JPN clusters, and monthly extremes such as Eloxx, Adomos, Boostheat, Zenergy, and Aduro. Add diagnostics for implausible rolling returns/volatilities.
5. **Define identifier semantics.** Decide whether historical rows carry point-in-time identifiers or the latest issue identifier. Refresh Research or change the S3 join accordingly, but continue joining by internal `id/gvkey/iid`, which are stable.
6. **Persist accounting lineage.** For each monthly accounting-derived characteristic, retain selected `datadate`, publication/availability date, and source-row version. This will explain Sunbelt/Driven Brands/Sony/SocGen/OVS differences and prevent accidental look-ahead.
7. **Add acceptance gates.** Require zero duplicate keys; exact universe reconciliation or documented rejections; exact RF equality; identifier-drift reporting; daily return/price correlation and outlier checks; all-monthly-column missingness/correlation checks; and zero unreviewed extreme calculated values.

## In-scope artifact index

- Monthly universe: [universe_summary.csv](universe_summary.csv), [universe_discrepancies.csv](universe_discrepancies.csv), [duplicate_records.csv](duplicate_records.csv), [month_over_month_universe_changes.csv](month_over_month_universe_changes.csv)
- Monthly identifiers: [canonical_identifier_and_classification_summary.csv](canonical_identifier_and_classification_summary.csv), [canonical_identifier_and_classification_mismatch_records.csv](canonical_identifier_and_classification_mismatch_records.csv)
- Monthly numeric characteristics: [numeric_column_comparison.csv](numeric_column_comparison.csv), [numeric_top_differences.csv](numeric_top_differences.csv), [monthly_material_numeric_discrepancy_records.csv](monthly_material_numeric_discrepancy_records.csv), [investigated_security_differences.csv](investigated_security_differences.csv)
- Daily universe: [daily_universe_summary.csv](daily_universe_summary.csv), [daily_universe_discrepancies.csv](daily_universe_discrepancies.csv), [daily_one_sided_security_runs.csv](daily_one_sided_security_runs.csv), [daily_date_coverage.csv](daily_date_coverage.csv), [daily_duplicate_records.csv](daily_duplicate_records.csv), [daily_month_over_month_universe_changes.csv](daily_month_over_month_universe_changes.csv)
- Daily identifiers: [daily_identifier_mismatch_summary.csv](daily_identifier_mismatch_summary.csv), [daily_identifier_mismatch_records.csv](daily_identifier_mismatch_records.csv), [daily_identifier_remapped_keys.csv](daily_identifier_remapped_keys.csv)
- Daily numeric returns/prices: [daily_numeric_column_comparison.csv](daily_numeric_column_comparison.csv), [daily_numeric_top_differences.csv](daily_numeric_top_differences.csv), [daily_material_numeric_discrepancy_records.csv](daily_material_numeric_discrepancy_records.csv), [daily_implied_rf_summary.csv](daily_implied_rf_summary.csv)
- Reproducibility scripts: [compare_daily_returns.py](compare_daily_returns.py), [export_monthly_material_numeric_discrepancies.py](export_monthly_material_numeric_discrepancies.py)
