# May–June 2026 production rerun analysis

**Run:** `run-20260727-rerun2/production`  
**Compared with:** `research.dbo.dailyreturnsproduction` and `research.dbo.characteristicsproduction`  
**Countries:** CAN, DEU, FRA, GBR, HKG, IND, ITA, JPN, NOR, USA  
**Research extraction:** 2026-07-27 18:34 UTC  
**Tolerance:** material when `abs(S3 - Research) > max(1e-8, 1e-6 * max(abs(S3), abs(Research)))`; a null on only one side is material.

## Executive answer

The rerun and Research use **the same stable internal security universe to about 99.87%, but not exactly the same composition or exact characteristic values**.

- Daily: 925,620 common keys out of 926,743 in the union (99.879%); 998 S3-only and 125 Research-only rows. Monthly: 43,763 common keys out of 43,820 (99.870%); 43 S3-only and 14 Research-only rows.
- Neither source has duplicate primary keys. No one-sided row can be remapped to a different internal ID by CUSIP, ISIN, SEDOL, or monthly `gvkey+iid`. The composition gaps are source/filter/as-of differences, not duplicate securities or broken internal-ID mapping.
- All seven daily numeric columns have Pearson and Spearman correlations above 0.999. Outside `ret_exc_dollar`, only 196 material daily value cells exist across the other six numeric columns.
- `ret_exc_dollar` differs on all 861,650 finite common daily rows because this rerun used monthly RF 0.0031 while Research implies 0.0029. The same 20 bp monthly gap affects all 43,581 finite monthly `ret_exc` rows. The rerun is internally consistent with its recorded FF snapshot; Research needs the same snapshot to compare exactly.
- Of 438 monthly numeric columns with defined Pearson correlations, 350 (79.9%) are at least 0.99 and 388 (88.6%) are at least 0.90. Five invariant flag columns have undefined correlation but no material differences. Thirty-six columns have Pearson below 0.50, yet almost all retain very high Spearman correlation because a few extreme Research observations dominate Pearson.
- Final scores are statistically very close, but **not identical account characteristics**. Pearson correlations range from 0.9885 for `mispricing_mgmt` to 0.99999 for `z_score`; QMJ and its components are 0.9941–0.9960. Rank-based scores shift slightly for nearly every security whenever the country-month universe or any input changes. A small set has genuine rank reversals caused by accounting-vintage or return-history differences.
- The previous daily RF precision bug and point-in-time identifier bug are fixed in this rerun. The earlier large missing-accounting block is mostly fixed: there are now 44 security-months where all four base accounting fields are S3-null and Research has a value, versus the prior report's 216-security block. All 44 are May only and 43 receive accounting in S3 in June. Nine of those 44 have frozen accounting inputs whose recorded availability predates May; BGIN and GrowHub also retain an older May vintage despite eligible frozen quarterly rows. **Update 29-07-2026: fully traced and resolved — see §11.** Five of the nine are correct point-in-time behavior, not a defect; four (plus BGIN/GrowHub) were two real pipeline bugs, both now fixed.

## 1. Run and comparison integrity

The run completed successfully with `bypass_crsp=true`, `compustat_source=xpressfeed`, `reuse_raw=false`, and end date 2026-06-30. It ran from 15:00:27 to 16:55:42 UTC. The run summary is [here](D:/jkp-full-run-20260727/run_logs/20260727T150027529839Z-pid7/run_summary.json), and its FF snapshot is [here](D:/jkp-full-run-20260727/source_snapshot_manifest.json).

No error, exception, traceback, or warning was found in the saved container/run logs, and `run_summary.json` has `error=null`. Resource use was high but completed without an OOM or failed phase, so the discrepancies below are data/logic differences rather than evidence of a partially failed run.

The S3 and Research schemas align exactly by column name: 13 daily columns and 455 monthly columns. Numeric comparison covers all 7 daily numeric value columns and all 443 comparable monthly numeric non-key columns, including integer flags, composites, ranks, and scores. Keys are `(excntry,date,id)` for daily and `(excntry,eom,id)` for monthly.

All counts, joins, tolerances, correlations, null directions, top records, and RF inferences were recomputed from the saved S3 CSVs and a fresh read-only Research extraction. The previous report was not used as comparison input; it was consulted only after completion for the regression section.

Full extraction lineage, S3 keys, row counts, table schemas, and the Research server timestamp are in [extraction_metadata.json](report_codex_27-07-2026_run_analysis_artifacts/raw/extraction_metadata.json). The exact aggregate counts are in [analysis_summary.json](report_codex_27-07-2026_run_analysis_artifacts/analysis_summary.json).

## 2. Universe coverage

### Daily rows

| Country | May S3 / Research / common / S3-only / R-only | June S3 / Research / common / S3-only / R-only |
|---|---:|---:|
| CAN | 35,388 / 35,235 / 35,233 / 155 / 2 | 38,766 / 38,652 / 38,649 / 117 / 3 |
| DEU | 15,954 / 15,919 / 15,919 / 35 / 0 | 16,714 / 16,692 / 16,692 / 22 / 0 |
| FRA | 14,434 / 14,415 / 14,415 / 19 / 0 | 15,104 / 15,085 / 15,085 / 19 / 0 |
| GBR | 24,966 / 24,935 / 24,935 / 31 / 0 | 25,727 / 25,697 / 25,697 / 30 / 0 |
| HKG | 51,804 / 51,804 / 51,804 / 0 / 0 | 54,611 / 54,611 / 54,611 / 0 / 0 |
| IND | 110,417 / 110,403 / 110,403 / 14 / 0 | 116,288 / 116,293 / 116,288 / 0 / 5 |
| ITA | 8,462 / 8,427 / 8,427 / 35 / 0 | 8,767 / 8,746 / 8,746 / 21 / 0 |
| JPN | 82,191 / 82,136 / 82,136 / 55 / 0 | 85,672 / 85,596 / 85,592 / 80 / 4 |
| NOR | 5,787 / 5,763 / 5,763 / 24 / 0 | 6,090 / 6,061 / 6,061 / 29 / 0 |
| USA | 102,017 / 101,896 / 101,842 / 175 / 54 | 107,459 / 107,379 / 107,322 / 137 / 57 |

### Monthly security-month rows

| Country | May S3 / Research / common / S3-only / R-only | June S3 / Research / common / S3-only / R-only |
|---|---:|---:|
| CAN | 1,784 / 1,782 / 1,780 / 4 / 2 | 1,769 / 1,764 / 1,762 / 7 / 2 |
| DEU | 761 / 760 / 760 / 1 / 0 | 761 / 760 / 760 / 1 / 0 |
| FRA | 688 / 687 / 687 / 1 / 0 | 689 / 687 / 687 / 2 / 0 |
| GBR | 1,191 / 1,190 / 1,190 / 1 / 0 | 1,171 / 1,170 / 1,170 / 1 / 0 |
| HKG | 2,474 / 2,474 / 2,474 / 0 / 0 | 2,495 / 2,495 / 2,495 / 0 / 0 |
| IND | 5,267 / 5,267 / 5,267 / 0 / 0 | 5,299 / 5,304 / 5,299 / 0 / 5 |
| ITA | 403 / 401 / 401 / 2 / 0 | 403 / 400 / 400 / 3 / 0 |
| JPN | 3,919 / 3,916 / 3,916 / 3 / 0 | 3,905 / 3,903 / 3,903 / 2 / 0 |
| NOR | 276 / 275 / 275 / 1 / 0 | 278 / 277 / 277 / 1 / 0 |
| USA | 5,131 / 5,124 / 5,119 / 12 / 5 | 5,142 / 5,141 / 5,141 / 1 / 0 |

Every one-sided monthly record is in [monthly_universe_discrepancies.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_universe_discrepancies.csv); all daily rows are in [daily_universe_discrepancies.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_universe_discrepancies.csv), condensed into traceable security runs in [daily_one_sided_security_runs.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_one_sided_security_runs.csv). [duplicate_keys.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/duplicate_keys.csv) contains no records.

### Why the universe differs

The evidence supports point-in-time exchange/status and source coverage as the main causes:

- S3-only BlackRock Smaller Companies Trust (`320404701`, GBR) and SDM SE (`335087601`, DEU) each have all 43 May–June trading rows in current XpressFeed. Vantage Drilling (`301415501`, NOR) also has 43 rows and became inactive only on 2026-07-11. S3 therefore includes valid May–June histories that Research's loaded universe omits.
- S3-only Groupe Media 6 (`323873601`, FRA) has 38 source rows through its 2026-06-23 last trade and a 2026-06-24 deletion date. The inclusion is consistent with point-in-time eligibility; a current-status filter can wrongly remove it from historical June.
- Research-only Canadian IDs `200507319` and `200854901` are legacy `19C`/`01C` cross-listings marked inactive in the security header. The source has only one May–June observation for each, whereas active S3-only Canadian issues such as Scandium Canada, Errington Metals, BCM Resources, and Saga Metals have 42 observations. This is filtering of inactive/legacy issues, not an ID mismatch.
- Five Research-only India June IDs—Jivial Industries, Riyaasat Lifestyle, Advit Jewels, Shreedhar Spinners, and Waterways Leisure—are active in the security header but have zero May–June rows in the XpressFeed `g_secd` source queried by this rerun. Research evidently retained them from a different/older feed snapshot; S3 cannot calculate them from the selected source.
- The remaining USA, Japan, Italy, France, and Canada one-sided records follow the same source/filter pattern. No same-date market identifier maps an S3-only row to a Research-only row under another `id`.

The source evidence is in [source_universe_trading_counts.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/source_universe_trading_counts.csv), [source_universe_examples.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/source_universe_examples.csv), [source_universe_domestic_examples.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/source_universe_domestic_examples.csv), and [source_universe_security_history.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/source_universe_security_history.csv).

### Month-over-month composition

Monthly additions/removals from May to June are:

| Country | S3 added / removed | Research added / removed |
|---|---:|---:|
| CAN | 4 / 19 | 3 / 21 |
| DEU | 5 / 5 | 5 / 5 |
| FRA | 2 / 1 | 2 / 2 |
| GBR | 4 / 24 | 4 / 24 |
| HKG | 23 / 2 | 23 / 2 |
| IND | 62 / 30 | 67 / 30 |
| ITA | 2 / 2 | 2 / 3 |
| JPN | 8 / 22 | 8 / 21 |
| NOR | 3 / 1 | 3 / 1 |
| USA | 45 / 34 | 40 / 23 |

S3 added 158 and removed 140 securities; Research added 157 and removed 132. The exact daily and monthly ID lists are in [month_over_month_universe_changes.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/month_over_month_universe_changes.csv).

## 3. Identifier and classification reconciliation

Internal identifiers are stable: no common-key `id`/`gvkey` mismatch and no remap candidate or collision was found. The large raw mismatch count is mostly null/format policy, not incorrect security identity.

- Daily has 570,046 identifier mismatch cells. Of these, 566,092 (99.3%) are SEDOL/ISIN populated by S3 but null in Research for all common USA and Canada securities. CUSIP differs on 2,234 rows across 145 IDs. Non-North-American ISIN/SEDOL value differences are limited to a small set of corporate-action histories.
- Monthly has 41,211 identifier/classification mismatch cells. `iid` accounts for 10,248 USA cells because S3 preserves `01`/`04` and Research stores `1`/`4`; the issue keys are semantically identical. Another 27,604 cells are USA/Canada `isin_orig` or SEDOL populated only by S3.
- Monthly `isin` differs on 2,700 cells, mainly because S3 prefers the real point-in-time ISIN while Research synthesizes country plus CUSIP when `isin_orig` is blank. Company-name differences (174 cells/91 IDs) are current-name versus stale-name changes, such as Potentially AI versus Tiger Alpha. SIC/NAICS/FF49 differences are concentrated in 91 old Research classifications, mainly Japan and USA.

The rerun now resolves `comp.sec_id_history` intervals as of the observation date, so the prior current-stamping defect is fixed. Research still has null or stale metadata, and recent history snapshots can themselves lag. This matters for external ISIN/CUSIP joins, but not for characteristics, which use stable internal keys.

See [daily_identifier_mismatch_summary.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_identifier_mismatch_summary.csv), [monthly_identifier_mismatch_summary.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_identifier_mismatch_summary.csv), and the complete [daily](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_identifier_and_classification_mismatches.csv) and [monthly](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_identifier_and_classification_mismatches.csv) record files.

## 4. Daily values and risk-free rate

| Column | Finite pairs | Pearson | Spearman | P95 absolute difference | Max absolute difference | Material values | Null mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ret` | 861,650 | 0.999994 | 0.999982 | 4.44e-10 | 0.116651 | 71 | 16 |
| `ret_dollar` | 861,650 | 0.999994 | 0.999983 | 4.53e-10 | 0.117068 | 71 | 16 |
| `ret_exc_dollar` | 861,650 | 0.999994 | 0.999983 | 9.5242e-6 | 0.117059 | 861,650 | 16 |
| `prc_open` | 819,725 | 1.000000 | 1.000000 | 0 | 0.25 | 11 | 3 |
| `prc_high` | 832,528 | 1.000000 | 1.000000 | 7.11e-15 | 0.60 | 15 | 1 |
| `prc_low` | 832,528 | 1.000000 | 1.000000 | 7.11e-15 | 28.20 | 15 | 1 |
| `prc_close` | 925,620 | 1.000000 | 1.000000 | 7.11e-15 | 0.60 | 13 | 0 |

### Why every excess return differs

For both months and every country, S3 implies monthly RF 0.0031 and Research implies 0.0029. Daily, that is approximately 0.0031/21 versus 0.0029/21, a median difference of 9.52381e-6 per trading day. The run manifest records `ff.factors_monthly`, maximum date 2026-05-01, latest RF 0.0031; the June calculation uses the last-available-month fallback. Research therefore used a different FF snapshot/value.

This rerun's daily RF now round-trips to exactly 0.0031, so the prior Decimal-scale bug that produced 0.00315 is fixed. The remaining RF discrepancy is not calculation drift inside the rerun; it is input-snapshot drift against Research. Exact country/month evidence is in [daily_implied_rf_summary.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_implied_rf_summary.csv) and [monthly_implied_rf_summary.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_implied_rf_summary.csv).

All daily material records are in [daily_material_numeric_discrepancy_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_material_numeric_discrepancy_records.csv); the compact statistics and trace examples are in [daily_numeric_column_comparison.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_numeric_column_comparison.csv) and [daily_numeric_top_differences.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_numeric_top_differences.csv).

## 5. All monthly numeric columns

There are 1,413,885 material finite-value cells and 34,313 asymmetric-null cells across 443 columns. This large count is driven by the strict tolerance and by rank/model columns that change for most rows when an input or the ranking universe changes; it does not mean that 1.4 million values are economically far apart.

| Pearson band | Columns | Interpretation |
|---|---:|---|
| >= 0.999 | 263 | Effectively the same distribution apart from isolated records/rounding |
| 0.99 to <0.999 | 87 | Very high agreement |
| 0.90 to <0.99 | 38 | High agreement, often refreshed model/rank inputs |
| 0.50 to <0.90 | 14 | Review outliers and broad input shifts |
| <0.50 | 36 | Mostly Pearson distortion from a few extreme records |
| Undefined | 5 | Invariant flags (`comp_tpci`, `primary_sec`, `exch_main`, `obs_main`, `common`); no material values differ |

Only `adjfct` (Spearman 0.634) and `eqnpo_1m` (0.755) have Spearman below 0.90. `adjfct` differs only on corporate-action records (P95 difference zero, maximum 9). `eqnpo_1m` also has near-zero P95 difference but isolated large changes.

Representative columns show why Pearson alone is misleading:

| Column | Pearson | Spearman | P95 abs diff | Max abs diff | Explanation |
|---|---:|---:|---:|---:|---|
| `taccruals_ni` | -0.925 | 0.983 | 4.55e-9 | 119,785 | Near-zero denominators and a few accounting-vintage outliers |
| `oaccruals_ni` | -0.037 | 0.988 | 3.53e-9 | 15,943 | Same denominator problem |
| `ret_48_12` | 0.098 | 0.997 | 4.53e-10 | 31,138,499 | Isolated total-return/corporate-action histories |
| `rvol_252d` | 0.027 | 0.999995 | 4.27e-7 | 825.62 | ADOMOS/Europlasma extreme Research histories |
| `beta_252d` | 0.040 | 0.998782 | 0.000603 | 2,530.05 | Extreme Research histories plus small broad RF/input changes |
| `beta_dimson_21d` | 0.250 | 0.999844 | 0.001305 | 2,006.26 | Zenergy outlier dominates Pearson |
| `resff3_6_1` | 0.851 | 0.995551 | 0.0822 | 81.16 | Broad refreshed daily/RF factor residuals plus outliers |
| `assets` | 1.000000 | 0.999847 | 3.12e-7 | 4,970.71 | Mostly exact; filing-vintage/null exceptions |

The complete all-column statistics are in [monthly_numeric_column_comparison.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_column_comparison.csv), with country and month breakouts in [monthly_numeric_comparison_by_country.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_comparison_by_country.csv) and [monthly_numeric_comparison_by_month.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_comparison_by_month.csv). Every material cell is in [monthly_material_numeric_discrepancy_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_material_numeric_discrepancy_records.csv); up to 25 largest records per column are in [monthly_numeric_top_differences.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_top_differences.csv).

## 6. Final scores and composites

| Score | Finite pairs | Pearson | Spearman | MAE | P95 abs diff | Max abs diff | Material values | Null mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `f_score` | 30,588 | 0.990645 | 0.990782 | 0.0293 | 0 | 5.000 | 601 | 90 |
| `o_score` | 33,176 | 0.999964 | 0.996501 | 0.0431 | 4.96e-10 | 161.405 | 1,095 | 96 |
| `z_score` | 32,820 | 0.999989 | 0.998020 | 0.1375 | 4.13e-9 | 1,594.229 | 1,120 | 119 |
| `mispricing_mgmt` | 39,111 | 0.988523 | 0.988578 | 0.00779 | 0.0387 | 0.9130 | 39,055 | 54 |
| `mispricing_perf` | 42,915 | 0.993632 | 0.994334 | 0.00337 | 0.00576 | 0.9911 | 42,517 | 16 |
| `qmj` | 34,433 | 0.996006 | 0.996006 | 0.01636 | 0.03885 | 3.3434 | 31,807 | 105 |
| `qmj_prof` | 39,498 | 0.994684 | 0.994684 | 0.01823 | 0.03700 | 3.2931 | 37,872 | 33 |
| `qmj_growth` | 34,449 | 0.994099 | 0.994099 | 0.01628 | 0.02953 | 3.4215 | 31,942 | 107 |
| `qmj_safety` | 40,893 | 0.995552 | 0.995552 | 0.01349 | 0.02676 | 3.4498 | 38,354 | 28 |

Answering the central question: **the score universe and signal are overwhelmingly the same, but the saved S3 rows are not exact copies of Research characteristics**. QMJ and mispricing are country-month percentile/rank composites. A changed RF, revised return history, updated accounting record, or one added/removed security changes the rank denominator and therefore slightly changes almost every score even when the underlying raw values are close.

Traceable large cases:

| Record | Difference | What happened and why |
|---|---|---|
| Ethero, FRA, both months, `328261501` | QMJ about -1.615 S3 vs +1.727 Research | S3 has assets 11.168 and book equity 1.692 from the FY2025 row available 2026-05-15; Research leaves them null while retaining sales/NI. Missing profitability/growth inputs reverse the rank. |
| Plenum, DEU, May, `322682401` | QMJ +1.398 vs -1.361; F-score 8 vs 3 | S3 May uses FY2024 inputs (assets 14.416, NI +1.037); Research May uses FY2025 (assets 10.931, NI -1.920), whose availability is 2026-06-09. S3 changes to the FY2025 values in June. Research has restated May with June information. |
| BGIN Blockchain, USA, May, `105142301` | `mispricing_mgmt` 0.180 vs 0.864; `mispricing_perf` 0.930 vs 0.044 | S3 May uses an older quarterly vintage (assets 194.853, sales 302.278, NI +65.932); Research uses FY2025 (92.836, 67.396, -176.858). The frozen FY2025 quarterly row is marked available 2026-04-24, but S3 adopts it only in June. This is a domestic quarterly attachment/selection defect candidate, not explained by May as-of timing. |
| GrowHub, USA, May, `105186301` | `mispricing_mgmt` 0.063 vs 0.976; O-score 5.80 vs 47.47 | S3 May uses the older statement (assets 3.217, NI -1.728); Research uses FY2025 (1.022, -13.373). The frozen quarterly row is marked available 2026-05-15, but S3 switches only in June. This has the same likely domestic quarterly attachment/selection cause as BGIN. |
| NewCelX, USA, May, `132113401` | QMJ -1.400 vs +1.111; `mispricing_perf` 0.061 vs 0.990 | S3 May uses the 2025-Q2 accounting vintage; Research uses FY2025 values with 2026-06-02 availability. S3 equals that newer base record in June. |
| Halo Minerals, GBR, May, `328856703` | `qmj_growth` -1.723 vs +1.699 | S3 May uses FY2024 assets 0.0325/NI -0.612; Research uses FY2025 assets 5.750/NI -2.222, available 2026-06-11. S3 changes in June. |
| ADOMOS, FRA, both months, `327569001` | `mispricing_perf` 0.0044 vs 0.9955; `qmj_safety` +1.722 vs -1.728 | Research has extreme return/model values (`rvol_252d` about 824–826 and `beta_252d` about 2,464–2,531) while S3 is economically plausible (rvol about 0.07, beta about 0.7–0.9). This is a stale/unadjusted Research return history, not an RF-only drift. |
| Potentially AI, GBR, May, `323724401` | `qmj_growth` +1.688 vs -1.678 | S3 carries the current name and `adjfct=0.1`; Research calls it Tiger Alpha and has a different accounting/corporate-action vintage. |
| Kendrick Resources, GBR, May, `320468502` | Z-score -39.29 vs -1,633.52 | A small denominator combined with differing accounting inputs creates an extreme Research score. Inspect the base fields before treating Pearson as a model failure. |

The exact score statistics are in [factor_score_comparison_summary.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/factor_score_comparison_summary.csv). The component-level records for these cases are in [investigated_score_component_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/investigated_score_component_records.csv) and [investigated_security_input_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/investigated_security_input_records.csv). Frozen run accounting lineage is in [frozen_accounting_lineage_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/frozen_accounting_lineage_records.csv).

## 7. Extreme outliers and low correlations

Several low-Pearson columns are explained by a handful of records rather than broad calculation disagreement:

- ELCID Investments (`334106801`, IND) has Research `ret_48_12` 31,138,552.887 in May versus S3 54.155; June is 28,325,749.777 versus 49.173. The source price jumps from 3.53 on 2024-10-28 to 236,250 on 2024-10-29 while the currently queried adjustment factors do not offset the discontinuity. The two systems' total-return histories treat this event differently. It must be reconciled against the frozen run's exact return input/corporate-action table before selecting either value.
- Eloxx (`103285801`, USA) has Research `ret_18_1=369,999` versus S3 approximately -0.999994. This extreme Research value persists from the prior report and indicates an unadjusted reverse split/return-chain discontinuity.
- ADOMOS drives the 825.62 maximum `rvol_252d` difference and 2,530 maximum `beta_252d` difference. Europlasma and Neovacs show the same French microcap/corporate-action pattern.
- Zenergy (`332108904`, DEU) has `beta_dimson_21d=-4.434` S3 versus 2,001.829 Research. One record largely explains the column's Pearson of 0.25 despite Spearman 0.99984. **Update 29-07-2026:** traced to a real, now-fixed contributing cause — see §11. Zenergy is one of six DEU micro-caps whose daily return series contained one-day stub-quote spikes (illiquid sub-cent prices bouncing between a real quote and a stale one); an undocumented, Python-only filter nulled the spike day but left the following "crash back to normal" day's equally spurious return untouched, one-sidedly contaminating rolling beta/volatility. Removed to match SAS, which applies no such filter.
- Clabo (`331949001`, ITA) has `taccruals_ni=46,025.96` S3 versus -73,759.21 Research and `oaccruals_ni=6,618.48` versus -9,324.78. The numerator-vintage difference is magnified by net income near zero. These ratios need denominator and source-statement validation, not correlation-based rejection alone.

ELCID source rows are in [source_elcid_price_history.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/source_elcid_price_history.csv). All other largest examples are directly traceable in [monthly_numeric_top_differences.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_top_differences.csv).

## 8. Remaining asymmetric accounting nulls

There are 44 May security-months where S3 has all four base fields (`assets`, `book_equity`, `sales`, `net_income`) null while Research has at least one value. All 44 are May only; 43 remain in the June universe and all 43 receive accounting values in S3 in June. This pattern is consistent with point-in-time withholding versus Research rewriting May with data present during the June/July database update.

However, nine records are not fully explained by publication timing. Their frozen annual/quarterly inputs contain non-null accounting with recorded availability before 2026-05-31, yet S3 attaches exactly the Research values only in June:

- Trade & Value (`331617101`, DEU)
- Cheops Technology (`328481101`, FRA)
- Eaux de Royan (`331551701`, FRA)
- Golden Rock Global (`335345401`, GBR; no June output row to observe the transition)
- Synagistics (`335414901`, HKG)
- Munoth Communication (`331244701`, IND)
- Gradiente Infotainment (`331318901`, IND)
- Vivanta Industries (`334349001`, IND)
- OPS Ecom (`332021901`, ITA)

The strongest likely cause is a one-month initial attachment/backfill boundary in the accounting-characteristic expansion or join, not missing source data: for the eight records observable in June, S3 June equals Research to output precision. BGIN and GrowHub show a related stale-value form of the problem: their eligible quarterly records exist before May, but S3 does not select them until June. Trace the `data_available` filter, annual/quarterly combination, `accounting_public_start`, monthly `expand`, and `(gvkey,eom) -> (gvkey,public_date)` join for these records.

**Update 29-07-2026 — traced and resolved, see §11 for the full accounting.** In short: Trade & Value, Cheops Technology, Eaux de Royan, Synagistics, and OPS Ecom are correct point-in-time behavior (their last eligible statement had genuinely expired under the 18-month staleness cap before a fresher one was public — not a defect). Golden Rock Global's and Gradiente Infotainment's May nulls are also correct; their Research values are themselves look-ahead artifacts from a report that wasn't public until June. Munoth Communication and Vivanta Industries were a real bug (`combine_ann_qtr_chars` silently dropping quarterly-only coverage) — now fixed. BGIN and GrowHub were a second, related real bug (out-of-order publication corrupting the expansion's coverage-window chaining) — also now fixed.

The complete 44 records are in [accounting_all_base_null_s3_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/accounting_all_base_null_s3_records.csv); the opposite six rows/four IDs where Research has all four fields null and S3 has data are in [accounting_all_base_null_research_records.csv](report_codex_27-07-2026_run_analysis_artifacts/tables/accounting_all_base_null_research_records.csv).

## 9. Regression against the previous report

| Prior finding | Rerun result |
|---|---|
| Five-country universe gaps | Unchanged exactly for USA/DEU/FRA/ITA/JPN: daily common 456,176, S3-only 598, Research-only 115; monthly common 21,774, S3-only 28, Research-only 5. This is expected because point-in-time/source filtering policy did not change. |
| Daily Decimal RF bug: S3 implied 0.00315 | **Fixed.** Rerun daily implied RF x21 is 0.0031 for every country/month, matching the manifest. The separate Research snapshot gap (0.0029) remains. |
| Current-stamped CUSIP/ISIN/SEDOL | **Fixed in S3.** The rerun resolves point-in-time history and tests the pre/post-reverse-split cases. Remaining mismatches are mostly Research nulls, stale metadata, and synthetic-ISIN policy. |
| Over-strict accounting availability marker | **Fixed as of 29-07-2026 (see §11).** Ethero attaches a May-15 record in May, while Plenum, NewCelX, and Halo correctly wait for June availability. BGIN and GrowHub's delay was traced to a separate bug — out-of-order publication corrupting the expansion's coverage-window chaining — and fixed. |
| Global row winning over fuller NA row | **Substantially fixed.** The rerun contains populated S3 accounting where Research is null for Ethero, Sunbelt, and several financial/dual-package cases. |
| About 216 securities with all accounting absent in S3 | **Fully resolved as of 29-07-2026 (see §11).** Current comparison has 44 May rows/44 IDs with all four base fields absent, and 43 populate in June. Of the nine flagged as unexplained, five are correct point-in-time behavior; the other four (Munoth, Vivanta, plus the related BGIN/GrowHub stale-value cases) were two real bugs, both now fixed. |
| Extreme Research corporate-action histories | **Still present.** Eloxx, ADOMOS, Zenergy and similar records continue to dominate Pearson correlations; new all-country scope adds ELCID. |

## 10. Recommended checks and fixes

2. ~~Trace the residual May accounting delays.~~ **Done 29-07-2026 — see §11.** Persist or inspect `acc_chars_world` and `world_data_prelim` for the nine all-null gvkeys plus BGIN and GrowHub, then test the `data_available` filter, NA/Global and annual/quarterly selection, monthly expansion start/end, and `public_date` join. Add a regression test that an already-eligible accounting record attaches in the first eligible month.
3. **Define Research restatement policy.** Decide whether May should remain frozen as known on May 31 or be rewritten by the June load. The present database mixes a restated May with a point-in-time S3 May; both cannot compare exactly until the policy is shared.
4. **Reconcile corporate-action total-return chains.** For ELCID, Eloxx, ADOMOS, Zenergy, Europlasma, Neovacs, and the other top records, compare the frozen run and Research daily adjustment factors around the discontinuity. Add sanity gates for daily returns, rolling volatility/beta, and cumulative returns beyond defensible bounds.
5. **Document identifier semantics.** Retain stable `id/gvkey/iid` as join keys. State that S3 CUSIP/ISIN/SEDOL are point-in-time, refresh `sec_id_history` regularly, and stop synthesizing a US-prefixed ISIN for non-US-domiciled USA listings when a real ISIN exists.
6. **Reconcile universe rejections explicitly.** Persist reason codes such as inactive current header, point-in-time exchange, no source price rows, insufficient observations, and delisted-after-period. The source evidence already explains representative cases; reason codes will explain every one-sided row automatically.
7. **Use robust acceptance metrics.** Gate on universe, null direction, median/P95 differences, Spearman, and top-record review in addition to Pearson. A single unadjusted microcap history can make Pearson near zero while 95% of values agree to floating-point precision.

## 11. Resolution status (as of 29-07-2026)

Following this report, the two open accounting threads (§8, §9) and the extreme-return outlier pattern (§7) were traced to source and, where they were genuine defects, fixed. This section is the closing accounting for that work; it does not change any number reported above, since those numbers describe this specific run.

### The nine "not fully explained" accounting-null records split into two categories

| Record | Root cause | Verdict |
|---|---|---|
| Trade & Value, Cheops Technology, Eaux de Royan, Synagistics, OPS Ecom | Their last eligible statement had genuinely expired under the 18-month staleness cap (`max_data_lag`) before a fresher report became public. No record legitimately covered May in either the annual or quarterly panel. | **Correct point-in-time behavior, not a defect.** |
| Golden Rock Global, Gradiente Infotainment | A quarterly record does legitimately cover May, but its values don't match what Research shows — Research's numbers come from a *later* report that wasn't public until June (look-ahead on Research's side). | **Correct; fixing the bug below does not make these match Research.** |
| Munoth Communication, Vivanta Industries | `combine_ann_qtr_chars` merged the annual and quarterly characteristic panels as `ann LEFT JOIN qtr`. Each panel is independently expanded to its own coverage window, and a slow-filing annual report can leave a real gap that a timelier quarterly filing doesn't share — the left join silently dropped those quarterly-only months entirely, discarding legitimately-public data. | **Real bug — fixed.** Outer join with coalesced keys preserves quarterly-only coverage; `book_equity`/`sales`/`net_income` (and for Vivanta, all three) now populate for May 2026 and match Research exactly. |

### BGIN and GrowHub — a second, related bug

Both carried a stale mid-2025 balance sheet into May 2026 despite their FY2025Q4 report being public since April/May. Cause: the accounting-panel expansion ended each record's coverage at the *next* fiscal period's start date — safe under the SAS original's plain 4-month publication lag (which is always monotone in `datadate`), but not once real availability dates are used (added 26-07-2026 to fix look-ahead): a period can be *published* after a later period's report, and the old chaining let the stale record's coverage window extend past the point where fresher data was already public. **Fixed**: a record now ends before the earliest start among all *later* records, not just the next one, and records fully superseded before their own start drop out cleanly instead of producing empty ranges. Verified directly against BGIN/GrowHub's frozen inputs — the fix reproduces the exact values this report already identified as correct (BGIN assets 92.836, GrowHub assets 1.022). Across the full frozen 2026-07-27 inputs, 99,301 quarterly and 11,173 annual records had the out-of-order-publication precondition for this bug.

### The DEU extreme-outlier cluster (§7 Zenergy, and five more)

Tracing daily returns for Zenergy and five other DEU micro-caps (illiquid, sub-cent prices) found a shared pattern: a one-day price spike (a stub quote or bad tick) followed by an equal-magnitude reversion the next day. An undocumented, Python-only guard in `gen_returns_df` nulled any return exceeding +1000%, which caught the spike day but left the reversion day's equally spurious return untouched — one-sided by construction, since a return can never be more negative than -100% so there is no symmetric floor to add. This directly explains why these show up as *null* mismatches on the S3 side (report §4/§9) rather than as large value differences, and — checked against production SAS directly — is not part of the source methodology at all: SAS applies no magnitude filter to raw returns, handling outliers exclusively via a separate winsorized column that (in both systems) rolling daily characteristics like `betabab_1260d`/`rvol_*` never consume. Confirmed harmful, not just extraneous: for one of the six (Petro Matad, gvkey 328875202), S3's own `betabab_1260d` was *more* distorted than Research's fully-unfiltered value. **Removed**, matching SAS; only genuinely undefined returns (±∞/NaN, e.g. a zero prior price) are still nulled.

### Still open, not addressed by the above

- **Extreme Research corporate-action histories** (ELCID, Eloxx, ADOMOS, Europlasma, Neovacs, Clabo) — unrelated to the DEU cluster above; these are unadjusted price discontinuities on Research's side, not something fixable in this pipeline. §10 recommendation 4 still applies.
- **Research restatement policy** (§10 recommendation 3) — undecided.
- A separate, independent formula-level audit (`FACTOR_CALCULATION_AUDIT.md`, 26-07-2026) found production SAS itself fails to null early-history rows for `gpoa_ch5`/`roe_ch5`/`roa_ch5`/`cfoa_ch5`/`gmar_ch5` (an unresolved `&horizon.` macro-variable bug), which propagates into `qmj_growth`/`qmj` — a real source of S3-vs-Research disagreement on those characteristics, but the defect is on SAS's side. The same audit found `lnoa_gr1a` has a numerator/denominator formula error shared by *both* SAS and Python relative to the cited HXZ definition — this doesn't cause S3-vs-Research disagreement (both compute it the same way) but means neither matches the academic formula. Neither has been acted on.

## Evidence index

- Universe: [daily summary](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_universe_summary.csv), [monthly summary](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_universe_summary.csv), [daily records](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_universe_discrepancies.csv), [monthly records](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_universe_discrepancies.csv), [security runs](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_one_sided_security_runs.csv)
- Identifiers: [daily summary](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_identifier_mismatch_summary.csv), [monthly summary](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_identifier_mismatch_summary.csv), [daily records](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_identifier_and_classification_mismatches.csv), [monthly records](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_identifier_and_classification_mismatches.csv)
- Numeric comparisons: [daily all columns](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_numeric_column_comparison.csv), [monthly all columns](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_numeric_column_comparison.csv), [daily material records](report_codex_27-07-2026_run_analysis_artifacts/tables/daily_material_numeric_discrepancy_records.csv), [monthly material records](report_codex_27-07-2026_run_analysis_artifacts/tables/monthly_material_numeric_discrepancy_records.csv)
- Scores and traces: [score summary](report_codex_27-07-2026_run_analysis_artifacts/tables/factor_score_comparison_summary.csv), [score components](report_codex_27-07-2026_run_analysis_artifacts/tables/investigated_score_component_records.csv), [security inputs](report_codex_27-07-2026_run_analysis_artifacts/tables/investigated_security_input_records.csv)
- Source/root-cause evidence: [frozen accounting lineage](report_codex_27-07-2026_run_analysis_artifacts/tables/frozen_accounting_lineage_records.csv), [live annual examples](report_codex_27-07-2026_run_analysis_artifacts/tables/source_accounting_annual_examples.csv), [live domestic annual examples](report_codex_27-07-2026_run_analysis_artifacts/tables/source_accounting_domestic_annual_examples.csv), [ELCID history](report_codex_27-07-2026_run_analysis_artifacts/tables/source_elcid_price_history.csv), [source universe counts](report_codex_27-07-2026_run_analysis_artifacts/tables/source_universe_trading_counts.csv)
