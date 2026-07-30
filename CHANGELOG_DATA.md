# Data Changelog
This change log keeps track of changes to the underlying data set. In brackets, we highlight versions of importance. The version with _factor data set_ is the basis of the factor portfolios we upload at [https://jkpfactors.com/](https://jkpfactors.com/). The version with _paper data set_ is the basis of [Jensen, Kelly and Pedersen (2023)](https://onlinelibrary.wiley.com/doi/full/10.1111/jofi.13249).

This repository ports the original SAS pipeline ([ReplicationCrisis](https://github.com/bkelly-lab/ReplicationCrisis)) to Python using Polars. Entries up to and including 05-03-2025 are from the original change log.

## 30-07-2026
__Changes__:
- The per-country production CSVs (`processed/production/{monthly,daily}/<country>.csv`) now carry `config.PRODUCTION_OUTPUT_YEARS` (3) of history counted back from the end date, instead of the whole panel; `--production-years 0` emits everything, which a downstream re-seed needs. These files feed an appending loader that only reads rows past its own high-water mark — normally one month — so the full panel wrote, stored and shipped roughly 87% of rows that nothing read. 3 years rather than the 2 months actually consumed, so that restatements keep propagating (the loader deletes and reinserts everything the file covers) and a loader that has not run for a while still finds its overlap instead of leaving a permanent hole. **This is an output bound only: every characteristic is still computed over the full source window, so no value changes in the rows that remain** — the unit tests assert the retained rows are byte-identical to the unsliced output. The bound is applied before the volume, min-price, identifier and FX joins, so those shrink with it. The six cross-country files (`market_returns{,_daily}.csv`, the three cutoffs, `world_ret_monthly.csv`) and the `characteristics/`, `return_data/` and `accounting_data/` outputs are unchanged and still carry full history. **The downstream loader must not fall more than 3 years behind, or the append will leave a gap.**
- A run without an explicit `--start-date` now downloads a rolling window of `config.ROLLING_INPUT_YEARS` (23) years back from the end date, instead of the full history implied by `ACCOUNTING_START_DATE` (1949-12-31). Cost stays flat rather than growing a year every year, and the emitted history begins 23 years before the end date. 23 rather than the bare 20 that the longest lookback implies: `seas_16_20` requires 240 monthly *observations*, and its gate counts a security's own rows rather than calendar months, so a security with missing months needs more than 20 calendar years to reach 240. Measured on the USA monthly panel (1,849 securities mature enough to qualify), a 23-year window loses no security that a 26-year window keeps, while 22 loses one and 21 loses two. Characteristics with shorter lookbacks (60 months for the `ch5` family, `beta_60m`, `ret_60_36`; 1260 trading days for `betabab_1260d`/`corr_1260d`; 3 years for `*_gr3a`) sit far inside the window, and `age` is unaffected because it reads the unfiltered `comp_age_anchor`. An explicit `--start-date` still overrides, `--full-history` restores the previous complete-history download (`ACCOUNTING_START_DATE`) for re-seeding a downstream store or reissuing after a change that rewrites historical values, and a window shorter than 240 months now logs a warning naming the characteristics it would null. `--start-date` and `--full-history` together are rejected rather than silently resolved. **The emerging-market panels were not sampled and are likely gappier than the USA; verify before shortening `ROLLING_INPUT_YEARS` further.**
- Consolidated the production CSV outputs into a single `processed/production/` directory and removed `processed/output/`. Every per-country CSV was previously written twice — once to `processed/production/{monthly,daily}/` and again, byte-identical, to `processed/output/{CharacteristicsProduction/,}` — solely so the second copy presented the SAS directory shape. That duplicate cost roughly 95 GiB and about half the export phase on every run. `processed/production/` now holds `monthly/<country>.csv`, `daily/<country>.csv`, and the six cross-country files (`market_returns.csv`, `market_returns_daily.csv`, `nyse_cutoffs.csv`, `return_cutoffs.csv`, `return_cutoffs_daily.csv`, `world_ret_monthly.csv`), which previously sat in `processed/output/`. No cell values, columns, or row sets change — this is purely where the files land. Consumers reading `processed/output/` must repoint to `processed/production/`; note that `monthly/` and `daily/` replace the `CharacteristicsProduction/` folder and the flat per-country daily CSVs respectively.

## 28-07-2026
__Changes__:
- Removed an undocumented, Python-only guard in `gen_returns_df` that nulled any daily or monthly return exceeding +1000%. The production SAS applies no magnitude filter to raw `ret`/`ret_exc` at all — extreme returns are handled exclusively by winsorizing into a separate `ret_exc_wins` column (and, independently, by the return-cutoff clipping already applied before factor-portfolio and market-return construction in both SAS and this pipeline); rolling daily characteristics (`betabab_1260d`, `rvol_*`, and the rest of the `roll_apply_daily` family) read the raw, unwinsorized return series in both systems, so this guard was never standing in for a documented protection. It was also one-sided: it deleted a return spike but left the following day's equally spurious "crash back to normal" return untouched, which can distort rolling beta/volatility more than leaving the round-trip alone. Traced example: gvkey 328875202 (DEU) — for Petro Matad, python's own `betabab_1260d` (5.05) was *more* extreme than Research's fully-unfiltered value (1.60), evidence the guard was making some characteristics worse, not better. Raw `ret`/`ret_local`/`ret_dollar` now match SAS exactly for extreme moves; only ±∞/NaN (an undefined pct_change, e.g. from a zero prior price) are still nulled.
- Fixed `combine_ann_qtr_chars` silently dropping security-months covered only by the quarterly accounting panel. The merge inner-joined-on-the-left (`ann LEFT JOIN qtr`) via annual coverage, which is safe under the SAS original's plain publication lag because consecutive annual records' coverage windows never gap. The actual-availability guard added on 26-07-2026 broke that guarantee: a slow-filing annual report can leave a real gap in the annual panel (capped by the 18-month staleness limit on one end, delayed by its own late availability on the other) that a timelier quarterly filing does not share, and the left join discarded that month entirely instead of falling back to the quarterly value. Now an outer join with coalesced keys preserves quarterly-only coverage; the existing per-field substitution logic (prefer quarterly when present and more recent) already handles the rest unchanged. Vivanta Industries and Munoth Communication regain book_equity/sales/net_income for May 2026 that had been silently absent.
- Fixed the accounting-panel expansion for reports published out of fiscal order. Each record's coverage was ended at the next fiscal period's start date, which is safe under the plain publication lag of the SAS original but not under the actual-availability guard added on 26-07-2026: a fiscal period published *after* a later period's report (e.g. a Q3 filed after Q4) extended the preceding stale record over months where fresher data was already public, and the stale record won the overlap. BGIN Blockchain and GrowHub carried mid-2025 balance sheets into May 2026 this way despite their FY2025Q4 reports being public since April/May. A record now ends before the earliest start among all later records, and fully superseded records drop out of the panel instead of emitting empty ranges. The frozen 2026-07-27 inputs show 99,301 quarterly and 11,173 annual records with out-of-order publication where stale extension was possible.

## 26-07-2026
__Changes__:
- Removed the Bessembinder-style Compustat correction layer introduced on 23-07-2026 (decimal-shift repairs, unreliable-history filtering, and the conditional never-dividend `trfd` recovery). The pipeline now reproduces the WRDS/SAS source cells exactly.
- Restored the production return-index construction: a missing total-return factor is replaced with 1 (`coalesce(trfd,1)` daily, `coalesce(trfm,1)` monthly, as in the production SAS), so missing factors no longer null returns.
- Resolved Compustat exchange membership from `sec_history.EXCHG` at each observation date and month-end identifiers from the last trading day in the month.
- Matched the production SAS company market equity in the CRSP-bypass build: USA main-exchange listings carry the sum of `me` over all USA main-exchange listings of the company per `(gvkey, date)` (share classes and preferred issues alike), and all other Compustat rows keep `me_company=me`. This replaces the July 23 per-gvkey global sum, which double-counted international cross-listings. Also populated `ret_local_lead1m` with the same continuity guard as `ret_exc_lead1m`.
- Prevented accounting look-ahead by delaying each annual/quarterly record until the later of the normal publication lag and its actual publication month. Availability is the **earliest** publication marker (`pdate`/`rdq`, falling back to `fdate`), because `pdate` records the preliminary release and `fdate` the final filing weeks later; using the latest marker withheld statements that were already public and shrank the eligible panel relative to the production SAS. Verified against Driven Brands, OVS, Sony, and Akanda.
- Added `source_snapshot_manifest.json`, recording the exact `ff.factors_monthly` hash, date range, and latest RF used by a run.
- Chose between a company's Global and NA Compustat filings on completeness instead of always taking the Global row. Banks file the sparse `FS` format globally while their NA `INDL` filing carries capex and working capital, and a dual filer whose Global row has not caught up is a near-empty stub — so the old rule reliably discarded the richer filing, nulling `fcf_me`, `capex_abn`, `noa_at`, `noa_gr1a` and everything derived from them for ~66 dual-listed financials and ~101 companies overall. The SAS leaves this undefined (a `proc sort nodupkey` with no ORDER BY), so this is now deterministic where production was not; Global still wins when both rows are equally populated.
- Resolved production `cusip`, `isin`, and `sedol` point-in-time instead of stamping every historical row with the identifier in force today. The XpressFeed feed exposes only current identifiers, so a reverse split or SPAC completion silently rewrote identifiers on months that closed before it happened (T3 Defense reported `67054R302` for May 2026, when `67054R203` was in force). New table `comp.sec_id_history` is backfilled once from WRDS and extended each month by `sql/xpressfeed_views/capture_sec_ids.py`. `conm` has no historical source in the feed and remains current-stamped. The history also fills `isin_orig` where `g_secd` supplies none, which matters for foreign issuers listed in the US: the export otherwise fabricates `excntry[:2] + cusip`, a valid ISIN only for US-domiciled issuers. **1,250 of 5,108 US production securities (24.5%) now publish their real non-US ISIN** (Cayman 556, Canada 248, BVI 94, Israel 82, ...) instead of an invented `US...` one; the synthetic remains as a last resort, and the security header is kept as a fallback so the column never regresses to null.

## 23-07-2026
__Changes__:
- Applied the Bessembinder et al. (2023) Compustat daily-security corrections before return construction: decimal-shift repairs for North America and Global, unreliable-observation filters, never-dividend `trfd=1` recovery, and a residual screen that nulls returns above 1000%.
- Changed degenerate rolling-daily windows to return null instead of artificial extreme signals, scrubbed non-finite daily characteristics before assembly, and prevented non-finite inputs from entering QMJ ranks.
- Replaced four semantically unnecessary full joins in Quality Minus Junk with left joins, preserving its row set while avoiding Polars' 32-bit intermediate-row overflow.

## 28-06-2026
__Changes__:
- Added a CRSP-bypass build mode (`config.BYPASS_CRSP`, `jkp build --bypass-crsp`) that constructs the dataset from Compustat only, mirroring the SAS `bypass_crsp=1` path. When enabled: security files contain Compustat rows only (`source_crsp=0`); excess returns use the Fama-French risk-free rate with a last-available-month fallback instead of the CRSP 30-year T-bill; NYSE size breakpoints are identified via Compustat `exchg=11` instead of CRSP `exchcd=1`; SIC/NAICS industry codes come from Compustat only; and factor portfolios are built from Compustat (`PORTFOLIO_SETTINGS["source"]=["COMPUSTAT"]`). This mode is the new default.

## 27-04-2026
__Changes__:
- Added `mkt_vw_cap_exc` (cap-weighted excess market return) to market returns output ([#81](https://github.com/bkelly-lab/jkp-data/pull/81))

## 22-04-2026
__Changes__:
- Fixed a case-sensitivity bug in the Fama-French 49 industry classification that left some stocks unclassified ([#78](https://github.com/bkelly-lab/jkp-data/pull/78))
- Fixed non-deterministic deduplication in the Compustat preparation step, which could cause output to differ slightly between runs ([#83](https://github.com/bkelly-lab/jkp-data/pull/83))

## 14-04-2026
__Changes__:
- Default screening filters are now applied before saving output, consistent with the documentation ([#68](https://github.com/bkelly-lab/jkp-data/pull/68))

## 05-03-2025 [Factor data set]
__Changes__:
- Added 2024 data
- moved world_data_prelim from scratch to work folder
- updated market returns macro to add a capped value option
- corrected error in o-score calculation
- added end_date filter in macros saving daily and monthly returns 

## 11-03-2024 
__Changes__:
- Added 2023 data
- Updated the country classification according the latest MSCI market classification

## 03-03-2023 
__Changes__:
- Added 2022 data
- Added 'me' (market equity) and 'ret' (total return) and removed 'source_crsp' from daily return files

__Impact__:
- Replication rate: 83.2% 

## 30-06-2022 [Paper data set] 

__Changes__:
- Changed name of "Skewness" cluster to "Short-Term Reversal"

__Impact__:
- Replication rate: 82.4% 

## 08-02-2022 

__Changes__:
- Fix error in the construction of intrinsic_value. Previously, we failed to scale intrinsic_value by market equity as done in Frankel and Lee (1998). We call the new characteristic ival_me and keep intrinsic_value in the data set. The alpha of the new factor based on ival_me is significantly different from zero, while the factor based on intrinsic_value is insignificant.

__Impact__:
- Replication rate: 82.4% (added 2020 data)


## 16-11-2021 

__Changes__:
- Changed return cutoffs to depend on all stocks, instead of only stocks from CRSP.
- Added monthly and daily returns to the output folder. 
- Changed the 'source' (character) column to 'source_crsp' (integer),. source_crsp is 1 if CRSP is the return data source.
- Changed the 'id' column from character to integer. For stocks from CRSP, the id is just their permno. For stocks from Compustat, the first digits is 1 if the stocks is traded on a US exchange, 2 if it's traded on a Canadian exchange, and 3 otherwise. The next two digits are the IID from Compustat, and the remaining six digits are the gvkey.  
- Adapted the primary_sec column such that all observations from CRSP have primary_sec=1. 
- Previously, we treated a zero return as a missing observation. Now, we have removed this screen, such that a zero return is treated like any other return. 
- Previously, we winsorized daily returns, market equity, and dollar volume, before creating charactersitics based on daily stock market data. Now, we have removed this winsorization, and daily characteristics are based on the raw data. 
- Added the option to create daily factor return in the portfolios.R code.
- Added the option to create industry returns in the portfolios.R code.

__Impact__:
- Replication rate: 83.2%

## 27-08-2021 

__Changes__:
- Fixed a bug regarding how daily delisting returns from CRSP is incorporated.
- Added indfmt='FS' to the international accounting data. 

__Impact__:
- Replication rate: 83.2%

## 14-06-2021 

__Changes__:

- We changed the winsorization scheme. First, we removed the 0.01%/99.9% winsorization of market equity in all countries. Second, we removed the winsorization of returns from the CRSP database. For Compustat returns, we set returns above (below) the 99.9% (0.01%) of CRSP returns in the same month, to that level. In other words, we base our winsorization of Compustat data on CRSP data from the same month. 
- We made several changes to the code for easier usability. Notably, the updated `main.sas` file returns a zip folder called "output" in the scratch folder, which contains all data neccesary to re-produce the results in the paper.  

__Impact__:

- Replication rate: 83.2%
- The revisions impacted all factors slightly, but the overall results are qualitatively very similar. 

## 02-19-2021 

__Changes__:

- Previously we did not exclude securities that are only traded over the counter. In the new version of the data set, we include an indicator column "exch_main" to exclude non-standard exchanges. In the US, the main exchanges are AMEX, NASDAQ and NYSE. Outside of the US, we exclude over the counter exchanges, stock connect exchanges in China and cross-country exchanges such as BATS Chi-X Europe. The documentation includes a full list of the excluded exchanges.  
- Included SIC, NAICS and GICS industry codes.

__Impact__:

- Replication rate: 84.0%. 
- Excluding non-standard exchanges mainly affected the US. By December 2019, the number of stocks in the US dropped from 5,256 to 4,102 (-22%) after adding the new 'exch_main' screen. The excluded securities are mainly tiny stocks traded over the counter, so the aggregate market cap only dropped by 2%. The change also mostly affected post 2000 data, because over the counter observations in Compustat are very rare before this point in time.    
- The change had a small effect outside of the US, because of our 'primary_sec' screen. It's very rare for Compustat to identify a security traded on a non-standard exchange as the primary security of a firm. 
- Because the changes mainly affected tiny stocks, our results did not change much. Across the 153 factors in the US, Developed and Emerging regions, the change in posterior monthly alpha ranged from -0.06% to +0.07.

## 02-15-2021

__Changes__:

- A bug caused _ivol_ff3_21d_, _iskew_ff3_21d_, _ivol_hxz4_21d_ and _iskew_hxz4_21d_ to require 17 (ff3) and 18 (hxz) observations for a valid estimate. Consistent with our original intent, we now require at least 15 observations for a valid estimate.    

__Impact__:

- Replication Rate: 84.0%. 
- The changes had a negligible effect on the affected factors.

## 02-01-2021 

__Changes__:

- Fixed a small bug in the bidask_hl() macro.
- When creating asset pricing factors (FF and HXZ), we previously required at least 5 stocks in a sub-portfolio (e.g. small stocks with high BM) for the observation to be valid. This led to missing observation in the 1950's for small stocks with low bm. We lowered this requirement to at least 3 stocks. Furthermore, when creating asset pricing factors, we changed the breakpoints to be based on NYSE stocks in the US instead of non-microcap stocks. Outside of the US, breakpoints are still based on non-microcap stocks.
  
__Impact__:

- Replication Rate: 84.0% 
- The _bidaskhl_21d_ factor changed slightly but is still significantly negative in all regions. The US factor IR changed from -0.11 to -0.09.
- The change in asset pricing factor generally didn't affect the results much.

## 01-25-2021
__Changes__:

- Changed residual momentum characteristics (resff3_12_1 & resff3_6_1) to be scaled with the standard deviation of residuals consistent with Blitz, Huij and Mertens (2011). 
- Fixed error in creating _qmj_prof_. The issue was that the _oaccruals_at_ used the value instead of the z-score of ranks. This effectively meant that accruals didn't impact the profitability score. 
- Fixed error for annual seasonality characteristics (factor names starting with seas_ and ending with _an). There was a bug in the screening procedure which meant that the characteristic for one stock could use information from an unrelated stock. 
- Rounding issues when converting a .csv file to an excel file, caused the zero_trades_* variables to not have any decimals which made the turnover tie-breaker ineffective.
- Standardized unexpected earnings (niq_su) and sales (saleq_su) is computed as the actual value minus the expected value (standardized by the standard deviation of this change). Before, the expected value was computed as the mean yearly change over the last 8 quarters added to the last quarterly value. Now the expected value is the same mean yearly change, but added to the quarterly value 4 quarters ago consistent with Jegadeesh and Livnat (2006).
  
__Impact__:

- Replication Rate: 84.0%
- The change to the residual momentum variables, made them slightly weaker. As an example, the monthly OLS information ratio of the US resff3_12_1 factor dropped from 0.33 to 0.28. 
- The _qmj_prof_ change made _qmj_prof_ and _qmj_ slightly stronger. As an example, the monthly OLS information ratio of the US _qmj_prof_ factor increased from 0.16 to 0.22.  
- The seasonality fix didn't have a large qualitative impact for the US factors, but did have a large positive effect outside of the US. As an example, the OLS IR of the developed market _seas_11_15an_ factor changed from -0.06 to 0.11.   
- The zero_trades where missing in the developed market because of too few non-missing observations. The developed market zero trades factors are generally strong and the IR ranges from 0.07 to 0.20. Similarly, The Emerging market zero trades factors where slightly negative before. After, the factors are strong with IRs ranging from 0.14 to 0.17. The US market zero trades factors improved slightly. The IR of zero_trades_21d has the most notable increase from 0.05 to 0.09.   
- The standardized unexpected sales (saleq_su) variable went from a significant IR of 0.12 to an insignificant IR of 0.05. This explains the drop in the replication rate. On the other hand, niq_su increased from 0.11 to 0.19.

## 01-15-2021 
__Changes__:

  - Base data set used in the first online version of Jensen, Kelly and Pedersen (2021).
  
__Impact__:

- Replication Rate: 84.9%
