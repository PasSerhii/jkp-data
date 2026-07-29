# Root-cause addendum: is it our bug or Research's?

REPORT.md establishes *what* differs. This addendum resolves *why* for the
material items, by querying the XpressFeed RDS and WRDS directly rather than
inferring from the output files. Verdicts are evidence-backed; items I could not
verify are listed as open rather than assumed.

## Verified: our output is correct, Research is stale or wrong

### 1. Risk-free rate — 0.0031 (ours) vs 0.0029 (Research)

WRDS `ff.factors_monthly`, queried directly:

| date | rf |
|---|---|
| 2026-03-01 | 0.0029 |
| 2026-04-01 | 0.0029 |
| **2026-05-01** | **0.0031** |
| 2026-06-01 | *not published* |

May 2026 RF **is** 0.0031. Research's 0.0029 is April's value carried forward,
i.e. Research was built before Fama-French published May. June is unpublished by
FF, so both sides fall back: ours to May (0.0031, the correct latest), Research
to April (0.0029).

**Verdict: not our bug — for the monthly rate.** Ours is correct and current.
Note the *daily* rate was a separate defect of ours; see section 16. Also note
that Research is rebuilt monthly, so its 0.0029 was the best value available
when it ran rather than staleness in the pejorative sense. This input explains
the level shift in every monthly `ret_exc` cell and propagates into the beta, residual,
and volatility characteristics REPORT.md flags as broadly different.

### 2. Universe differences — both directions are point-in-time exchange resolution

The run resolves exchange from `sec_history` as of each observation date; Research
uses the *current* security header. Where a security changed exchange after the
observation month, the two disagree — in both directions.

Research-only in May 2026 (we exclude, correctly — all were OTC `exchg=19` in May):

| Security | gvkey | OTC until | Uplisted to |
|---|---|---|---|
| Eloxx Pharmaceuticals | 032858 | 2026-06-08 | NASDAQ (14) 06-09 |
| Trulieve Cannabis | 034214 | 2026-06-09 | NYSE (11) 06-10 |
| Glass House Brands | 039216 | 2026-06-29 | NYSE (11) 06-30 |
| StablecoinX | 039893 | 2026-06-25 | NASDAQ (14) 06-26 |
| Bitzero Holdings | 075154 | 2026-06-08 | NASDAQ (14) 06-09 |

All five have complete daily price data in the RDS, so this is a filter decision,
not missing source data. They were genuinely OTC during May and are correctly
excluded from May; all appear in June once uplisted.

S3-only in May 2026 (we include, correctly — all were on a main exchange in May):

| Security | gvkey | Header now | In May 2026 |
|---|---|---|---|
| GoHealth | 036635 | 19 (OTC) | **14 NASDAQ** thru 2026-06-15 |
| Functional Brands | 045712 | 19 (OTC) | **14 NASDAQ** thru 2026-06-15 |

**Verdict: not our bug.** Research applies today's exchange to historical months
in both directions — the look-ahead defect the `sec_history` fix removes.

### 3. DEU `333909001` daily prices/returns (May 1–20)

Reconstructing returns from the RDS raw closes for gvkey 339090 / iid 01W:

| date | return implied by RDS closes | ours | Research | matches |
|---|---|---|---|---|
| 2026-05-06 | -0.0129870130 | -0.0129870 | +0.0201342 | **ours** |
| 2026-05-07 | +0.0131578947 | +0.0131579 | -0.0131579 | **ours** |
| 2026-05-08 | +0.0064935065 | +0.0064935 | +0.0666667 | **ours** |

**Verdict: not our bug.** Ours reproduces the source closes exactly to 7+
decimals on every checked day; Research does not.

### 4. Extreme Research values driving the low correlations

REPORT.md notes 41 sub-0.90 columns whose P95 absolute difference is under
`1e-6`, i.e. their Pearson correlation is destroyed by a handful of extreme
Research cells. Those cells are not plausible values: Eloxx `ret_18_1` = 369,999
(a +37,000,000% 18-month return) against our -0.999994; Adomos `rvol_252d` =
825.7 against our 0.074; Zenergy `beta_dimson_21d` = 2,001.8 against our -4.43.

**Verdict: not our bug.** These are unadjusted corporate-action artefacts on the
Research side. The correlation statistic is misleading here — read the P95
difference column instead.

## Verified: a real defect on our side (now fixed)

### 5. Accounting inputs — the availability guard was too strict

Traced record by record against the RDS (`co_adesind` + `co_afnd1`). The
production SAS applies a plain four-month lag and no publication check at all
(`accounting_chars.sas:1071`, `start_date = intnx('month', datadate, 4, 'e')`).
Our guard delayed a statement to the later of that lag and
`max(pdate, fdate, rdq)`:

| Security | Statement | 4-month lag | pdate | fdate | Research | Ours (before fix) |
|---|---|---|---|---|---|---|
| Driven Brands | FY2025 (2025-12-31) | 2026-04-30 | 2026-05-19 | 2026-06-03 | FY2025 | FY2024 |
| OVS | FY2025 (2026-01-31) | 2026-05-31 | 2026-04-21 | 2026-06-05 | FY2025 | FY2024 |
| Sony | FY2025 (2026-03-31) | **2026-07-31** | 2026-05-08 | 2026-06-23 | FY2025 | FY2024 |
| Akanda | FY2025 (2025-12-31) | 2026-04-30 | **NULL** | 2026-07-01 | — | blocked |

`pdate` is the preliminary release; `fdate` is the final filing, typically weeks
later. Taking the maximum withheld Driven Brands and OVS from the May panel even
though both were public before May 31 — hence their assets, sales, book equity
and net income came from the prior fiscal year. Fixed by using the **earliest**
marker (commit `798ea1e`), which satisfies every case: Driven Brands and OVS
enter in May; Sony stays out until the four-month lag clears; Akanda stays
blocked, because with no `pdate` its `fdate` of 2026-07-01 is the only evidence
of when the data existed. Switching to `pdate`-only would have fixed the first
two and silently reintroduced the Akanda look-ahead.

**Correction — Sony is the same defect, not a Research one.** An earlier draft
of this addendum called Sony a Research error because its *annual* FY2025
statement is not eligible until 2026-07-31. That was wrong: the May panel
resolves Sony through the **quarterly** path, and `combine_ann_qtr_chars` prefers
the quarterly value when its `datadate` is fresher.

| Sony record | datadate | atq (USD m) | rdq | fdateq | eligible (4-mo lag) |
|---|---|---|---|---|---|
| Q2 FY2025 | 2025-09-30 | 244,127.028 | 2025-11-11 | 2025-12-09 | 2026-01-31 |
| **Q3 FY2025** | **2025-12-31** | **101,315.298** | **2026-02-05** | **2026-06-15** | **2026-04-30** |

Research's 101,315.298 is exactly Q3's `atq`, and Q3 is eligible from 2026-04-30,
so Research is right. Our pre-fix guard blocked Q3 because `fdateq` (2026-06-15)
fell after May, and the panel fell back to Q2 — our 244,126.9 is Q2's value. The
`min` fix keys availability off `rdq` (2026-02-05) and restores Q3.

So the availability guard affected **three** securities, not two: Driven Brands,
OVS and Sony, all repaired by the same one-line change. The 2.4x assets gap is
still the Sony Financial spinoff — Q2 is pre-spinoff, Q3 post — but the correct
value for May/June is Q3, i.e. Research's.

## Verified: our nulls/older values are point-in-time correct, Research has look-ahead

The monthly loader rewrites history — `_characteristics_production_update` selects
`eom >= max_date_in_db` and re-inserts, so the June run (executed in early July)
recomputes the May row with whatever reference and accounting data exists by
then. That is the mechanism behind the exchange look-ahead in section 2, and it
also produces accounting look-ahead. Two clean examples:

### 8. Terra Innovatum — Research uses filings that did not exist in May

| record | datadate | AT | pdate | fdate |
|---|---|---|---|---|
| Annual FY2024 | 2024-12-31 | 0.134 | none | **2026-06-26** |
| Annual FY2025 | 2025-12-31 | **106.136** | none | **2026-06-29** |
| Q4 FY2025 | 2025-12-31 | 106.136 | rdq 2026-06-16 | 2026-06-29 |

This is a recent de-SPAC: *every* filing first appeared in mid-to-late June 2026.
Research's May row carries assets 106.136, i.e. data filed **2026-06-29** used in
a May 31 observation. Our 55 nulls are correct — at May 31 the company had no
published financials at all. The plain four-month lag cannot catch this, because
the lag is computed from `datadate` and knows nothing about when the filing
appeared.

### 9. Norma Group — the guard blocks a genuinely unpublished quarter

| record | datadate | ATQ (EUR) | pdateq | four-month lag | our start |
|---|---|---|---|---|---|
| Q1 FY2025 | 2025-03-31 | 1,415.962 | 2025-05-10 | 2025-07-31 | 2025-07-31 |
| Q2 FY2025 | 2025-06-30 | 1,353.581 | 2025-08-15 | 2025-10-31 | 2025-10-31 |
| Q3 FY2025 | 2025-09-30 | 1,299.136 | **2026-06-05** | 2026-01-31 | **2026-06-30** |
| Q4 FY2025 | 2025-12-31 | 1,250.738 | **2026-06-05** | 2026-04-30 | **2026-06-30** |

Research's assets (1,468.52 USD) is Q4 converted at ~1.174; ours is an earlier
quarter. Norma did not publish Q3/Q4 until 2026-06-05, so at May 31 neither was
public and the guard correctly withholds them; both become eligible in June.
Here `pdateq` equals `fdateq`, so the `min` fix changes nothing — the block is
genuine, not the over-strictness fixed in section 5.

**These differences are expected and should persist after the fix.** They are the
guard doing its job. Do not treat them as regressions when re-running the
comparison.

## Verified: a second defect on our side — identifiers are current-stamped

`canonical_identifier_and_classification_mismatch_records.csv` holds 646 rows.
They split into three unrelated causes.

### 10. CUSIP / ISIN / SEDOL / conm (230 + 96 rows) — our bug

The compatibility view `comp._security` builds `cusip`/`isin`/`sedol`/`tic` from
`public.sec_idcurrent` (`_security.sql:4,11,15`), which carries no date
intervals, and `secd.sql:69` joins it as `JOIN comp._security hdr ON
hdr.gvkey=b.gvkey AND hdr.iid=b.iid` — **with no date condition**. Every
historical row therefore carries the identifier as of the run date. `conm` has
the same problem via `public.company`.

The differences are not random. Ours is always the newer issue, incremented by
the CUSIP reverse-split convention:

| Security | ours | Research |
|---|---|---|
| T3 Defense | 67054R**30**2 | 67054R**20**3 |
| SRX Global | 08771Y**50**1 | 08771Y**40**2 |
| Vivakor | 92852R**60**1 | 92852R**50**2 |
| Rhinebeck Bancorp | 762093**20**1 | 762093**10**2 |

Confirmed against the source for T3 Defense (gvkey 022320, iid 01):

| table | CUSIP | effective |
|---|---|---|
| `comp.sec_idhist` | **67054R203** | 2024-10-24 -> open |
| `public.sec_idcurrent` | **67054R302** | current |

Research's value is what was effective in May; ours is what is effective today.
The `conm` cases are the same defect — we show post-merger SPAC names
(Securitize Corp, General Fusion) where Research correctly shows the pre-merger
shells (Cantor Equity Partners II, Spring Valley III).

**Fix:** resolve identifiers from `comp.sec_idhist` as of `datadate` — the table
already exists on the RDS with 860,428 rows and `efffrom`/`effthru` columns — and
fall back to `sec_idcurrent` when no interval covers the date. This is the same
ASOF pattern already used for `EXCHG`. Note `sec_idhist` is a static snapshot
copied from WRDS (2026-07-09) and needs periodic refresh, so it will lag for very
recent changes.

**There is no native alternative.** Searching every column in the database for
`%cusip%`/`%isin%`/`%sedol%` returns only current-header tables:
`public.security` (which all `comp.*` views derive from) and
`public.sec_idcurrent` (`gvkey, iid, item, itemvalue, pacvertofeedpop` — no
dates). Native `public.sec_history` carries date intervals but only for EXCHG,
EXCHGTIER, EPF, PRIHIST*, MKVALINCL and MKTLIST — no identifier items. Capital IQ
offers only `ciqtradingitem.tickersymbol`, and `ciqcrossrefidentifiertype` is a
173-row type lookup whose value table is not loaded. The WRDS-copied
`comp.sec_idhist` is the only historical identifier source that exists here.

Three ways forward:

1. **ASOF-join `comp.sec_idhist`** — correct immediately and covers full history,
   but keeps a WRDS dependency for refreshes, which cuts against the point of the
   XpressFeed migration. The snapshot is already ~2.5 weeks stale: it still shows
   T3 Defense at 67054R203 open-ended, i.e. it predates the reverse split.
2. **Accept current-stamped identifiers** and document that `cusip`/`isin`/
   `sedol`/`conm` are "as of run date", not point-in-time. Defensible because no
   characteristic uses them and `id`/`gvkey`/`iid` are stable, but any downstream
   ISIN-keyed join on a historical row is silently wrong.
3. **Accumulate history from the native feed.** Snapshot `sec_idcurrent` on each
   monthly run and append changed values with an `efffrom`, building genuine
   intervals from today forward. Self-maintaining and WRDS-free, with
   `sec_idhist` used once as the historical backfill. This is the only option
   that ends the WRDS dependency.

**Impact:** identifiers are metadata; no characteristic is computed from them,
and `id`/`gvkey`/`iid` match exactly on every common key, so internal joins are
unaffected. It matters only where a downstream consumer maps a historical row to
another system by CUSIP/ISIN — there it silently returns a future identifier.

### 10b. Validation of the fix against all 230 identifier cells

With `comp.sec_id_history` in place, every mismatching cell can be checked
against what was actually in force on its observation date:

| what the history says | cells | meaning |
|---|---:|---|
| equals Research | 182 | our current-stamping bug; the fix corrects it |
| equals neither side | 48 | all `isin`, all reconciled below |
| equals ours | 0 | — |

All 48 "neither" cells are `isin`, and **all 48 reconcile exactly** as
`excntry[:2] + historical CUSIP + check digit`, i.e. Research's value is the
synthetic ISIN built from the CUSIP that was in force. Since the fix corrects the
CUSIP, the synthetic derived from it is corrected too. **All 230 cells share one
root cause and are all repaired by the fix.**

Of the 52 `conm` IDs, 41 have no identifier mismatch at all — they are pure
renames (Expro Group Holdings NV -> Expro Ltd, Credo Tech Group Holding Ltd ->
Credo Technology Group). These will persist: the feed has no name history.

### 10c. New finding — the published `isin` is often not a real ISIN

Reconciling those 48 cells exposed a separate issue. When `isin_orig` is missing,
`_add_isin` synthesises `isin = excntry[:2] + cusip + check digit`, and `excntry`
is the *listing* country, not the issuer's domicile. For a foreign issuer listed
in the US that fabricates a `US...` ISIN which does not exist:

| security | real ISIN (history) | we publish |
|---|---|---|
| Expro | GB0003119392 | US G3314M1092 |
| Osisko Gold | CA68828E8099 | US68827X1054 |
| Hitek Global | KYG451391216 | USG451391399 |
| Niki Biosolutions | KYG6096M1226 | US6539421023 |
| Securitize Corp | KYG1827P1063 | US81517B1017 |

This is faithful to the production SAS (`cats(substr(a.excntry,1,2), b.cusip)`),
so it is not a regression — but the value will not resolve in any external
system. Scope: **9,592 of 51,232 US-listed issues (18.7%) have a real ISIN with a
non-US prefix** — CA 5,165, KY 2,323, VG 328, BM 284, IL 234, GB 156, MH 142,
NL 115. Only those whose `isin_orig` is empty publish the synthetic value, so
9,592 is the population at risk rather than the exact count.

`comp.sec_id_history` now carries the real ISINs, so `isin_orig` could be filled
from it whenever `g_secd` does not supply one, leaving the synthetic as a last
resort. That is an improvement over production and therefore a deliberate
deviation — worth deciding, especially for the Israeli issues (IL 234) given the
ILS reporting in this output.

### 11. sic / naics / ff49 (271 rows) — Research defect

All 271 are May-only, and in every one we have a value while Research is null.
The affected names include Nike, Paychex, Conagra, AAR Corp, MillerKnoll and
AngioDynamics — large, long-established US issuers that cannot legitimately lack
an industry code. Our values are correct (Nike 3021 rubber footwear, Paychex 8721
accounting services, Conagra 2000 food). `ff49` (89 rows) simply propagates
`sic`. This is a failed industry join in Research's May load, not a difference in
methodology.

### 12. sedol / isin_orig (12 cells) — same cause, same fix

The three non-US issues follow the identical point-in-time pattern, confirmed
against the history at 2026-05-31:

| security | item | ours | Research | history |
|---|---|---|---|---|
| DISO Verwaltungs (DEU) | SEDOL | BXFHV70 | B1427Y1 | **B1427Y1** |
| Trawell Co (ITA) | SEDOL | BVPF6Y2 | BK5RYR0 | **BK5RYR0** |
| Giftee Group (JPN) | ISIN | JP3264880000 | JP3264870001 | **JP3264870001** |

`_ID_HISTORY_ITEMS` covers SEDOL and ISIN, so the fix corrects these too, taking
the identifier total to **242 of 242 cells repaired**.

### 13. gics (13 cells) — interval boundaries, ours is stricter

GICS comes from `public.co_hgic`, which carries `indfrom`/`indthru` intervals.
Both directions are boundary effects:

| security | interval | effect |
|---|---|---|
| KalVista | 2016-12-05 .. **2026-06-12** | closes mid-June with no successor, so a June 30 observation has no covering interval: ours NULL, Research carries the value forward |
| Honeywell Aerospace | **2026-06-30** .. open | opens exactly on the observation date: ours populated, Research's snapshot has no interval yet |

Ours respects the intervals strictly, which is point-in-time correct. Carrying a
value past `indthru` is also defensible when the gap is a feed lag rather than a
genuine de-classification, but nothing computed depends on `gics`, so this is
presentation only.

### 14. date (1 cell) — Research is missing a quote

BNP Paribas Funds - Global (gvkey 369699): we report the month-end observation
date as 2026-06-30, Research as 2026-06-29. The feed has a quote on 2026-06-30
(prccd 114.609, carried from 06-29), so June 30 is a valid last-trading date and
our value is supported. Research's snapshot lacks that row.

### 15. Daily identifiers (3,479 cells) — same defect, and proof Research is current-stamped too

`daily_identifier_mismatch_records.csv` is the daily analogue: 3,479 cells over
127 issues (cusip 3,143 across 117 US issues; isin and sedol 168 each across the
same 10 non-US issues). Unlike the monthly file the daily `isin` is `isin_orig`
taken straight from `g_secd`, with no synthetic construction, so these are real
identifier comparisons.

| what the history says | cells | ids |
|---|---:|---:|
| equals Research | 3,460 | 127 |
| equals neither | 19 | 1 |
| equals ours | 0 | 0 |

The 19 exceptions are all Hitek Global (gvkey 034482), which changed CUSIP three
times in four months:

| CUSIP | effective |
|---|---|
| G45139105 | 2018-12-24 .. 2026-04-05 |
| **G45139113** | **2026-04-06 .. 2026-05-28** |
| G45139121 | 2026-05-29 .. 2026-07-05 |
| G45139139 | 2026-07-06 .. open |

The 19 cells are exactly the trading days 2026-05-01 to 2026-05-28, where the
correct value is G45139113. Research reports G45139121 — the value current when
its June run executed in early July. We report G45139139 — the value current at
our 2026-07-26 run.

**Both sides are current-stamped; they are simply frozen at different run dates.**
That is independent confirmation of the look-ahead thesis: Research is not
point-in-time either, and only looks correct on the other 3,460 cells because no
further change happened between the observation and its run.

Consequence for the next comparison: after the fix we will report G45139113 for
those 19 cells and **still differ from Research** — because we will be right and
Research will not. Do not read that as a regression.

### Complete accounting of the 646 rows

| cause | cells | verdict |
|---|---:|---|
| cusip / isin current-stamped | 230 | our bug — **fixed** (`e988a0e`) |
| sedol / isin_orig current-stamped | 12 | our bug — **fixed** |
| `conm` current-stamped | 96 | our bug — **not fixable**, no name history in the feed |
| sic / naics / ff49 null in Research | 271 | Research's failed May industry join |
| size_grp | 23 | expected propagation of the exchange fix |
| gics | 13 | interval boundaries; ours stricter |
| date | 1 | Research missing a 2026-06-30 quote |

Nothing in this file affects a computed characteristic: every column in it is an
identifier or a classification label.

### 16. Daily risk-free rate — a precision bug of ours (now fixed)

`daily_material_numeric_discrepancy_records.csv` holds 439,374 cells, of which
439,180 are `ret_exc_dollar`. That column's difference is a pure constant:
median -1.19047646873e-05, with p0.1 to p99.9 spanning only ±5e-10, and just 59
cells deviating (the same securities as the `ret` differences). So 439,106 cells
are nothing but the risk-free rate.

The implied rates are exact and uniform across all five countries and both
months: **ours 0.00015/day, Research 0.000138095/day**. Research's is
0.0029/21. Ours implies a monthly 0.00315 — but the RDS, WRDS and the run's own
`source_snapshot_manifest.json` all say May 2026 rf is **0.00310**, and
0.00310/21 is 0.00014761904, not 0.00015.

The giveaway is that our own output was internally inconsistent: the monthly file
implied 0.0031 and the daily file 0.00315. One input cannot yield two rates.

**Cause.** `ff.factors_monthly.rf` is `numeric(7,5)`, which Polars reads as
`Decimal(7, 5)`. Decimal division preserves the operand's scale, so `rf / 21`
rounds 0.00014761904 to **0.00015**. Reproduced deterministically:

    Decimal(7,5):  0.00310 / 21 = 0.00015              (x21 -> 0.00315)
    Float64:       0.00310 / 21 = 0.0001476190476...    (x21 -> 0.00310)

The monthly path divides by 1, so no rounding occurs and it stayed correct.

**Impact.** Every daily excess return in the full history is overstated in
subtraction by 2.38e-06/day — the daily risk-free rate is 1.6% too high. Because
the error is a constant shift it largely cancels in covariance-based
characteristics (betas, correlations, volatilities), but it biases anything built
on the level of daily excess returns. Monthly output is unaffected.

**Fix** (`rf` and `t30ret` cast to Float64 at load): the earlier report described
this as "close to the run-manifest 0.0031 after source precision/rounding" and
moved on. It was not rounding noise in the comparison — it was a real defect in
the pipeline, and the manifest recording 0.0031 while the output used 0.00315 is
itself a warning that a recorded input does not prove what the code did with it.

## Corrected and fixed: choosing between the Global and NA packages

### 6. The Global row won unconditionally, even when it was the sparse one

**This section previously concluded the opposite. It was wrong and is corrected
here.** The earlier reading — that preferring Global was "a faithful port of a
production weakness", so changing it would be a deliberate deviation — does not
survive reading the SAS to the end.

The SAS defines no rule. `set __gfunda __funda` concatenates both rows
(`accounting_chars.sas:413-423`) and the survivor is decided by a
`proc sort nodupkey` inside `%add_helper_vars` with no `ORDER BY` and no
stable-sort guarantee. Production behaviour here is incidental, not intended, so
there was nothing to be faithful to. (The *within*-Global INDL/FS rule at
`:357` is explicit and we already implement it via `apply_indfmt_filter`.)

Preferring Global was deterministic but reliably picked the weaker row, in two
shapes:

| shape | Global | NA |
|---|---|---|
| banks/insurers | `FS` — no capex, no working capital | `INDL` — populated |
| dual filers not yet caught up | stub (Sunbelt FY2025: 3/14 fields) | complete (14/14) |

UBS is the clearest case: Global `FS` has `capx` NULL, NA `INDL` has
`capx = 2008`. Measured on the feed since 2024: **478 companies file in both
packages, 101 have a Global row that is NULL where NA has data, and 66 of those
are the FS/INDL bank pattern.** It nulled `fcf_me`, `capex_abn`, `noa_at`,
`noa_gr1a` and their derivatives for every dual-listed financial — Tokio Marine,
AXA, UBS, Deutsche Bank, Janus Henderson, CRCAM, Bladex, SocGen.

Fixed in `88ab0e4`: rank candidates by how many reported values they carry, keep
the fullest, break ties toward Global so the outcome is unchanged wherever the
choice costs nothing. Whole rows are compared rather than coalescing field by
field, so a record always reflects one filing. This also pre-empts the Sunbelt
regression that would have appeared in the August run, when its FY2025 becomes
eligible and would have resolved to the 3/14 stub.

Because the SAS is non-deterministic here, a few securities where its sort
happened to keep Global will now differ from Research. That is us being
deterministic and better-populated, not a regression.

### 7. Société Générale and the other financials — same cause, now fixed

**Also corrected.** This section previously said our nulls were structural to
`FS` filings and that Research holding values was "unexplained". The explanation
is section 6: SocGen files `FS` in the Global package but `INDL` under NA, and
Research ends up with the NA row. `FS` genuinely lacks the current/non-current
split, but the NA `INDL` filing does not — so the values were always available
and we were discarding them. The `88ab0e4` fix resolves this class.

## UNRESOLVED — the largest single finding

### 17. ~197 securities have no accounting at all, cause not found

`numeric_top_differences.csv` contains 538 securities. **216 have every
accounting value null on our side** while Research has them — the biggest block
in the monthly comparison, and it is *not* explained by any fix landed today.

Six hypotheses were tested against all 213 gvkeys with a control group, and all
six were refuted:

| hypothesis | test | result |
|---|---|---|
| availability-guard coverage gap | replay eligibility under both rules | explains **4** of 213 |
| FX join nulls the row (`var * null`) | currency coverage in `exrt_dly` | refuted — USD (151) and EUR (68) fully covered |
| 52/53-week fiscal ends miss the month-end grid | month-end check | refuted — **0%**, affected and control alike |
| `start > end` yields an empty date range | replay window arithmetic | refuted — **0** securities |
| ME join drops rows | it is `how="left"`; removing the security from `world_msf` entirely | refuted — `assets` still produced |
| `combine_ann_qtr_chars` prefers a null quarterly | logic has a `qtr.notnull()` guard; replicated in Polars | refuted — combine *preserves* values |

A subset run over 5 affected gvkeys plus 2 controls, with full 2000+ history,
**reproduces Research exactly**: I Grandi Viaggi yields `assets = 117.97`
against Research's `117.971281`. The pipeline is correct in isolation.

What the published row shows: 126 columns populated, 329 empty. Every populated
column is `id`-keyed (returns, betas, rolling stats, seasonality) or a base
security field; every empty one needs the accounting join on `gvkey`. So in the
production run `acc_chars_world` had **no row** for that gvkey/month, though the
join key is present and correct in the output.

**Working lead, not a conclusion:** the trigger is something a 7-gvkey frame
cannot recreate. `create_acc_chars` contains many `.shift(N)` calls that rely on
sort order plus a `count > 12` guard rather than an explicit `.over("gvkey")`
partition; at ~50,000 gvkeys with different sort/streaming behaviour that class
of construct can misbehave in ways a small frame never will. This is untested.

**To settle it:** preserve `interim/acc_chars_world.parquet`,
`achars_world.parquet` and `acc_std_ann.parquet` from the next run. One lookup
then separates "join failed" from "row never produced", and if it is the latter
the same lookup across the three files pinpoints the step.

### 18. `combine_ann_qtr_chars` hard-crashes on small inputs

Incidental to the above: the step exits 127 with no Python traceback — a
segfault-class failure inside the ibis/DuckDB call — when run over a 7-gvkey
frame. It did not affect the analysis (the logic was replicated in Polars) but a
production step that can abort the process without an exception deserves its own
investigation.

## Resolved-but-minor, and genuinely open items

- **`size_grp` (23 of 21,774 rows, 0.1%)** — RESOLVED as expected propagation.
  All 23 are single-bucket flips (large/small 9, small/micro 9, micro/nano 3,
  plus one each at the mega boundary) with **zero non-adjacent** moves, and 22 of
  23 shift the same direction: we classify one bucket larger, i.e. our NYSE
  breakpoints sit slightly lower. A percentile-definition artefact (DuckDB
  `QUANTILE_DISC` vs SAS `QNTLDEF=5`) would be directionally random, so that is
  not the cause. A systematic shift means the NYSE breakpoint *sample* differs,
  which follows directly from point-in-time exchange resolution changing which
  securities are `exchg=11` in May. Same root cause as section 2, and ours is the
  more correct sample.

- **Identifier drift (CUSIP/ISIN/`conm`)** — this is a semantics decision, not a
  defect: whether historical rows should carry point-in-time or current
  identifiers. Internal `id`/`gvkey`/`iid` match exactly on every common key, so
  joins are unaffected either way. Decide and document the intended convention.
- **Farmer Bros (gvkey 004579)** — S3-only, but its `sec_history` EXCHG interval
  ends 2026-05-06 with no successor interval, so the ASOF join falls back to the
  current header (14, NASDAQ). The outcome is right, but this is the
  non-contiguous-interval fallback flagged when the fix was written; worth a
  sweep for securities where an expired interval and a changed header disagree.

## Recommended next steps

1. **Preserve `interim/` accounting artefacts on the next run.** This is the
   only outstanding blocker on section 17, the largest unexplained block in the
   comparison. Everything else below is secondary to it.
2. **Re-run the comparison and read it against expectations, not against zero.**
   Expect these to *persist* and be correct: universe deltas from point-in-time
   exchange (section 2), withheld unpublished accounting (sections 8-9), the
   identifier cells where Hitek-style double changes make Research wrong too
   (section 15), and a few dual-package securities where the SAS sort happened to
   pick differently (section 6).
3. **Refresh Research's FF snapshot**, or accept a constant offset on every
   monthly `ret_exc`-derived column. Ours is right.
4. **Rank columns by P95/median absolute difference, not Pearson.** Correlation
   is dominated by a handful of extreme Research cells and hides that 326 of 437
   columns agree to 0.99+.
5. **Add an output-side RF assertion.** The manifest recorded 0.0031 while the
   pipeline used 0.00315 (section 16); a recorded input does not prove what the
   code did with it. Assert implied daily RF x 21 equals the recorded monthly RF.
6. **Decide the two open policy questions:** whether `isin_orig` should be filled
   from `comp.sec_id_history` (section 10c — 1,250 securities per month publish a
   fabricated US-prefixed ISIN), and whether each month should stay frozen at its
   run-time vintage or be restated.
