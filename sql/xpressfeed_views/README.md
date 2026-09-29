# WRDS-compatible views over native XpressFeed

This directory constructs the Compustat tables downloaded by the pipeline as
SQL views over the current S&P XpressFeed delivery. The output objects are
`comp.funda`, `comp.g_funda`, `comp.fundq`, `comp.g_fundq`, `comp.secd`,
`comp.g_secd`, `comp.secm`, `comp.security`, `comp.g_security`, `comp.company`,
`comp.g_company`, `comp.sec_history`, `comp.g_sec_history`, `comp.co_hgic`,
`comp.g_co_hgic`, `comp.exrt_dly`, and `comp.r_ex_codes`.

The compatibility target is:

> Apply WRDS construction semantics to the current XpressFeed delivery for the
> columns used by this pipeline. When the current S&P feed and retained WRDS
> warehouse state disagree, the current feed is authoritative.

The full WRDS-shaped schemas are retained so the existing downloads continue
to work, but unused fields are not grounds for maintaining separate WRDS data.
In particular, `curr_sp500_flag` is an intentionally typed `NULL::float8`
placeholder in `company` and `security`; the pipeline does not read it.

With these views and `ff.factors_monthly`, the pipeline can use the XpressFeed
database without changing its Compustat SQL. The connection must point to the
XpressFeed database, and the pipeline must use `bypass_crsp` because this source
does not include CRSP.

## Current view state

Build 13 is deployed with SQL content hash
`5cb6b08e73cb7eae582515e744c58f031377b7b86eb98a65d9cb177743252e79`.
Its live package populations are:

| Membership | Companies |
|---|---:|
| North America | 58,112 |
| Global | 83,344 |
| Both | 2,916 |

The overlap is intentional. A foreign company can have a North-American issue
and a separate `W` Global issue, so NA and Global are independent memberships,
not an either/or classification.

Build 13 removes the two WRDS package/constituent snapshot dependencies. Its
independent post-cutover validation passed. It is safe to proceed with the
controlled, staged physical materialization described below; this does not
mean that the large logical views have already been materialized.

## Build 13 validation result

The fresh post-cutover suite passed the current-feed compatibility contract:

- All 18 objects matched the WRDS schema contract across all 2,718 columns.
- All 455 pipeline-consumed columns were present and proved populated; pipeline
  population checks covered 64,155 matched rows.
- Full row counts were evaluated for all 16 count-safe objects. For `secd` and
  `g_secd`, 110 security pairs each had exact per-pair row counts and date
  ranges without an unsafe full scan.
- The full/sampled value comparison covered 3,283,236 matched rows and
  15,619,419 pipeline-consumed cells. It found 16,248 differences, all
  root-caused as current-feed/WRDS timing, WRDS-retained stale state, or an
  accepted current-feed policy (classes `b`, `c`, or `d`). There were no
  class-`a` construction bugs and no duplicate natural-key groups.
- The complete live NA and Global gvkey sets matched WRDS exactly, including
  all 2,916 dual-package companies.

The machine-readable evidence is
[`VALIDATION_EVIDENCE_POST_CUTOVER_BUILD13.json`](VALIDATION_EVIDENCE_POST_CUTOVER_BUILD13.json)
(SHA-256
`5b10b8b1edb8fa16191d66ca51333e7afaf6178c4aa146fef0e9ce47d706d326`).
See the [current validation report](VALIDATION_REPORT_CODEX_Sol.md#current-wrds-compatibility-validation)
for the human-readable result and interpretation.

## Construction rules

The accounting rules were reverse-engineered against WRDS and checked against
the official WRDS Compustat build diagrams. The population rules are derived
directly from the current native feed.

- `_na_gvkeys` contains a company when any retained `public.security` row for
  that company has `iid NOT LIKE '%W'`.
- `_gl_gvkeys` contains a company when `company.fic` is distinct from both
  `USA` and `CAN` (including a null `fic`) and any retained security has
  `iid LIKE '%W'`.
- These rules use all retained security rows, not only currently active rows,
  so historical and delisted companies remain in the appropriate population.
  There is no frozen population snapshot, new-gvkey fallback, or manual
  either/or assignment. New feed companies are classified automatically from
  their company and security records.
- `funda`/`g_funda`: spine = `co_adesind`; LEFT JOIN `co_afnd1`/`co_afnd2`
  items on the six-part key, `co_aaudit` at `rank=1`, `co_industry`,
  `co_amkt` pivoted on `cfflag`, company header, and security at the primary
  issue. The fiscal market row is keyed by `datadate`; its `year` selects the
  calendar row, with XpressFeed's `year=0` first-observation sentinel mapped to
  `year(datadate)`. A calendar-only row is emitted on an equal descriptor date
  only when no fiscal row anchors that company/year/currency. A fiscal row
  whose year is null suppresses calendar output. Calendar adjustment factors
  use the selected calendar row's date.
- Annual primary iid follows the WRDS build: NA = `PRIUSA`, else `PRICAN`, else
  `PRIROW`; Global = `PRIROW`, else the single Global security when exactly one
  `W` issue exists. Historical NA `hiid` is selected from the row currency:
  `CAD` uses `PRIHISTCAN`, `USD` uses `PRIHISTUSA`, and other/null currencies
  stay null without cross-package fallback. Global uses `PRIHISTROW` only.
- `fundq`/`g_fundq`: the analogous construction uses the `co_idesind` spine,
  `co_ifndq`, `co_ifndytd`, `co_iaudit`, and `co_imkt`. Quarterly join keys
  include `fyr`, and NA historical primary selection uses `curcdq`.
- `secd`/`g_secd`: a deduplicated event-date spine comes from `sec_dprc`,
  `sec_divid`, and `sec_dtrt` starts; Global also includes verified split-only
  dates from `sec_split`. Return factors use deterministic latest-effective
  interval selection. Global `monthend` is computed after completing the event
  spine. Typed keys still allow pair filters to reach the raw indexes. NA uses
  non-`W` issues; Global uses `W` issues belonging to `_gl_gvkeys`.
- `secm`: spine = `sec_mth` full-joined to `sec_mthprc` and `sec_mthtrt`, then
  left-joined to `sec_mthdiv` and `sec_mshare`. Eight S&P-history fields come
  from `sec_spind`. The indexed/deduplicated `cshoq` lookup is attached only to
  the company's current NA primary issue.
- `_security`: native `security` enriched with current `CUSIP`, `ISIN`,
  `SEDOL`, and `TIC` pivoted from `sec_idcurrent`.
- Every output column is cast to the checked-in WRDS type, including numeric
  precision/scale and varchar length, so downloaded Parquet schemas remain
  compatible.

## Usage

```bash
# Regenerate DDL from the checked-in WRDS schema contract and live raw schema.
uv run --with "psycopg[binary]" python sql/xpressfeed_views/gen_views.py

# Optional: deliberately refresh the reviewed schema contract from live WRDS.
uv run --with "psycopg[binary]" python sql/xpressfeed_views/gen_views.py --refresh-contract

# Refresh ff.factors_monthly, then transactionally create/replace the views.
uv run --with "psycopg[binary]" python sql/xpressfeed_views/apply_views.py
```

Credentials come from `.env`: `ENV_USERNAME`/`ENV_PASSWORD` are used for WRDS
access, and `COMPUSTAT` identifies the XpressFeed database. Ordinary generation
uses the checked-in schema contract and needs only `COMPUSTAT`; refreshing the
schema contract or Fama-French data requires WRDS.

There is no `load_wrds_population.py` workflow. Build 13 neither creates nor
reads `comp.wrds_population` or `comp.wrds_sp500`, and the view-application script
does not refresh either table. `curr_sp500_flag` remains null by design because
it is unused by the pipeline.

Their removal from the current database was a one-time, dependency-checked
upgrade action. Normal view applications intentionally do not repeat destructive
table drops. An older database copied from the former design should receive the
same one-time cleanup after confirming that no external object depends on those
tables; a fresh installation never creates them.

## Required support data

`ff.factors_monthly` remains required because the pipeline consumes its `rf`
column. Every `apply_views.py` run refreshes this small table from current WRDS,
loads it atomically, and verifies the resulting RDS rows directly against the
row count, date range, and content hash held in memory from that refresh. No FF
refresh history is stored in the database.

If WRDS access will end, the last verified FF snapshot can continue to run the
pipeline temporarily, but future months require another maintained source for
the same factors. This is the only WRDS-sourced data snapshot among the current
runtime inputs.

`wrds_schema_contract.json` is different: it is a checked-in build-time schema
specification, not a runtime data snapshot. It records column names, order, and
types used by the SQL generator. It changes only after an intentional schema
review.

## No database history tables

The view builder stores no execution, build, or snapshot history in Postgres.
`comp.compatibility_snapshot_history` and
`comp.compatibility_view_build_history` were removed. Current compatibility is
established from the checked-in SQL, the current FF refresh, and the external
validation report rather than records of earlier runs.

## Validation artifacts

Only the current validation artifacts are retained:

- `VALIDATION_REPORT_CODEX_Sol.md` is the human-readable result, methodology,
  root-cause analysis, and decision.
- `VALIDATION_EVIDENCE_POST_CUTOVER_BUILD13.json` is its machine-readable
  evidence and acceptance record.

The superseded reports, classification overlays, and evidence files from the
former snapshot-backed design were deleted. Neither current artifact is read by
the views or application process.

## Known policies and materialization guidance

- The Global daily 1.0 factor fallback represents a current-feed construction
  policy for securities with price rows but no current factor history. It
  preserves within-security returns rather than obsolete absolute factor
  levels retained by an older warehouse state.
- North-American daily histories remain null when neither current raw factor
  table supplies an interval. Whether to change this policy should be decided
  from its effect on pipeline returns, not from stale WRDS-only constants.
- `comp.sec_idhist`, if retained separately in the database, is outside this
  view build and is not used by the pipeline. It is not refreshed or required
  by `apply_views.py`.
- The checked-in objects are ordinary logical views. `secd`, `g_secd`, and
  `secm` have not been turned into production physical tables by this workflow.
  Because the daily raw source is roughly 472 million rows, do not use a plain
  unpartitioned full-refresh materialized view.
- For production performance, build the three large outputs into staging tables
  partitioned by date, enforce uniqueness and indexes on
  `(gvkey, iid, datadate)`, compare the staged used columns with the logical
  views, publish atomically, and incrementally rebuild affected partitions.
