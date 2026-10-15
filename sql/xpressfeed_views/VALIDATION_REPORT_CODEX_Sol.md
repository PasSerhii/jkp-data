# Current WRDS compatibility validation

Validation date: 2026-07-20 (Asia/Jerusalem)
Time-series cutoff: 2026-06-30
RDS statement timeout: 300 seconds
Validated compatibility-view SQL SHA-256: `5cb6b08e73cb7eae582515e744c58f031377b7b86eb98a65d9cb177743252e79`

## Current verdict

**PASS for the current-feed, pipeline-used-column contract. Proceed with a
staged, partitioned materialization of `secd`, `g_secd`, and `secm`.**

This is deliberately narrower than “reproduce every historical WRDS warehouse
cell.” The current S&P XpressFeed delivery is authoritative, and the pipeline's
455 consumed columns are the compatibility scope. Within that scope, the current views
has no remaining class-(a) construction bug. The remaining differences are
current-feed freshness (b), rows or constants retained only by WRDS (c), or
documented current-feed policies (d).

The result is not a claim of literal all-cell identity. The run compared
15,619,419 consumed cells on 3,283,236 matched rows and found 16,248 mismatches.
Of those, 15,880 are `trfd` constants/fallbacks for histories with no current
raw factor data. Re-introducing frozen WRDS overrides could reproduce those old
levels, but would contradict the chosen current-feed contract and recreate the
maintenance problem removed in this cutover.

## Current construction

1. Replaced snapshot-backed package membership with live rules:
   - NA: a company has any retained `public.security` issue whose `iid` does
     not end in `W`;
   - Global: `company.fic` is neither `USA` nor `CAN` (null remains eligible)
     and the company has a retained issue whose `iid` ends in `W`.
   Memberships are independent, so a company may belong to both packages.
2. Removed the weak new-gvkey fallback that admitted 90 unwanted Global
   companies in build 10.
3. Removed every runtime/application dependency on `comp.wrds_population` and
   `comp.wrds_sp500`; deleted both tables without `CASCADE` after proving zero
   dependents. Six obsolete population/S&P provenance rows were also removed.
4. Kept `curr_sp500_flag` as `NULL::float8` in `company` and `security`. Its
   WRDS shape is preserved, but it is not one of the pipeline's consumed
   columns.
5. Kept `ff.factors_monthly`, because the pipeline reads `date` and `rf`.
   Deployment now verifies its exact schema, count, and recomputed live content
   hash under a share lock.
6. Refreshed the live WRDS schema contract. During this check WRDS had changed
   `r_ex_codes.exchgdesc` from `varchar(100)` to `varchar(101)`; the current view
   applied the corrected typmod through the guarded no-`CASCADE` migration.
7. Preserved the internal helper's existing unbounded `varchar` interface so
   live membership can be deployed with `CREATE OR REPLACE VIEW`.

## Independent methodology and guardrails

The post-cutover evidence is
`VALIDATION_EVIDENCE_POST_CUTOVER_BUILD13.json`, SHA-256
`5b10b8b1edb8fa16191d66ca51333e7afaf6178c4aa146fef0e9ce47d706d326`.
It did not overwrite or import results from the build-10 evidence. An initial
sample was discarded when I noticed it inherited the prior deterministic
seed; the final run rebuilt all sample-dependent sections with the new seed
`post-cutover-build13-20260720`.

Checks ran in the required order:

1. all information-schema metadata for all 18 objects;
2. a fresh source-code derivation of all 455 consumed columns, followed by
   existence and non-null population checks;
3. full counts for the 16 permitted objects and explicit-pair count/min/max
   checks for 110 `secd` plus 110 `g_secd` histories;
4. cell-by-cell comparison of the natural-key union of every consumed column;
5. targeted current raw-table queries for every difference class;
6. end-of-run schema/build/hash and credential-leak gates.

All RDS sessions used a 300-second statement timeout. No unfiltered count,
aggregate, or scan was run against RDS `comp.secd` or `comp.g_secd`; daily
predicates were expanded scalar `(gvkey=%s AND iid=%s)` clauses. Decimal
comparisons used relative tolerance `1e-9` and absolute floor `1e-12`.

## Schema and pipeline contract

| Check | Result |
|---|---:|
| Exact object schemas | 18 / 18 |
| Schema columns compared | 2,718 |
| Pipeline objects checked | 18 / 18 |
| Pipeline-consumed columns | 455 / 455 present |
| Consumed columns proved populated | 455 / 455 |
| Matched rows in population samples | 64,155 |
| Duplicate natural-key groups | 0 |

The live population rules matched both their RDS helper views and the complete
current WRDS company key sets, including ordered full-set fingerprints:

| Membership | RDS | WRDS | Full key set | Overlap |
|---|---:|---:|---|---:|
| North America | 58,112 | 58,112 | exact | 2,916 |
| Global | 83,344 | 83,344 | exact | 2,916 |

This is why a binary “USA/Canada else Global” geography rule is incorrect:
2,916 companies legitimately occupy both WRDS packages. The issue-package
rule reproduces that behavior and automatically classifies future companies
once their security rows arrive.

## Row counts

Delta is RDS minus WRDS. Full counts were never run on either daily view.

| Object | RDS | WRDS | Delta | Root class |
|---|---:|---:|---:|---|
| `funda` | 941,345 | 941,340 | +5 | b |
| `g_funda` | 1,090,891 | 1,088,573 | +2,318 | b/c |
| `fundq` | 2,130,143 | 2,130,133 | +10 | b |
| `g_fundq` | 4,097,992 | 4,092,887 | +5,105 | b/c |
| `secm` | 8,508,272 | 8,509,070 | -798 | c |
| `security` | 77,211 | 77,212 | -1 | c |
| `g_security` | 131,705 | 131,707 | -2 | c |
| `company` | 58,112 | 58,112 | 0 | exact |
| `g_company` | 83,344 | 83,344 | 0 | exact |
| `sec_history` | 331,156 | 331,158 | -2 | c |
| `g_sec_history` | 180,194 | 180,195 | -1 | b/c |
| `co_hgic` | 45,758 | 45,759 | -1 | b/c |
| `g_co_hgic` | 89,139 | 89,137 | +2 | b |
| `exrt_dly` | 2,221,564 | 2,221,260 | +304 | b |
| `r_ex_codes` | 245 | 245 | 0 | exact |
| `ff.factors_monthly` | 1,199 | 1,199 | 0 | exact |

The 110 explicitly filtered histories for each daily object had zero count,
minimum-date, or maximum-date differences through the cutoff.

## Pipeline-used value results

“Cells” counts pipeline-consumed cells only. Natural-key-only fields used for
the merge were also checked but are not inflated into this total.

| Object | Scope | Matched rows | Cells | Mismatches by consumed column | RDS only | WRDS only |
|---|---|---:|---:|---|---:|---:|
| `funda` | 36 gvkeys (15 fresh) | 768 | 65,280 | none | 0 | 0 |
| `g_funda` | 22 gvkeys (15 fresh) | 396 | 31,284 | none | 0 | 1 |
| `fundq` | 28 gvkeys (15 fresh) | 2,028 | 192,660 | none | 0 | 0 |
| `g_fundq` | 22 gvkeys (15 fresh) | 1,777 | 156,376 | none | 0 | 0 |
| `secd` | 10 histories (8 fresh) | 13,188 | 276,948 | `exchg` 177; `tpci` 177; `trfd` 527 | 0 | 0 |
| `g_secd` | 17 histories (8 fresh plus cutover pairs) | 40,218 | 965,232 | `trfd` 15,353 | 0 | 83 |
| `secm` | 30 histories (15 fresh) | 8,148 | 138,516 | `dvpsxm` 5; `prccm` 1; `prchm` 3; `prclm` 1 | 0 | 0 |
| `security` | full | 77,211 | 463,266 | `exchg`, `excntry`, `secstat` 1 each | 0 | 1 |
| `g_security` | full | 131,705 | 790,230 | none | 0 | 2 |
| `company` | full | 58,112 | 232,448 | none | 0 | 0 |
| `g_company` | full | 83,344 | 333,376 | none | 0 | 0 |
| `sec_history` | full | 331,156 | 1,655,780 | none | 0 | 2 |
| `g_sec_history` | full | 180,171 | 900,855 | `thrudate` 1 | 23 | 24 |
| `co_hgic` | full | 45,757 | 183,028 | none | 1 | 2 |
| `g_co_hgic` | full | 89,137 | 356,548 | none | 2 | 0 |
| `exrt_dly` | full through cutoff | 2,218,676 | 8,874,704 | none | 0 | 0 |
| `r_ex_codes` | full | 245 | 490 | none | 0 | 0 |
| `ff.factors_monthly` | full | 1,199 | 2,398 | none | 0 | 0 |
| **Total** | **18 objects** | **3,283,236** | **15,619,419** | **16,248** | **26** | **115** |

## Root causes and classifications

The evidence contains 26 explicit classification entries and zero unclassified
count, cell, or one-sided-row differences.

| Difference | Raw evidence | Class |
|---|---|---|
| Accounting count deltas | RDS view counts exactly equal its current `co_adesind`/`co_idesind` D/I descriptor counts. Global RDS descriptors are ahead; WRDS constructed Global tables also retain 95 annual and two quarterly rows beyond their current raw descriptors. Sampled common consumed cells are exact. | b/c |
| `g_funda` one-sided row | WRDS retains an `N` consolidation output for `201959`, 2024-03-31, while both current Global raw descriptors contain the `C` state. | c |
| `secm` count | `sec_mth` and `sec_mthprc` counts agree, but WRDS raw `sec_mthtrt` retains 808 more rows; the exact source union produces the 798-row view delta. | c |
| `secm` values | All ten 0.0001 differences already exist in current raw `sec_mthdiv`/`sec_mthprc`. The current half-up cast minimizes discrepancies (10 versus 21 for half-even and 4,705 for truncation). | b/d |
| `security` and `secd` headers | Current raw `050773/01` has `exchg=14`, `excntry=USA`, `secstat=A`, `tpci=0`; WRDS raw fields are null. The daily view repeats that current header over 177 dates. | b |
| NA `secd.trfd` | Both databases' current `sec_dtrt`, `sec_divid`, and `sec_split` have zero rows for `035213/02`. WRDS constructed output retains constant `2.02252245`; current-feed NA policy leaves it null. | c/d |
| Global `g_secd.trfd` | All nine mismatching pairs have zero current `sec_dtrt`/`g_sec_dtrt` and dividend history on both systems. WRDS retains security constants; the documented current-feed fallback emits 1.0. | c/d |
| Global daily one-sided rows | Two WRDS-only headers, `313270/03W` and `353869/03W`, are absent from current RDS raw security. WRDS raw retains one split-only date for the first and 82 price dates for the second, exactly explaining all 83 rows. | c |
| Security/history differences | `145162/19C` and its two history rows remain only on WRDS. Global history has 23 current-only and 24 WRDS-only re-dated rows plus one open/closed `thrudate` revision; targeted raw rows reproduce them. | b/c |
| GICS history | NA has one current-only and two WRDS-retained intervals; Global has two current-only intervals. Common cells are exact. | b/c |
| `exrt_dly` +304 | Both feeds have exactly 2,218,676 rows through the cutoff. RDS extends to 2026-07-19; WRDS stops at 2026-07-17. | b |
| `r_ex_codes` | After refreshing the schema contract, all 245 rows and both used columns are exact. | no difference |
| `ff.factors_monthly` | All 1,199 rows of `date` and `rf` are exact; the live content hash matches provenance. | no difference |

## Explicit construction-bug list

### Found and fixed in this cutover

1. The weak Global new-gvkey fallback admitted 90 companies not in the current
   WRDS Global package and propagated 114 issue rows. It was removed; live
   package key sets are now exact.
2. The checked-in `r_ex_codes.exchgdesc` typmod lagged a live WRDS change by
   one character. The contract and deployed view now use `varchar(101)`.
### Remaining

**None in the tested current-feed/pipeline-used scope.** The final evidence has
`no_class_a_construction_bug=true`, zero unclassified differences, identical
start/end schemas, and the same local/deployed SQL hash throughout.

## Runtime support and future operation

`comp.wrds_population` and `comp.wrds_sp500` no longer exist. Their deleted
contents can be recovered only from a database backup or by re-copying WRDS;
this was intentional. Runtime membership now follows the live S&P company and
security delivery without a refresh job or frozen-company exception list.

No compatibility history tables remain in Postgres. Applying the views always
refreshes `ff.factors_monthly` from current WRDS and immediately checks the RDS
row count, date range, schema, and content hash against that in-memory refresh.
No previous execution state participates in construction or validation.

The validation JSON is not production configuration: it is an external,
machine-readable checkpoint containing samples, counts, examples, root queries,
and acceptance assertions. It was captured before the two metadata-only history
tables were removed; that removal does not change any compatibility view or FF
value. Read this report for conclusions; use the JSON only when auditing a claim.

`ff.factors_monthly` is still a real future dependency because `rf` is used.
If WRDS access ends, a maintained replacement source must provide subsequent
months with the same semantics. The last verified snapshot can continue to
serve already-covered dates.

## Materialization recommendation

Proceed, but do not use an unpartitioned `REFRESH MATERIALIZED VIEW` over the
roughly 472-million-row daily source. Build date-partitioned staging tables,
enforce uniqueness and indexes on `(gvkey, iid, datadate)`, compare all used
columns against these logical views, and publish by an atomic swap. The known
`trfd` policies will be copied into the physical tables; materialization does
not create or cure those source-data differences.

## Comparison with the build-10 and prior reports

Agreements:

- the accounting/daily/monthly construction rules previously fixed remain
  sound;
- `trfd` constants absent from current raw data cannot be reconstructed
  exactly without retaining WRDS overrides;
- staged, partitioned physical tables remain the correct production design.

Superseding findings and disagreements:

- build 10's recommendation to retain/refresh `wrds_population` and
  `wrds_sp500` is rejected. The exact live package rule removes both runtime
  dependencies and the 90-company fallback error;
- `curr_sp500_flag` is now explicitly unsupported data with a schema-compatible
  typed null, because the pipeline never reads it;
- build 10's `r_ex_codes` +14 result was temporal. Current WRDS has caught up
  to 245 rows and changed the description typmod to 101; build 13 is fully
  exact;
- old absolute counts and classification overlays were superseded by this run
  and have been deleted from the working tree;
- this acceptance intentionally compares every pipeline-used column rather
  than spending resources on unused WRDS fields.

No further logical-view rewrite is required before the staged physical build.
