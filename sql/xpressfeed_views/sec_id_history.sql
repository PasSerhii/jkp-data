-- Point-in-time security identifiers (CUSIP / ISIN / SEDOL / TIC).
--
-- The XpressFeed feed has no historical identifier table: `public.sec_idcurrent`
-- holds only the value in force today, and `public.sec_history` carries date
-- intervals for EXCHG/EPF/PRIHIST* but no identifier items. Joining the current
-- header to a historical observation therefore stamps every past row with
-- today's identifier — a reverse split or SPAC completion silently rewrites the
-- identifier on months that closed long before it happened.
--
-- This table closes that gap with two sources:
--   * `wrds` rows, seeded once from the `comp.sec_idhist` snapshot copied from
--     WRDS. This is the historical backfill and is never modified afterwards.
--   * `feed` rows, appended by capture_sec_ids.py on each monthly run by
--     diffing `public.sec_idcurrent` against the open interval. From the first
--     capture onward the history maintains itself with no WRDS dependency.
--
-- `effthru = 2900-01-01` marks an open interval, matching the sentinel used by
-- sec_idhist and sec_history.

CREATE SCHEMA IF NOT EXISTS comp;

CREATE TABLE IF NOT EXISTS comp.sec_id_history (
  gvkey     varchar(7)  NOT NULL,
  iid       varchar(4)  NOT NULL,
  item      varchar(8)  NOT NULL,
  itemvalue varchar(21) NOT NULL,
  efffrom   date        NOT NULL,
  effthru   date        NOT NULL DEFAULT DATE '2900-01-01',
  source    varchar(4)  NOT NULL,
  CONSTRAINT sec_id_history_source_ck CHECK (source IN ('wrds', 'feed')),
  CONSTRAINT sec_id_history_item_ck CHECK (item IN ('CUSIP', 'ISIN', 'SEDOL', 'TIC')),
  CONSTRAINT sec_id_history_range_ck CHECK (effthru >= efffrom)
);

-- Resolution is always "the interval covering this datadate for this issue",
-- so lead with the issue key and keep efffrom ordered for the range scan.
CREATE INDEX IF NOT EXISTS sec_id_history_lookup_idx
  ON comp.sec_id_history (gvkey, iid, item, efffrom, effthru);

-- At most one open interval per issue-item; the capture script relies on this
-- to find what to close.
CREATE UNIQUE INDEX IF NOT EXISTS sec_id_history_open_idx
  ON comp.sec_id_history (gvkey, iid, item)
  WHERE effthru = DATE '2900-01-01';
