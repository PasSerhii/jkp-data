CREATE OR REPLACE VIEW comp.secd AS
SELECT
  b."gvkey"::varchar(7) AS "gvkey",
  b."iid"::varchar(4) AS "iid",
  b."datadate"::date AS "datadate",
  NULLIF(RTRIM(hdr."tic"), '')::varchar(8) AS "tic",
  NULLIF(RTRIM(hdr."cusip"), '')::varchar(21) AS "cusip",
  NULLIF(RTRIM(co."conm"), '')::varchar(70) AS "conm",
  NULLIF(RTRIM(dv."curcddv"), '')::varchar(4) AS "curcddv",
  dv."capgn"::numeric(16,4) AS "capgn",
  dv."cheqv"::numeric(16,4) AS "cheqv",
  dv."div"::numeric(16,4) AS "div",
  dv."divd"::numeric(16,4) AS "divd",
  NULLIF(RTRIM(dv."divdpaydateind"), '')::varchar(2) AS "divdpaydateind",
  dv."divrc"::numeric(16,4) AS "divrc",
  dv."divsp"::numeric(16,4) AS "divsp",
  dv."dvrated"::numeric(16,4) AS "dvrated",
  NULLIF(RTRIM(dv."paydateind"), '')::varchar(2) AS "paydateind",
  dv."anncdate"::date AS "anncdate",
  dv."capgnpaydate"::date AS "capgnpaydate",
  dv."cheqvpaydate"::date AS "cheqvpaydate",
  dv."divdpaydate"::date AS "divdpaydate",
  dv."divrcpaydate"::date AS "divrcpaydate",
  dv."divsppaydate"::date AS "divsppaydate",
  dv."paydate"::date AS "paydate",
  dv."recorddate"::date AS "recorddate",
  NULLIF(RTRIM(p."curcdd"), '')::varchar(4) AS "curcdd",
  p."adrrc"::numeric(18,4) AS "adrrc",
  p."ajexdi"::numeric(18,8) AS "ajexdi",
  p."cshoc"::numeric(19,0) AS "cshoc",
  p."cshtrd"::numeric(28,8) AS "cshtrd",
  p."dvi"::numeric(18,8) AS "dvi",
  p."eps"::numeric(18,6) AS "eps",
  p."epsmo"::int2 AS "epsmo",
  p."prccd"::numeric(28,8) AS "prccd",
  p."prchd"::numeric(28,8) AS "prchd",
  p."prcld"::numeric(28,8) AS "prcld",
  p."prcod"::numeric(28,8) AS "prcod",
  p."prcstd"::int4 AS "prcstd",
  t.trfd::float8 AS "trfd",
  hdr."exchg"::int4 AS "exchg",
  NULLIF(RTRIM(hdr."secstat"), '')::varchar(2) AS "secstat",
  NULLIF(RTRIM(hdr."tpci"), '')::varchar(9) AS "tpci",
  NULLIF(RTRIM(co."cik"), '')::varchar(11) AS "cik",
  NULLIF(RTRIM(co."fic"), '')::varchar(4) AS "fic"
-- WRDS emits event rows even when there is no price. UNION deduplicates the
-- price/dividend/factor-start date spine; Global additionally emits split-only
-- dates, while North-American secd deliberately does not.
FROM (
  SELECT gvkey, iid, datadate FROM public.sec_dprc
  UNION
  SELECT gvkey, iid, datadate FROM public.sec_divid
  UNION
  SELECT gvkey, iid, datadate FROM public.sec_dtrt
) b
LEFT JOIN public.sec_dprc p
  ON p.gvkey=b.gvkey AND p.iid=b.iid AND p.datadate=b.datadate
LEFT JOIN public.sec_divid dv
  ON dv.gvkey=b.gvkey AND dv.iid=b.iid AND dv.datadate=b.datadate
LEFT JOIN LATERAL (
  SELECT rt.trfd
  FROM public.sec_dtrt rt
  WHERE rt.gvkey=b.gvkey AND rt.iid=b.iid
    AND b.datadate >= rt.datadate
    AND b.datadate <= COALESCE(rt.thrudate, 'infinity'::timestamp)
  ORDER BY rt.datadate DESC
  LIMIT 1
) t ON true
JOIN comp._security hdr ON hdr.gvkey=b.gvkey AND hdr.iid=b.iid
LEFT JOIN public.company co ON co.gvkey=b.gvkey
WHERE hdr.iid NOT LIKE '%W';
