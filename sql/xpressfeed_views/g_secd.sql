CREATE OR REPLACE VIEW comp.g_secd AS
SELECT
  x.*,
  (CASE WHEN x.datadate = MAX(x.datadate) OVER
    (PARTITION BY x.gvkey, x.iid, date_trunc('month', x.datadate::timestamp))
    THEN 1 ELSE 0 END)::float8 AS "monthend"
FROM (
SELECT
  b."gvkey"::varchar(7) AS "gvkey",
  b."iid"::varchar(4) AS "iid",
  b."datadate"::date AS "datadate",
  NULLIF(RTRIM(co."conm"), '')::varchar(87) AS "conm",
  NULLIF(RTRIM(p."curcdd"), '')::varchar(4) AS "curcdd",
  p."ajexdi"::numeric(18,8) AS "ajexdi",
  p."cshoc"::numeric(19,0) AS "cshoc",
  p."cshtrd"::numeric(28,8) AS "cshtrd",
  p."prccd"::numeric(28,8) AS "prccd",
  p."prchd"::numeric(28,8) AS "prchd",
  p."prcld"::numeric(28,8) AS "prcld",
  p."prcod"::numeric(28,8) AS "prcod",
  p."prcstd"::int4 AS "prcstd",
  p."qunit"::numeric(16,4) AS "qunit",
  NULLIF(RTRIM(dv."curcddv"), '')::varchar(4) AS "curcddv",
  dv."cheqv"::numeric(16,4) AS "cheqv",
  dv."cheqvgross"::numeric(16,4) AS "cheqvgross",
  dv."cheqvnet"::numeric(16,4) AS "cheqvnet",
  NULLIF(RTRIM(dv."cheqvtm"), '')::varchar(3) AS "cheqvtm",
  dv."div"::numeric(16,4) AS "div",
  dv."divd"::numeric(16,4) AS "divd",
  dv."divdgross"::numeric(16,4) AS "divdgross",
  dv."divdnet"::numeric(16,4) AS "divdnet",
  NULLIF(RTRIM(dv."divdtm"), '')::varchar(3) AS "divdtm",
  dv."divgross"::numeric(16,4) AS "divgross",
  dv."divnet"::numeric(16,4) AS "divnet",
  dv."divrc"::numeric(16,4) AS "divrc",
  dv."divrcgross"::numeric(16,4) AS "divrcgross",
  dv."divrcnet"::numeric(16,4) AS "divrcnet",
  dv."divsp"::numeric(16,4) AS "divsp",
  dv."divspgross"::numeric(16,4) AS "divspgross",
  dv."divspnet"::numeric(16,4) AS "divspnet",
  NULLIF(RTRIM(dv."divsptm"), '')::varchar(3) AS "divsptm",
  dv."anncdate"::date AS "anncdate",
  dv."cheqvpaydate"::date AS "cheqvpaydate",
  dv."divdpaydate"::date AS "divdpaydate",
  dv."divrcpaydate"::date AS "divrcpaydate",
  dv."divsppaydate"::date AS "divsppaydate",
  dv."paydate"::date AS "paydate",
  dv."recorddate"::date AS "recorddate",
  sp."split"::numeric(18,8) AS "split",
  NULLIF(RTRIM(sp."splitf"), '')::varchar(9) AS "splitf",
  CASE WHEN p.gvkey IS NULL AND dv.gvkey IS NULL AND te.gvkey IS NULL THEN NULL ELSE COALESCE(t.trfd, CASE WHEN p.gvkey IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.sec_dtrt x WHERE x.gvkey=b.gvkey AND x.iid=b.iid) THEN 1.0 END) END::float8 AS "trfd",
  NULLIF(RTRIM(hdr."epf"), '')::varchar(2) AS "epf",
  hdr."exchg"::int4 AS "exchg",
  NULLIF(RTRIM(hdr."isin"), '')::varchar(21) AS "isin",
  NULLIF(RTRIM(hdr."secstat"), '')::varchar(2) AS "secstat",
  NULLIF(RTRIM(hdr."sedol"), '')::varchar(21) AS "sedol",
  NULLIF(RTRIM(hdr."tpci"), '')::varchar(9) AS "tpci",
  NULLIF(RTRIM(co."fic"), '')::varchar(4) AS "fic",
  NULLIF(RTRIM(co."gind"), '')::varchar(7) AS "gind",
  NULLIF(RTRIM(co."gsubind"), '')::varchar(9) AS "gsubind",
  NULLIF(RTRIM(co."loc"), '')::varchar(4) AS "loc"
-- WRDS emits event rows even when there is no price. UNION deduplicates the
-- price/dividend/factor-start date spine; Global additionally emits split-only
-- dates, while North-American secd deliberately does not.
FROM (
  SELECT gvkey, iid, datadate FROM public.sec_dprc
  UNION
  SELECT gvkey, iid, datadate FROM public.sec_divid
  UNION
  SELECT gvkey, iid, datadate FROM public.sec_dtrt
  UNION
  SELECT gvkey, iid, datadate FROM public.sec_split
) b
LEFT JOIN public.sec_dprc p
  ON p.gvkey=b.gvkey AND p.iid=b.iid AND p.datadate=b.datadate
LEFT JOIN public.sec_divid dv
  ON dv.gvkey=b.gvkey AND dv.iid=b.iid AND dv.datadate=b.datadate
LEFT JOIN public.sec_split sp
  ON sp.gvkey=b.gvkey AND sp.iid=b.iid AND sp.datadate=b.datadate
LEFT JOIN public.sec_dtrt te
  ON te.gvkey=b.gvkey AND te.iid=b.iid AND te.datadate=b.datadate
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
WHERE hdr.iid LIKE '%W' AND b.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys)
) x;
