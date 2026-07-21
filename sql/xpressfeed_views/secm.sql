CREATE OR REPLACE VIEW comp.secm AS
SELECT
  NULLIF(RTRIM(b."gvkey"), '')::varchar(7) AS "gvkey",
  NULLIF(RTRIM(b."iid"), '')::varchar(4) AS "iid",
  b."datadate"::date AS "datadate",
  NULLIF(RTRIM(hdr."tic"), '')::varchar(8) AS "tic",
  NULLIF(RTRIM(hdr."cusip"), '')::varchar(21) AS "cusip",
  NULLIF(RTRIM(co."conm"), '')::varchar(70) AS "conm",
  b."ajexm"::numeric(18,8) AS "ajexm",
  b."ajpm"::numeric(18,8) AS "ajpm",
  NULLIF(RTRIM(b."isalrt"), '')::varchar(9) AS "isalrt",
  NULLIF(RTRIM(b."primiss"), '')::varchar(2) AS "primiss",
  b."cheqvm"::numeric(19,4) AS "cheqvm",
  NULLIF(RTRIM(b."curcddvm"), '')::varchar(4) AS "curcddvm",
  b."dvpspm"::numeric(19,4) AS "dvpspm",
  b."dvpsxm"::numeric(19,4) AS "dvpsxm",
  b."dvrate"::numeric(19,4) AS "dvrate",
  b."csfsm"::numeric(19,4) AS "csfsm",
  b."cshtrm"::numeric(19,4) AS "cshtrm",
  NULLIF(RTRIM(b."curcdm"), '')::varchar(4) AS "curcdm",
  b."navm"::numeric(19,4) AS "navm",
  b."prccm"::numeric(19,4) AS "prccm",
  b."prchm"::numeric(19,4) AS "prchm",
  b."prclm"::numeric(19,4) AS "prclm",
  b."trfm"::numeric(23,4) AS "trfm",
  b."trt1m"::numeric(23,4) AS "trt1m",
  b."rawpm"::numeric(18,8) AS "rawpm",
  b."rawxm"::numeric(18,8) AS "rawxm",
  NULLIF(RTRIM(spind."sph100"), '')::varchar(9) AS "sph100",
  NULLIF(RTRIM(spind."sphcusip"), '')::varchar(10) AS "sphcusip",
  spind."sphiid"::int4 AS "sphiid",
  spind."sphmid"::int4 AS "sphmid",
  NULLIF(RTRIM(spind."sphname"), '')::varchar(32) AS "sphname",
  spind."sphsec"::int4 AS "sphsec",
  NULLIF(RTRIM(spind."sphtic"), '')::varchar(9) AS "sphtic",
  NULLIF(RTRIM(spind."sphvg"), '')::varchar(2) AS "sphvg",
  fq.cshoq::numeric(18,4) AS "cshoq",
  b."adrrm"::numeric(18,4) AS "adrrm",
  b."cmth"::int2 AS "cmth",
  b."cshom"::numeric(19,0) AS "cshom",
  b."cyear"::int4 AS "cyear",
  NULLIF(RTRIM(b."mkvalincl"), '')::varchar(2) AS "mkvalincl",
  hdr."exchg"::int4 AS "exchg",
  NULLIF(RTRIM(hdr."secstat"), '')::varchar(2) AS "secstat",
  NULLIF(RTRIM(hdr."tpci"), '')::varchar(9) AS "tpci",
  NULLIF(RTRIM(co."cik"), '')::varchar(11) AS "cik",
  NULLIF(RTRIM(co."fic"), '')::varchar(4) AS "fic"
FROM (
  SELECT gvkey, iid, datadate, ms."adrrm" AS "adrrm", m."ajexm" AS "ajexm", m."ajpm" AS "ajpm", md."cheqvm" AS "cheqvm", ms."cmth" AS "cmth", mp."csfsm" AS "csfsm", ms."cshom" AS "cshom", mp."cshtrm" AS "cshtrm", md."curcddvm" AS "curcddvm", mp."curcdm" AS "curcdm", ms."cyear" AS "cyear", md."dvpspm" AS "dvpspm", md."dvpsxm" AS "dvpsxm", md."dvrate" AS "dvrate", m."isalrt" AS "isalrt", ms."mkvalincl" AS "mkvalincl", mp."navm" AS "navm", mp."prccm" AS "prccm", mp."prchm" AS "prchm", mp."prclm" AS "prclm", m."primiss" AS "primiss", mst."rawpm" AS "rawpm", mst."rawxm" AS "rawxm", m."spgim" AS "spgim", m."spiim" AS "spiim", m."spmim" AS "spmim", mt."trfm" AS "trfm", mt."trt1m" AS "trt1m"
  FROM public.sec_mth m
  FULL JOIN public.sec_mthprc mp USING (gvkey, iid, datadate)
  FULL JOIN public.sec_mthtrt mt USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mthdiv md USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mshare ms USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mthspt mst USING (gvkey, iid, datadate)
) b
JOIN comp._security hdr ON hdr.gvkey=b.gvkey AND hdr.iid=b.iid
LEFT JOIN public.company co ON co.gvkey=b.gvkey
LEFT JOIN public.sec_spind spind
  ON spind.gvkey=b.gvkey AND spind.iid=b.iid AND spind.datadate=b.datadate
LEFT JOIN LATERAL (
  -- co_ifndq carries fyr siblings.  Aggregate the indexed, parameterized
  -- lookup so it cannot multiply monthly rows.  WRDS attaches cshoq only to
  -- the company's current NA primary issue, not every historical issue.
  SELECT MAX(q.cshoq) AS cshoq
  FROM public.co_ifndq q
  WHERE b.iid=COALESCE(co.priusa, co.prican, co.prirow)
    AND q.gvkey=b.gvkey AND q.datadate=b.datadate
    AND q.indfmt='INDL' AND q.datafmt='STD'
    AND q.popsrc='D' AND q.consol='C'
) fq ON true
WHERE b.iid NOT LIKE '%W';
