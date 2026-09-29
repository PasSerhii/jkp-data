CREATE OR REPLACE VIEW comp.g_security AS
SELECT
  NULLIF(RTRIM(s."tic"), '')::varchar(1) AS "tic",
  NULLIF(RTRIM(s."gvkey"), '')::varchar(7) AS "gvkey",
  NULLIF(RTRIM(s."iid"), '')::varchar(4) AS "iid",
  NULLIF(RTRIM(s."cusip"), '')::varchar(21) AS "cusip",
  NULLIF(RTRIM(s."dlrsni"), '')::varchar(9) AS "dlrsni",
  NULLIF(RTRIM(s."dsci"), '')::varchar(29) AS "dsci",
  NULLIF(RTRIM(s."epf"), '')::varchar(2) AS "epf",
  s."exchg"::int4 AS "exchg",
  NULLIF(RTRIM(s."excntry"), '')::varchar(4) AS "excntry",
  NULLIF(RTRIM(s."ibtic"), '')::varchar(7) AS "ibtic",
  NULLIF(RTRIM(s."isin"), '')::varchar(21) AS "isin",
  NULLIF(RTRIM(s."secstat"), '')::varchar(2) AS "secstat",
  NULLIF(RTRIM(s."sedol"), '')::varchar(21) AS "sedol",
  NULLIF(RTRIM(s."tpci"), '')::varchar(9) AS "tpci",
  s."dldtei"::date AS "dldtei"
FROM comp._security s WHERE s.iid LIKE '%W' AND s.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys);
