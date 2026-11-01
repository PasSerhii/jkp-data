CREATE OR REPLACE VIEW comp.g_co_hgic AS
SELECT
  NULLIF(RTRIM(s."gvkey"), '')::varchar(7) AS "gvkey",
  NULLIF(RTRIM(s."indtype"), '')::varchar(9) AS "indtype",
  NULLIF(RTRIM(s."ggroup"), '')::varchar(5) AS "ggroup",
  NULLIF(RTRIM(s."gind"), '')::varchar(7) AS "gind",
  NULLIF(RTRIM(s."gsector"), '')::varchar(3) AS "gsector",
  NULLIF(RTRIM(s."gsubind"), '')::varchar(9) AS "gsubind",
  s."indfrom"::date AS "indfrom",
  s."indthru"::date AS "indthru"
FROM public.co_hgic s WHERE s.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys);
