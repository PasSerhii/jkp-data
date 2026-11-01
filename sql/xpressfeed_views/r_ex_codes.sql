CREATE OR REPLACE VIEW comp.r_ex_codes AS
SELECT
  s."exchgcd"::int4 AS "exchgcd",
  NULLIF(RTRIM(s."exchgdesc"), '')::varchar(101) AS "exchgdesc"
FROM public.r_ex_codes s;
