CREATE OR REPLACE VIEW comp.sec_history AS
SELECT
  NULLIF(RTRIM(s."gvkey"), '')::varchar(7) AS "gvkey",
  NULLIF(RTRIM(s."iid"), '')::varchar(4) AS "iid",
  NULLIF(RTRIM(s."item"), '')::varchar(21) AS "item",
  NULLIF(RTRIM(s."itemvalue"), '')::varchar(21) AS "itemvalue",
  s."effdate"::date AS "effdate",
  s."thrudate"::date AS "thrudate"
FROM public.sec_history s WHERE s.iid NOT LIKE '%W';
