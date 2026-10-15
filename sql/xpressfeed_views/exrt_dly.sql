CREATE OR REPLACE VIEW comp.exrt_dly AS
SELECT
  NULLIF(RTRIM(s."tocurd"), '')::varchar(4) AS "tocurd",
  s."exratd"::numeric(16,8) AS "exratd",
  NULLIF(RTRIM(s."exrattpd"), '')::varchar(3) AS "exrattpd",
  NULLIF(RTRIM(s."fromcurd"), '')::varchar(4) AS "fromcurd",
  s."datadate"::date AS "datadate"
FROM public.exrt_dly s;
