CREATE OR REPLACE VIEW comp._security AS
SELECT
  s.gvkey, s.iid, s.dldtei, s.dlrsni, s.dsci, s.epf, s.exchg, s.excntry, s.ibtic, s.secstat, s.tpci,
  COALESCE(s.cusip, i.cusip) AS cusip,
  COALESCE(s.isin,  i.isin)  AS isin,
  COALESCE(s.sedol, i.sedol) AS sedol,
  COALESCE(s.tic,   i.tic)   AS tic
FROM public.security s
LEFT JOIN (
  SELECT gvkey, iid,
    MAX(itemvalue) FILTER (WHERE item='CUSIP') AS cusip,
    MAX(itemvalue) FILTER (WHERE item='ISIN')  AS isin,
    MAX(itemvalue) FILTER (WHERE item='SEDOL') AS sedol,
    MAX(itemvalue) FILTER (WHERE item='TIC')   AS tic
  FROM public.sec_idcurrent
  GROUP BY gvkey, iid
) i ON i.gvkey=s.gvkey AND i.iid=s.iid;
