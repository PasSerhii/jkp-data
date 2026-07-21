CREATE SCHEMA IF NOT EXISTS ff;

CREATE TABLE IF NOT EXISTS ff.factors_monthly (
  date date,
  mktrf numeric(8,6),
  smb numeric(8,6),
  hml numeric(8,6),
  rf numeric(7,5),
  year float8,
  month float8,
  umd numeric(8,6),
  dateff date
);

CREATE UNIQUE INDEX IF NOT EXISTS factors_monthly_date_idx
  ON ff.factors_monthly (date);
