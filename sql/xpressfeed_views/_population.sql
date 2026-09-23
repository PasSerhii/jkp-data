CREATE OR REPLACE VIEW comp._gl_gvkeys AS
SELECT co.gvkey::varchar AS gvkey
FROM public.company co
WHERE co.fic IS DISTINCT FROM 'USA'
  AND co.fic IS DISTINCT FROM 'CAN'
  AND EXISTS (
    SELECT 1
    FROM public.security s
    WHERE s.gvkey = co.gvkey AND s.iid LIKE '%W'
  );

CREATE OR REPLACE VIEW comp._na_gvkeys AS
SELECT co.gvkey::varchar AS gvkey
FROM public.company co
WHERE EXISTS (
  SELECT 1
  FROM public.security s
  WHERE s.gvkey = co.gvkey AND s.iid NOT LIKE '%W'
);
