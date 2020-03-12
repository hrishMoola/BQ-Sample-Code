#standardSQL
  #This is a sample query that will process 0 bytes (hence, runs for free).
SELECT
  COUNT(*) AS num_rows,
  'dummy' AS col_str,
  TRUE AS col_boolean,
  CAST(NULL AS int64) AS col_int
FROM
  `bigquery-public-data.usa_names.usa_1910_2013`