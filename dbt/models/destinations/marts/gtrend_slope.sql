{{ config(
    materialized='table'
)}}

WITH
base AS (
  SELECT
    DISTINCT
    keyword,
    updated_datetime
  FROM {{ source('marts', 'gtrend') }}
),
raw_3m AS (
  SELECT
    keyword,
    SUM((x - x_bar) * (y - y_bar)) / SUM((x - x_bar) * (x - x_bar)) AS slope
  FROM (
    SELECT
      keyword,
      x,
      AVG(x) OVER() AS x_bar,
      y,
      AVG(y) OVER() AS y_bar
    FROM (
      SELECT
        keyword,
        ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY date ASC) AS x,
        CAST(hits AS INTEGER) AS y
      FROM {{ source('marts', 'gtrend') }}
      WHERE date >= DATE_SUB(DATE(updated_datetime), INTERVAL 90 DAY)
    )
  )
  GROUP BY keyword
),
raw_6m AS (
  SELECT
    keyword,
    SUM((x - x_bar) * (y - y_bar)) / SUM((x - x_bar) * (x - x_bar)) AS slope
  FROM (
    SELECT
      keyword,
      x,
      AVG(x) OVER() AS x_bar,
      y,
      AVG(y) OVER() AS y_bar
    FROM (
      SELECT
        keyword,
        ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY date ASC) AS x,
        CAST(hits AS INTEGER) AS y
      FROM {{ source('marts', 'gtrend') }}
      WHERE date >= DATE_SUB(DATE(updated_datetime), INTERVAL 180 DAY)
    )
  )
  GROUP BY keyword
),
raw_1y AS (
  SELECT
    keyword,
    SUM((x - x_bar) * (y - y_bar)) / SUM((x - x_bar) * (x - x_bar)) AS slope
  FROM (
    SELECT
      keyword,
      x,
      AVG(x) OVER() AS x_bar,
      y,
      AVG(y) OVER() AS y_bar
    FROM (
      SELECT
        keyword,
        ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY date ASC) AS x,
        CAST(hits AS INTEGER) AS y
      FROM {{ source('marts', 'gtrend') }}
      WHERE date >= DATE_SUB(DATE(updated_datetime), INTERVAL 365 DAY)
    )
  )
  GROUP BY keyword
)

SELECT
  keyword,
  raw_3m.slope AS slope_3m,
  raw_6m.slope AS slope_6m,
  raw_1y.slope AS slope_1y,
  (raw_3m.slope * 2 + raw_6m.slope * 2 + raw_1y.slope * 1) / 5  AS score,
  updated_datetime
FROM base
LEFT JOIN raw_3m
USING (keyword)
LEFT JOIN raw_6m
USING (keyword)
LEFT JOIN raw_1y
USING (keyword)