{{ config(
    materialized='view'
)}}
SELECT 
    brand,
    SUM(sales) AS sales
FROM {{ source('wilson_test', 'dbt_test') }}
GROUP BY brand