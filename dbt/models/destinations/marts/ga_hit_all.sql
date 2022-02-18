{{ config(
    materialized='view'
)}}

SELECT 
    *
FROM {{ ref('ga_hit_LAN') }}
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_KLS') }}