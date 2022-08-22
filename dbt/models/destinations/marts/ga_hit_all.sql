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
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_SHU') }}
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_GAB') }}
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_BIO') }}
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_HR') }}
UNION ALL
SELECT 
    *
FROM {{ ref('ga_hit_YSL') }}