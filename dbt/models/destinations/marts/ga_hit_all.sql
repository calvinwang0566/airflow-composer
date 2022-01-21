{{ config(
    materialized='view'
)}}

SELECT 
    *
FROM {{ ref('ga_hit_LAN') }}