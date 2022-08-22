{{ config(
    materialized='view'
)}}

SELECT
  CASE
    WHEN Account LIKE 'lpd_tw_lan%' THEN '01'
    WHEN Account LIKE 'lpd_tw_bio%' THEN '07'
    WHEN Account LIKE 'lpd_tw_hr%' THEN '15'
    WHEN Account LIKE 'lpd_tw_gio%' THEN '16'
    WHEN Account LIKE 'lpd_tw_shu%' THEN '40'
    WHEN Account LIKE 'lpd_tw_k%' THEN '45'
    WHEN Account LIKE 'lpd_tw_ysl%' THEN '51'
  END AS Brand,
  * EXCEPT(Account)
FROM {{ ref('google_ads_daily_report_ongoing') }}
WHERE Date >= '2022-01-01'
UNION DISTINCT
SELECT
  CASE
    WHEN Account LIKE 'lpd_tw_lan%' THEN '01'
    WHEN Account LIKE 'lpd_tw_bio%' THEN '07'
    WHEN Account LIKE 'lpd_tw_hr%' THEN '15'
    WHEN Account LIKE 'lpd_tw_gio%' THEN '16'
    WHEN Account LIKE 'lpd_tw_shu%' THEN '40'
    WHEN Account LIKE 'lpd_tw_k%' THEN '45'
    WHEN Account LIKE 'lpd_tw_ysl%' THEN '51'
  END AS Brand,
  * EXCEPT(Account)
FROM {{ source('Ads_and_Promotion', 'google_ads_daily_report_history') }}
