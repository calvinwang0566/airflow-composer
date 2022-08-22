{{ config(
    materialized='view'
)}}

WITH
google_ads_source AS (
  SELECT *
  FROM {{ source('google_Ads_Ms', 'p_CampaignBasicStats_7806077362') }}
  UNION DISTINCT
  SELECT *
  FROM {{ source('google_Ads_Coop', 'p_CampaignBasicStats_8532601374') }}
),
customer_to_brand AS (
  SELECT 
    ExternalCustomerId,
    CASE
      WHEN AccountDescriptiveName LIKE 'lpd_tw_lan%' THEN '01'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_bio%' THEN '07'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_hr%' THEN '15'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_gio%' THEN '16'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_shu%' THEN '40'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_k%' THEN '45'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_ysl%' THEN '51'
    END AS brand
  FROM {{ source('google_Ads_Ms', 'p_Customer_7806077362') }}
  UNION DISTINCT
  SELECT
    ExternalCustomerId,
    CASE
      WHEN AccountDescriptiveName LIKE 'lpd_tw_lan%' THEN '01'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_bio%' THEN '07'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_hr%' THEN '15'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_gio%' THEN '16'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_shu%' THEN '40'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_k%' THEN '45'
      WHEN AccountDescriptiveName LIKE 'lpd_tw_ysl%' THEN '51'
    END AS brand
  FROM {{ source('google_Ads_Coop', 'p_Customer_8532601374') }}
),
google_ads_final AS (
  SELECT
    brand,
    Date AS date,
    SUM(Clicks) AS clicks,
  FROM google_ads_source
  INNER JOIN customer_to_brand
  USING (ExternalCustomerId)
  GROUP BY brand, date
),
ga_final AS (
  SELECT
    brand,
    DATE(visitStartDateTime) AS date,
    COUNT(DISTINCT fullvisitorId) AS users,
    COUNT(DISTINCT CASE WHEN visitNumber = 1 THEN fullvisitorId END) AS new_users,
    COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END) AS sessions,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN totals.visits = 1 THEN totals.pageviews / totals.hits END), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 2) AS pages_per_session,
    ROUND(SAFE_DIVIDE(SUM(totals.timeOnSite / totals.hits), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 2) AS avg_session_duration,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN visitNumber = 1 AND totals.visits = 1 THEN sessionId END), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 4) AS new_sessions_ratio,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN totals.bounces = 1 AND totals.visits = 1 THEN sessionId END), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 4) AS bounce_rate,
    --   COUNT(CASE WHEN LOWER(hit.eventInfo.eventAction) LIKE '%click%' THEN 1 END) AS clicks,
    CAST(ROUND(SUM(totals.totalTransactionRevenue / 1000000 / totals.hits), 0) AS INTEGER) AS revenue,
    COUNT(DISTINCT CONCAT(sessionId, '-', hit.eventInfo.eventCategory, '-', COALESCE(hit.eventinfo.eventaction), '-', COALESCE(hit.eventinfo.eventlabel))) AS unique_events,
    ROUND(SAFE_DIVIDE(SUM(totals.transactions / totals.hits), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 4) AS ecommerce_conversion_rate
  FROM {{ ref('ga_hit_all') }}
  GROUP BY brand, date
)
SELECT
  brand,
  date,
  users,
  new_users,
  sessions,
  pages_per_session,
  avg_session_duration,
  new_sessions_ratio,
  bounce_rate,
  clicks,
  revenue,
  unique_events,
  ecommerce_conversion_rate
FROM google_ads_final
FULL OUTER JOIN ga_final
USING (brand, date)
ORDER BY brand, date