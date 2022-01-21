{{ config(
    materialized='view'
)}}

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
  COUNT(CASE WHEN LOWER(hit.eventInfo.eventAction) LIKE '%click%' THEN 1 END) AS clicks,
  CAST(ROUND(SUM(totals.totalTransactionRevenue / 1000000 / totals.hits), 0) AS INTEGER) AS revenue,
  COUNT(DISTINCT CONCAT(sessionId, '-', hit.eventInfo.eventCategory, '-', COALESCE(hit.eventinfo.eventaction), '-', COALESCE(hit.eventinfo.eventlabel))) AS unique_events,
  ROUND(SAFE_DIVIDE(SUM(totals.transactions / totals.hits), COUNT(DISTINCT CASE WHEN totals.visits = 1 THEN sessionId END)), 4) AS ecommerce_conversion_rate
FROM {{ source('marts', 'ga_hit_all') }}
GROUP BY brand, date 
ORDER BY brand, date