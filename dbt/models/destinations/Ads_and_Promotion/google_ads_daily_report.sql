{{ config(
    materialized='view'
)}}

WITH
campaign AS (
  SELECT 
    CampaignId AS campaign_id,
    CampaignName AS campaign
  FROM {{ source('google_Ads_Coop', 'p_Campaign_8532601374') }}
  WHERE 1=1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CampaignId ORDER BY _PARTITIONTIME DESC) = 1
),
customer AS (
  SELECT
    ExternalCustomerId AS external_customer_id,
    AccountCurrencyCode AS currency_code
  FROM {{ source('google_Ads_Coop', 'p_Customer_8532601374') }}
  WHERE 1=1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ExternalCustomerId ORDER BY _PARTITIONTIME DESC) = 1
),
current_model AS (
  SELECT
    Date AS date,
    CampaignId AS campaign_id,
    SUM(CurrentModelAttributedConversions) AS conversions_current_model
  FROM {{ source('google_Ads_Coop', 'p_CampaignStats_8532601374') }}
  GROUP BY date, campaign_id
),
basic_stats AS (
  SELECT 
    Date AS date,
    ExternalCustomerId AS external_customer_id,
    CampaignId AS campaign_id,
    SUM(Clicks) AS clicks,
    SUM(Impressions) AS impressions,
    ROUND(SAFE_DIVIDE(SUM(Clicks), SUM(Impressions)), 4) AS click_through_rate,
    ROUND(SAFE_DIVIDE(SUM(Cost / 1000000), SUM(Clicks)), 2) AS avg_cost_per_click,
    SUM(Cost / 1000000) AS cost,
    -- impressions_abs_top_ratio,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN Slot = 'SearchTop' THEN impressions END), SUM(CASE WHEN Slot IN ('SearchOther', 'SearchTop') THEN impressions END)), 4) AS impressions_top_ratio,
    SUM(ViewThroughConversions) AS view_through_conversions,
    ROUND(SAFE_DIVIDE(SUM(Cost / 1000000), SUM(Conversions)), 2) AS cost_per_conversion,
    ROUND(SAFE_DIVIDE(SUM(Conversions), SUM(Clicks)), 4) AS conversion_rate,
  FROM {{ source('google_Ads_Coop', 'p_CampaignBasicStats_8532601374') }}
  GROUP BY date, external_customer_id, campaign_id
)
SELECT 
  basic_stats.date,
  campaign.campaign,
  customer.currency_code,
  basic_stats.clicks,
  basic_stats.impressions,
  basic_stats.click_through_rate,
  basic_stats.avg_cost_per_click,
  basic_stats.cost,
  basic_stats.impressions_top_ratio,
  current_model.conversions_current_model,
  basic_stats.view_through_conversions,
  basic_stats.cost_per_conversion,
  basic_stats.conversion_rate
FROM basic_stats
LEFT JOIN campaign
ON basic_stats.campaign_id = campaign.campaign_id
LEFT JOIN customer
ON basic_stats.external_customer_id = customer.external_customer_id
LEFT JOIN current_model
ON basic_stats.date = current_model.date AND basic_stats.campaign_id = current_model.campaign_id
ORDER BY campaign, date