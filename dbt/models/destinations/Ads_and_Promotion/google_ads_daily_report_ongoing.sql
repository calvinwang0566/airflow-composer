{{ config(
    materialized='view'
)}}

WITH
campaign_source AS (
  SELECT 
    *
  FROM {{ source('google_Ads_Ms', 'p_Campaign_7806077362') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CampaignId ORDER BY _PARTITIONTIME DESC) = 1
  UNION DISTINCT
  SELECT 
    *
  FROM {{ source('google_Ads_Coop', 'p_Campaign_8532601374') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CampaignId ORDER BY _PARTITIONTIME DESC) = 1
),
campaign AS (
  SELECT 
    CampaignId AS campaign_id,
    CampaignName AS campaign,
    AdvertisingChannelType AS campaign_type
  FROM campaign_source
),
customer_source AS (
  SELECT
    *
  FROM {{ source('google_Ads_Ms', 'p_Customer_7806077362') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ExternalCustomerId ORDER BY _PARTITIONTIME DESC) = 1
  UNION DISTINCT
  SELECT
    *
  FROM {{ source('google_Ads_Coop', 'p_Customer_8532601374') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ExternalCustomerId ORDER BY _PARTITIONTIME DESC) = 1
),
customer AS (
  SELECT
    AccountDescriptiveName AS account,
    ExternalCustomerId AS external_customer_id,
    AccountCurrencyCode AS currency_code
  FROM customer_source
),
current_model_source AS (
  SELECT 
    *
  FROM {{ source('google_Ads_Ms', 'p_CampaignStats_7806077362') }}
  UNION DISTINCT
  SELECT 
    *
  FROM {{ source('google_Ads_Coop', 'p_CampaignStats_8532601374') }}
),
current_model AS (
  SELECT
    Date AS date,
    CampaignId AS campaign_id,
    CAST(SUM(CurrentModelAttributedConversions) AS INTEGER) AS conversions_current_model
  FROM current_model_source
  GROUP BY date, campaign_id
),
basic_stats_source AS (
  SELECT 
    *
  FROM {{ source('google_Ads_Ms', 'p_CampaignBasicStats_7806077362') }}
  UNION DISTINCT
  SELECT 
    *
  FROM {{ source('google_Ads_Coop', 'p_CampaignBasicStats_8532601374') }}
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
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN Slot = 'SearchTop' THEN impressions END), SUM(CASE WHEN Slot IN ('SearchOther', 'SearchTop') THEN impressions END)), 4) AS impressions_top_ratio,
    SUM(ViewThroughConversions) AS view_through_conversions,
    ROUND(SAFE_DIVIDE(SUM(Cost / 1000000), SUM(Conversions)), 2) AS cost_per_conversion,
    ROUND(SAFE_DIVIDE(SUM(Conversions), SUM(Clicks)), 4) AS conversion_rate,
  FROM basic_stats_source
  GROUP BY date, external_customer_id, campaign_id
),
advanced_stats_source AS (
  SELECT 
    *
  FROM {{ source('google_Ads_Ms', 'p_CampaignCookieStats_7806077362') }}
  UNION DISTINCT
  SELECT 
    *
  FROM {{ source('google_Ads_Coop', 'p_CampaignCookieStats_8532601374') }}
),
advanced_stats AS (
  SELECT
    Date AS date,
    CampaignId AS campaign_id,
    CAST(AbsoluteTopImpressionPercentage AS FLOAT64) AS impressions_absolute_top_ratio
  FROM advanced_stats_source
),
final AS (
  SELECT
    account AS Account,
    basic_stats.date AS Date,
    campaign.campaign AS Campaign_name,
    campaign.campaign_type AS Advertising_channel_type,
    customer.currency_code AS Currency_code,
    basic_stats.clicks AS Clicks,
    basic_stats.impressions AS impressions,
    basic_stats.click_through_rate AS CTR,
    basic_stats.avg_cost_per_click AS CPC,
    basic_stats.cost AS Cost,
    advanced_stats.impressions_absolute_top_ratio AS Absolute_top_impression_percentage,
    basic_stats.impressions_top_ratio AS Top_impression_percentage,
    current_model.conversions_current_model AS Conversions,
    basic_stats.view_through_conversions AS View_through_conversions,
    basic_stats.cost_per_conversion AS Cost_per_conversion,
    basic_stats.conversion_rate AS Conversion_rate
  FROM basic_stats
  INNER JOIN campaign
  ON basic_stats.campaign_id = campaign.campaign_id
  INNER JOIN customer
  ON basic_stats.external_customer_id = customer.external_customer_id
  INNER JOIN current_model
  ON basic_stats.date = current_model.date AND basic_stats.campaign_id = current_model.campaign_id
  INNER JOIN advanced_stats
  ON basic_stats.date = advanced_stats.date AND basic_stats.campaign_id = advanced_stats.campaign_id
)
SELECT
  *
FROM final
ORDER BY Campaign_name, Date