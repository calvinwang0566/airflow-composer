{{ config(
    materialized='table',
    cluster_by = ["brand", "date"]
)}}

WITH
ga AS (
  SELECT
    brand,
    marsId,
    visitStartDateTime,
    CASE WHEN marsId IS NOT NULL THEN LAST_VALUE(fullVisitorId) OVER (PARTITION BY marsId ORDER BY visitStartDateTime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ELSE fullVisitorId END AS fullVisitorId,
    sessionId,
    CASE WHEN DENSE_RANK() OVER (PARTITION BY marsId ORDER BY DATE(visitStartDateTime)) = 1 THEN TRUE ELSE FALSE END AS isNewVisit,
    channelGrouping,
    IFNULL(mapping_type, 'others') AS mediumGrouping,
    hit.isInteraction AS hit_isInteraction,
    hit.eventInfo.eventCategory AS hit_eventCategory,
    hit.eventInfo.eventAction AS hit_eventAction,
    totals.hits AS totals_hits,
    totals.timeOnSite AS totals_timeOnSite,
    totals.pageviews AS totals_pageviews
  FROM {{ ref('ga_hit_all') }}
  LEFT JOIN {{ ref('utm_medium_mapping') }}
  ON LOWER(trafficSource.medium) = utm_medium
  WHERE 1=1
    AND totals.visits = 1
),
final AS (
    SELECT
    brand,
    marsId,
    fullVisitorId,
    DATE(visitStartDateTime) AS date,
    LOGICAL_OR(isNewVisit) AS isNewVisit,
    COUNT(DISTINCT sessionId) AS sessions,
    COUNT(DISTINCT channelGrouping) AS channels,
    COUNT(DISTINCT mediumGrouping) AS mediums,
    COUNT(DISTINCT CASE WHEN hit_isInteraction THEN hit_eventCategory END) AS events,
    ROUND(SUM(totals_timeOnSite / totals_hits)) AS browsing_time,
    ROUND(SUM(totals_pageviews / totals_hits)) AS pageviews,
    ROUND(SUM(totals_hits / totals_hits)) AS hits,
    -- channel
    COUNT(DISTINCT IF(channelGrouping = 'Affiliation', sessionId, NULL)) AS channel_affiliation_sessions,
    ROUND(SUM(IF(channelGrouping = 'Affiliation', totals_timeOnSite / totals_hits, 0))) AS channel_affiliation_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Direct', sessionId, NULL)) AS channel_direct_sessions,
    ROUND(SUM(IF(channelGrouping = 'Direct', totals_timeOnSite / totals_hits, 0))) AS channel_direct_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Display', sessionId, NULL)) AS channel_display_sessions,
    ROUND(SUM(IF(channelGrouping = 'Display', totals_timeOnSite / totals_hits, 0))) AS channel_display_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Email', sessionId, NULL)) AS channel_email_sessions,
    ROUND(SUM(IF(channelGrouping = 'Email', totals_timeOnSite / totals_hits, 0))) AS channel_email_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Organic Search', sessionId, NULL)) AS channel_organic_search_sessions,
    ROUND(SUM(IF(channelGrouping = 'Organic Search', totals_timeOnSite / totals_hits, 0))) AS channel_organic_search_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Paid Search', sessionId, NULL)) AS channel_paid_search_sessions,
    ROUND(SUM(IF(channelGrouping = 'Paid Search', totals_timeOnSite / totals_hits, 0))) AS channel_paid_search_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Referral', sessionId, NULL)) AS channel_referral_sessions,
    ROUND(SUM(IF(channelGrouping = 'Referral', totals_timeOnSite / totals_hits, 0))) AS channel_referral_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = 'Social & Influencer', sessionId, NULL)) AS channel_social_influencer_sessions,
    ROUND(SUM(IF(channelGrouping = 'Social & Influencer', totals_timeOnSite / totals_hits, 0))) AS channel_social_influencer_browsing_time,
    COUNT(DISTINCT IF(channelGrouping = '(Other)', sessionId, NULL)) AS channel_others_sessions,
    ROUND(SUM(IF(channelGrouping = '(Other)', totals_timeOnSite / totals_hits, 0))) AS channel_others_browsing_time,
    -- medium
    COUNT(DISTINCT IF(mediumGrouping = 'Organic', sessionId, NULL)) AS medium_organic_sessions,
    ROUND(SUM(IF(mediumGrouping = 'Organic', totals_timeOnSite / totals_hits, 0))) AS medium_organic_browsing_time,
    COUNT(DISTINCT IF(mediumGrouping = 'Owned Media', sessionId, NULL)) AS medium_owned_media_sessions,
    ROUND(SUM(IF(mediumGrouping = 'Owned Media', totals_timeOnSite / totals_hits, 0))) AS medium_owned_media_browsing_time,
    COUNT(DISTINCT IF(mediumGrouping = 'Paid Media', sessionId, NULL)) AS medium_paid_media_sessions,
    ROUND(SUM(IF(mediumGrouping = 'Paid Media', totals_timeOnSite / totals_hits, 0))) AS medium_paid_media_browsing_time,
    COUNT(DISTINCT IF(mediumGrouping = 'Referral', sessionId, NULL)) AS medium_referral_sessions,
    ROUND(SUM(IF(mediumGrouping = 'Referral', totals_timeOnSite / totals_hits, 0))) AS medium_referral_browsing_time,
    COUNT(DISTINCT IF(mediumGrouping = 'others', sessionId, NULL)) AS medium_others_sessions,
    ROUND(SUM(IF(mediumGrouping = 'others', totals_timeOnSite / totals_hits, 0))) AS medium_others_browsing_time,
    -- event
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Product Detail', sessionId, NULL)) AS event_product_detail_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Product Impressions', sessionId, NULL)) AS event_product_impression_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Promotion Impressions', sessionId, NULL)) AS event_promotion_impression_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Product Click', sessionId, NULL)) AS event_product_click_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Promotion Click', sessionId, NULL)) AS event_promotion_click_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Add to Cart', sessionId, NULL)) AS event_add_to_cart_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'ecommerce' AND hit_eventAction = 'add to favorites', sessionId, NULL)) AS event_add_to_favorite_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Checkout', sessionId, NULL)) AS event_checkout_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Ecommerce' AND hit_eventAction = 'Purchase', sessionId, NULL)) AS event_purchase_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'Video' AND hit_eventAction = 'Play', sessionId, NULL)) AS event_video_play_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'store locator' AND hit_eventAction = 'find a store', sessionId, NULL)) AS event_find_store_sessions,
    COUNT(DISTINCT IF(hit_eventCategory = 'internal search' AND hit_eventAction = 'display', sessionId, NULL)) AS event_internal_search_sessions,
    COUNT(DISTINCT IF(hit_eventCategory LIKE '%virtual try on%', sessionId, NULL)) AS event_virtual_try_on_sessions,
    COUNT(DISTINCT IF(hit_eventCategory LIKE '%analysis::skindr%', sessionId, NULL)) AS event_analysis_skindr_sessions,
    COUNT(DISTINCT IF(LOWER(hit_eventCategory) LIKE '%shade-finder%', sessionId, NULL)) AS event_shade_finder_sessions
    FROM ga
    GROUP BY 1, 2, 3, 4
)
SELECT
    *
FROM final