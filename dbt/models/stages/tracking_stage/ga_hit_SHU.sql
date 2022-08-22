{{ config(
    materialized='table',
    cluster_by = ["visitStartDateTime", "channelGrouping", "isNewVisit"]
)}}

WITH 
final AS (
    SELECT
        (SELECT value FROM UNNEST(hit.customDimensions) AS customDimension WHERE customDimension.index = 10) AS marsId,
        CONCAT(date, '-', fullvisitorId, '-', CAST(visitId AS STRING)) AS sessionId,
        fullVisitorId,
        visitId,
        visitNumber,
        channelGrouping,
        IF(visitNumber = 1, TRUE, FALSE) AS isNewVisit,
        DATETIME(TIMESTAMP_SECONDS(visitStartTime), '+8') AS visitStartDateTime,
        STRUCT(
            totals.visits,
            totals.hits,
            totals.pageviews,
            totals.timeOnSite,
            totals.transactions,
            totals.totalTransactionRevenue,
            totals.bounces
        ) AS totals,
        STRUCT(
            trafficSource.referralPath,
            trafficSource.campaign,
            trafficSource.source,
            trafficSource.medium,
            trafficSource.keyword,
            trafficSource.adContent
        ) AS trafficSource,
        STRUCT(
            device.browser,
            device.operatingSystem,
            device.isMobile,
            device.mobileDeviceBranding,
            device.mobileDeviceModel,
            device.mobileDeviceInfo,
            device.deviceCategory
        ) AS device,
        STRUCT(
            geoNetwork.continent,
            geoNetwork.subContinent,
            geoNetwork.country,
            geoNetwork.region,
            geoNetwork.metro,
            geoNetwork.city
        ) AS geoNetwork,
        STRUCT(
            hit.type,
            hit.hitNumber,
            DATETIME(TIMESTAMP_MILLIS(visitStartTime * 1000 + hit.time), '+8') AS hitDateTime,
            hit.isInteraction,
            hit.isEntrance,
            hit.isExit,
            hit.referer,
            hit.page,
            hit.product,
            hit.contentGroup,
            hit.eventInfo,
            hit.experiment
        ) AS hit
    FROM {{ source('tracking_SHU', 'ga_sessions_*') }},
    UNNEST(hits) AS hit
    WHERE 1=1
)
SELECT
    '40' AS brand,
    LAST_VALUE(marsId IGNORE NULLS) OVER (PARTITION BY fullVisitorId ORDER BY visitNumber, hit.hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS marsId,
    * EXCEPT(marsId)
FROM final 