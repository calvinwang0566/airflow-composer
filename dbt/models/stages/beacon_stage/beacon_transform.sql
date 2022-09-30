{{
    config(materialized='table')
}}

SELECT SPLIT(event_name, '_')[OFFSET(0)] AS event_name,
       SPLIT(event_name, '_')[OFFSET(1)] AS store_name,
       event_start_at,
       event_end_at,
       receiver_uid,
       event_type,
       DATE(event_time) AS event_date,
       DATE(first_follow) AS first_follow_date,
       DATE(bind_date) AS bind_date,
       brand_id
  FROM {{ source('beacon_api', 'beacon_api') }}