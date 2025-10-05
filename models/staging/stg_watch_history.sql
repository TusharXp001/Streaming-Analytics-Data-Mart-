{{ config(materialized='incremental', unique_key='watch_id') }}

SELECT
    WATCH_ID AS watch_id,
    USER_ID AS user_id,
    MOVIE_ID AS movie_id,
    WATCH_DATE AS watch_date,
    WATCH_DURATION_MINUTES AS watch_duration_minutes,
    DEVICE_TYPE AS device_type,
    LOAD_TIMESTAMP AS load_timestamp
FROM {{ source('RAW', 'raw_watch_history') }}

{% if is_incremental() %}
WHERE LOAD_TIMESTAMP > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
