{{ config(materialized='incremental', unique_key='rating_id') }}

SELECT
    RATING_ID AS rating_id,
    USER_ID AS user_id,
    MOVIE_ID AS movie_id,
    RATING AS rating,
    RATING_DATE AS rating_date,
    LOAD_TIMESTAMP AS load_timestamp
FROM {{ source('RAW', 'raw_ratings') }}

{% if is_incremental() %}
WHERE LOAD_TIMESTAMP > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
