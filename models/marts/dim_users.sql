{{ config(materialized='table') }}

SELECT
    USER_ID,
    COUNTRY,
    total_movies_watched,
    total_watch_events,
    total_ratings_given,
    avg_user_rating,
    last_watch_date
FROM {{ ref('int_user_engagement') }}
