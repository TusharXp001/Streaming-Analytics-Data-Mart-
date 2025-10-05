{{ config(materialized='view') }}

-- Aggregate user-level engagement metrics
WITH watch_data AS (
    SELECT
        wh.USER_ID,
        COUNT(DISTINCT wh.MOVIE_ID) AS total_movies_watched,
        COUNT(*) AS total_watch_events,
        MAX(wh.WATCH_DATE) AS last_watch_date
    FROM {{ ref('stg_watch_history') }} wh
    GROUP BY wh.USER_ID
),
rating_data AS (
    SELECT
        r.USER_ID,
        COUNT(DISTINCT r.RATING_ID) AS total_ratings_given,
        AVG(r.RATING) AS avg_user_rating
    FROM {{ ref('stg_ratings') }} r
    GROUP BY r.USER_ID
)
SELECT
    u.USER_ID,
    u.COUNTRY,
    COALESCE(w.total_movies_watched, 0) AS total_movies_watched,
    COALESCE(w.total_watch_events, 0) AS total_watch_events,
    COALESCE(r.total_ratings_given, 0) AS total_ratings_given,
    COALESCE(r.avg_user_rating, 0) AS avg_user_rating,
    w.last_watch_date
FROM {{ ref('stg_users') }} u
LEFT JOIN watch_data w ON u.USER_ID = w.USER_ID
LEFT JOIN rating_data r ON u.USER_ID = r.USER_ID
