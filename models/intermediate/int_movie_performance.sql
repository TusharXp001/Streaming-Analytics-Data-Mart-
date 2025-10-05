{{ config(materialized='view') }}

-- Aggregate movie-level performance metrics
WITH watch_data AS (
    SELECT
        wh.MOVIE_ID,
        COUNT(DISTINCT wh.USER_ID) AS unique_viewers,
        COUNT(*) AS total_views
    FROM {{ ref('stg_watch_history') }} wh
    GROUP BY wh.MOVIE_ID
),
rating_data AS (
    SELECT
        r.MOVIE_ID,
        COUNT(DISTINCT r.USER_ID) AS unique_raters,
        AVG(r.RATING) AS avg_rating
    FROM {{ ref('stg_ratings') }} r
    GROUP BY r.MOVIE_ID
)
SELECT
    m.MOVIE_ID,
    m.TITLE,
    m.GENRE,
    m.RELEASE_YEAR,
    COALESCE(w.unique_viewers, 0) AS unique_viewers,
    COALESCE(w.total_views, 0) AS total_views,
    COALESCE(r.unique_raters, 0) AS unique_raters,
    COALESCE(r.avg_rating, 0) AS avg_rating
FROM {{ ref('stg_movies') }} m
LEFT JOIN watch_data w ON m.MOVIE_ID = w.MOVIE_ID
LEFT JOIN rating_data r ON m.MOVIE_ID = r.MOVIE_ID
