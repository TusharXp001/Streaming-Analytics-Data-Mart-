{{ config(materialized='table') }}

SELECT
    MOVIE_ID,
    TITLE,
    GENRE,
    RELEASE_YEAR,
    unique_viewers,
    total_views,
    unique_raters,
    avg_rating
FROM {{ ref('int_movie_performance') }}
