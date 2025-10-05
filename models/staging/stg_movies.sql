{{ config(materialized='incremental', unique_key='movie_id') }}

SELECT
    MOVIE_ID AS movie_id,
    TITLE AS title,
    GENRE AS genre,
    LANGUAGE AS language,
    RELEASE_YEAR AS release_year,
    LOAD_TIMESTAMP AS load_timestamp
FROM {{ source('RAW', 'raw_movies') }}

{% if is_incremental() %}
WHERE LOAD_TIMESTAMP > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
