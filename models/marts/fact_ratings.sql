{{ config(
    materialized='incremental',
    unique_key='RATING_ID' 
) }}

SELECT
    RATING_ID,
    USER_ID,
    MOVIE_ID,
    RATING,
    RATING_DATE,
    -- 💡 CRITICAL: Include the timestamp column used for filtering
    LOAD_TIMESTAMP 
FROM {{ ref('stg_ratings') }}

{% if is_incremental() %}
  -- Filter records in the source ('stg_ratings') that were loaded 
  -- AFTER the maximum load timestamp already in the target table ('{{ this }}').
  WHERE LOAD_TIMESTAMP > (SELECT MAX(LOAD_TIMESTAMP) FROM {{ this }})
{% endif %}