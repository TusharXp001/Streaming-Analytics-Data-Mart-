{{ config(
    materialized='incremental',
    unique_key='WATCH_ID'
) }}

SELECT
    WATCH_ID,
    USER_ID,
    MOVIE_ID,
    WATCH_DATE,
    -- NOTE: You MUST include the load_timestamp for incremental filtering
    LOAD_TIMESTAMP 
FROM {{ ref('stg_watch_history') }}

{% if is_incremental() %}
  -- This WHERE clause ensures only new records are processed on subsequent runs.
  -- It filters source data where the LOAD_TIMESTAMP is newer than the max 
  -- timestamp already in the destination table ({{ this }}).
  WHERE LOAD_TIMESTAMP > (SELECT MAX(LOAD_TIMESTAMP) FROM {{ this }})
{% endif %}
-- NO SEMICOLON HERE. dbt adds it when compiling.