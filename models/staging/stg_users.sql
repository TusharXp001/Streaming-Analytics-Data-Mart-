{{ config(materialized='incremental', unique_key='user_id') }}

SELECT
    USER_ID AS user_id,
    NAME AS name,
    AGE AS age,
    GENDER AS gender,
    COUNTRY AS country,
    SIGNUP_DATE AS signup_date,
    LOAD_TIMESTAMP AS load_timestamp
FROM {{ source('RAW', 'raw_users') }}

{% if is_incremental() %}
WHERE LOAD_TIMESTAMP > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
