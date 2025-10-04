CREATE OR REPLACE DATABASE STREAMING_DB;
CREATE OR REPLACE SCHEMA RAW;

CREATE OR REPLACE STORAGE INTEGRATION gcs_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://ecom_dbt');

Desc Integration gcs_integration;

CREATE OR REPLACE STAGE gcs_stage
  URL = 'gcs://ecom_dbt'
  STORAGE_INTEGRATION = gcs_integration;

list  @gcs_stage; 

CREATE OR REPLACE TABLE raw_users (
    user_id INT,
    name STRING,
    age INT,
    gender STRING,
    country STRING,
    signup_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name STRING
);

CREATE OR REPLACE TABLE raw_movies (
    movie_id INT,
    title STRING,
    genre STRING,
    language STRING,
    release_year INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name STRING
);

CREATE OR REPLACE TABLE raw_watch_history (
    watch_id INT,
    user_id INT,
    movie_id INT,
    watch_date TIMESTAMP,
    watch_duration_minutes INT,
    device_type STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name STRING
);

CREATE OR REPLACE TABLE raw_ratings (
    rating_id INT,
    user_id INT,
    movie_id INT,
    rating FLOAT,
    rating_date TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name STRING
);

CREATE OR REPLACE TABLE load_status (
    table_name STRING,
    file_name STRING,
    rows_loaded INT,
    rows_failed INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);




