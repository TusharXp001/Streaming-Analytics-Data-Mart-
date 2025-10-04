CREATE OR REPLACE PROCEDURE load_status()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

 COPY INTO raw_users (user_id, name, age, gender, country, signup_date, load_timestamp, file_name)
FROM (
    SELECT
        $1::INT,
        $2::STRING,
        $3::INT,
        $4::STRING,
        $5::STRING,
        $6::DATE,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @gcs_stage/users -- Adjust the stage path as needed
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = CONTINUE;


Insert into load_status(table_name,file_name,Rows_loaded,Rows_failed)
SELECT 
'raw_users',$1,$4,$6
 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t;


COPY INTO raw_movies (movie_id, title, genre, language, release_year, load_timestamp, file_name)
FROM (
    SELECT
        $1::INT,
        $2::STRING,
        $3::STRING,
        $4::STRING,
        $5::INT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @gcs_stage/movies -- Adjust the stage path as needed
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = CONTINUE;


Insert into load_status(table_name,file_name,Rows_loaded,Rows_failed)
SELECT 
'raw_users',$1,$4,$6
 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t;

COPY INTO raw_watch_history (watch_id, user_id, movie_id, watch_date, watch_duration_minutes, device_type, load_timestamp, file_name)
FROM (
    SELECT
        $1::INT,
        $2::INT,
        $3::INT,
        $4::TIMESTAMP,
        $5::INT,
        $6::STRING,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @gcs_stage/watch_history -- Adjust the stage path as needed
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = CONTINUE;


Insert into load_status(table_name,file_name,Rows_loaded,Rows_failed)
SELECT 
'raw_users',$1,$4,$6
 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t;

COPY INTO raw_ratings (rating_id, user_id, movie_id, rating, rating_date, load_timestamp, file_name)
FROM (
    SELECT
        -- Navigate the single VARIANT column ($1) using dot notation for fields
        $1:rating_id::INT,
        $1:user_id::INT,
        $1:movie_id::INT,
        $1:rating::FLOAT,
        $1:rating_date::TIMESTAMP, -- Assumes the date is in a standard format
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @gcs_stage/ratings -- Ensure this path points to the JSON files
)
FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE  -- Use this if the entire file is a single JSON array (e.g., [ {...}, {...} ])
)
ON_ERROR = CONTINUE;


Insert into load_status(table_name,file_name,Rows_loaded,Rows_failed)
SELECT 
'raw_users',$1,$4,$6
 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t;

RETURN  'Data Loaded Successfully';
  
END;

Call load_status();


Select * from raw_movies;
Select * from raw_ratings;
Select * from raw_users;
Select * from raw_watch_history;
Select * from load_status;



truncate table raw_movies;
truncate table raw_ratings;
truncate table raw_users;
truncate table raw_watch_history;
truncate table load_status;





