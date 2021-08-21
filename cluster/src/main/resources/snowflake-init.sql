CREATE DATABASE IF NOT EXISTS TEST_DB;
USE TEST_DB;
DROP TABLE IF EXISTS USERS;
CREATE TABLE USERS(
                      firstName varchar(64) NOT NULL,
                      lastName varchar(64) NOT NULL,
                      age INT NOT NULL,
                      ip varchar(64) NOT NULL
);
# create file format
CREATE OR REPLACE FILE FORMAT "TEST_DB"."PUBLIC".PARQUET TYPE = 'PARQUET';

# create aws s3 stage
# A stage in Snowflake is an intermediate space where you can upload the files so that you can use the COPY command to load or unload tables.
create or replace stage USERS_S3_STAGE
              url = 'path-to-s3-bucket-folder'
              credentials = (aws_key_id='aws-access-key' aws_secret_key='aws-secret-key')
              file_format = PARQUET;

# create snowPipe on s3 stage with PARQUET file format
# Snowpipe is Snowflake's continuous data ingestion service.
# SnowPipe loads data within minutes after files are added to a stage and submitted for ingestion
create or replace pipe USERS_PIPE auto_ingest=true as copy into USERS
          from (select $1:firstname as firstname, $1:lastname as lastname, $1:age as age, $1:ip as ip from @USERS_S3_STAGE)
              file_format = PARQUET;

alter pipe USERS_PIPE refresh;

