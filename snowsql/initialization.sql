CREATE OR REPLACE DATABASE tennis_data;
CREATE OR REPLACE SCHEMA tennis_data.processed;

USE DATABASE tennis_data;
USE SCHEMA processed;

CREATE OR REPLACE STORAGE INTEGRATION tennis_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::account_id:role/tennis-predictor-s3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://tennis-predictor-data/');
  
DESC INTEGRATION tennis_s3_integration;