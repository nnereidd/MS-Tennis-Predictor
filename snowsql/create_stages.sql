USE DATABASE tennis_data;
USE SCHEMA processed;

-- 5 tables player statistics
CREATE OR REPLACE STAGE player_statistics_stage
  URL = 's3://tennis-predictor-data/processed/player_statistics/'
  STORAGE_INTEGRATION = tennis_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

-- 4 tables mcp
CREATE OR REPLACE STAGE match_charting_project_stage
  URL = 's3://tennis-predictor-data/processed/match_charting_project/'
  STORAGE_INTEGRATION = tennis_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

-- rankings
CREATE OR REPLACE STAGE rankings_stage
  URL = 's3://tennis-predictor-data/processed/rankings/'
  STORAGE_INTEGRATION = tennis_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

-- h2h
CREATE OR REPLACE STAGE h2h_stage
  URL = 's3://tennis-predictor-data/processed/h2h/'
  STORAGE_INTEGRATION = tennis_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

LIST @player_statistics_stage;
