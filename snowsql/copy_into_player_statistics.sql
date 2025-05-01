USE SCHEMA tennis_data.processed;

COPY INTO tennis_data.processed.player_statistics_pbp_points
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:gp_conv::FLOAT,
    $1:bp_conv::FLOAT,
    $1:sp_conv::FLOAT,
    $1:mp_conv::FLOAT,
    $1:bp_saved::FLOAT,
    $1:sp_saved::FLOAT,
    $1:mp_saved::FLOAT,
    $1:tb_spw::FLOAT,
    $1:tb_rpw::FLOAT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/pbp-points_.*\\.parquet';