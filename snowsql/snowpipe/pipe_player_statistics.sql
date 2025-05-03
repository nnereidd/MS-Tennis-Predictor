USE SCHEMA tennis_data.processed;

CREATE OR REPLACE PIPE pipe_ps_pbp_points
AUTO_INGEST = TRUE
AS
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

CREATE OR REPLACE PIPE pipe_ps_pbp_games
AUTO_INGEST = TRUE
AS
COPY INTO tennis_data.processed.player_statistics_pbp_games
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:bp_games::FLOAT,
    $1:bp_convperbpg::FLOAT,
    $1:breakback::FLOAT,
    $1:bpf_games::FLOAT,
    $1:holdperbpfg::FLOAT,
    $1:consol::FLOAT,
    $1:svforset::FLOAT,
    $1:svstayset::FLOAT,
    $1:svformatch::FLOAT,
    $1:svstaymatch::FLOAT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/pbp-games_.*\\.parquet';

CREATE OR REPLACE PIPE pipe_ps_pbp_stats
AUTO_INGEST = TRUE
AS
COPY INTO tennis_data.processed.player_statistics_pbp_stats
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:blr::FLOAT,
    $1:"dr+"::FLOAT,
    $1:ei::FLOAT,
    $1:cbf::FLOAT,
    $1:deuce_a::FLOAT,
    $1:deuce_spw::FLOAT,
    $1:ad_a::FLOAT,
    $1:ad_spw::FLOAT,
    $1:deuce_rpw::FLOAT,
    $1:ad_rpw::FLOAT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/pbp-stats_.*\\.parquet';

CREATE OR REPLACE PIPE pipe_ps_serve_speed
AUTO_INGEST = TRUE
AS
COPY INTO tennis_data.processed.player_statistics_serve_speed
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:avg_speed::FLOAT,
    $1:"1st_avg"::FLOAT,
    $1:"1st_stdev"::FLOAT,
    $1:"1st_t_avg"::FLOAT,
    $1:"1st_wide_avg"::FLOAT,
    $1:"max_1st"::INT,
    $1:"min_1st"::FLOAT,
    $1:"2nd_avg"::FLOAT,
    $1:"2nd_stdev"::FLOAT,
    $1:"2nd_t_avg"::FLOAT,
    $1:"2nd_wide_avg"::FLOAT,
    $1:"max_2nd"::FLOAT,
    $1:"min_2nd"::INT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/serve-speed_.*\\.parquet';

CREATE OR REPLACE PIPE pipe_ps_winners_errors
AUTO_INGEST = TRUE
AS
COPY INTO tennis_data.processed.player_statistics_winners_errors
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:winners::BIGINT,
    $1:ufes::BIGINT,
    $1:ratio::FLOAT,
    $1:wnrperpt::FLOAT,
    $1:ufeperpt::FLOAT,
    $1:rallywinners::BIGINT,
    $1:rallyufes::BIGINT,
    $1:rallyratio::FLOAT,
    $1:rally_wnrperpt::FLOAT,
    $1:rally_ufeperpt::FLOAT,
    $1:fh_wnrperpt::FLOAT,
    $1:bh_wnrperpt::FLOAT,
    $1:vs_ratio::FLOAT,
    $1:vs_wnrperpt::FLOAT,
    $1:vs_ufeperpt::FLOAT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/winners-errors_.*\\.parquet';