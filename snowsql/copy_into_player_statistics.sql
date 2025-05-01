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

COPY INTO tennis_data.processed.player_statistics_pbp_stats
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:blr::FLOAT,
    $1:dr_plus::FLOAT,
    $1:ei::FLOAT,
    $1:cbf::FLOAT,
    $1:deuce_ace_pct::FLOAT,
    $1:deuce_spw_pct::FLOAT,
    $1:ad_ace_pct::FLOAT,
    $1:ad_spw_pct::FLOAT,
    $1:deuce_rpw_pct::FLOAT,
    $1:ad_rpw_pct::FLOAT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/pbp-stats_.*\\.parquet';

COPY INTO tennis_data.processed.player_statistics_serve_speed
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:avg_speed::FLOAT,
    $1:first_avg::FLOAT,
    $1:first_stdev::FLOAT,
    $1:first_t_avg::FLOAT,
    $1:first_wide_avg::FLOAT,
    $1:max_first::INT,
    $1:min_first::INT,
    $1:second_avg::FLOAT,
    $1:second_stdev::FLOAT,
    $1:second_t_avg::FLOAT,
    $1:second_wide_avg::FLOAT,
    $1:max_second::INT,
    $1:min_second::INT,

    metadata$filename AS source_file
  FROM @player_statistics_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/serve-speed_.*\\.parquet';

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