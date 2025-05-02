USE SCHEMA tennis_data.processed;

COPY INTO tennis_data.processed.rankings
FROM (
  SELECT
    $1:elo_rank::BIGINT,
    $1:player::STRING,
    $1:age::FLOAT,
    $1:elo::FLOAT,
    $1:hard_elo_rank::BIGINT,
    $1:hard_elo::FLOAT,
    $1:clay_elo_rank::BIGINT,
    $1:clay_elo::FLOAT,
    $1:grass_elo_rank::BIGINT,
    $1:grass_elo::FLOAT,
    $1:peak_month::STRING,
    $1:atp_rank::BIGINT,
    $1:log_diff::FLOAT,
    $1:player_id::BIGINT,

    -- Extract report_date from timestamp in filename
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    metadata$filename AS source_file
  FROM @rankings_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*ms_rankings_.*\\.parquet';

COPY INTO tennis_data.processed.h2h
FROM (
  SELECT

    SPLIT_PART(REGEXP_SUBSTR(metadata$filename, '^processed/h2h/([^/]+)', 1, 1, 'e'), '-', 1) AS player_name,
    SPLIT_PART(REGEXP_SUBSTR(metadata$filename, '^processed/h2h/([^/]+)', 1, 1, 'e'), '-', 2) AS player_id,

    SPLIT_PART(REGEXP_SUBSTR(metadata$filename, '[^/]+_\\d{14}\\.parquet$'), '-', 1) AS opponent_name,
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)_\\d{14}\\.parquet$', 1, 1, 'e') AS opponent_id,

    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:date::STRING,
    $1:tournament::STRING,
    $1:surface::STRING,
    $1:rd::STRING,
    $1:rk::STRING,
    $1:vrk::STRING,
    $1:winner::STRING,
    $1:score::STRING,
    $1:tp::STRING,
    $1:aces::STRING,
    $1:dfs::STRING,
    $1:sp::STRING,
    $1:"1_sp"::STRING,
    $1:"2_sp"::STRING,
    $1:va::STRING,
    $1:time::STRING,
    $1:num_sets::BIGINT,
    $1:games_won_by_winner::BIGINT,
    $1:games_won_by_loser::BIGINT,
    $1:total_minutes::BIGINT,
    $1:"__index_level_0__"::BIGINT,

    metadata$filename AS source_file
  FROM @h2h_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/.*_\\d{14}\\.parquet';