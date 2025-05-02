USE SCHEMA tennis_data.processed;

COPY INTO tennis_data.processed.match_charting_project_rally
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:rallylen::FLOAT,
    $1:"rlen-serve"::FLOAT,
    $1:"rlen-return"::FLOAT,
    $1:"1-3_w"::FLOAT AS win_1_3,
    $1:"4-6_w"::FLOAT AS win_4_6,
    $1:"7-9_w"::FLOAT AS win_7_9,
    $1:"10+_w"::FLOAT AS win_10_plus,
    $1:fhpergs::FLOAT AS fh_per_gs,
    $1:bh_slice::FLOAT,
    $1:fhp::FLOAT,
    $1:fhpper100::FLOAT AS fhp_per_100,
    $1:bhp::FLOAT,
    $1:bhpper100::FLOAT AS bhp_per_100,

    metadata$filename AS source_file
  FROM @match_charting_project_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/mcp-rally_.*\\.parquet';

COPY INTO tennis_data.processed.match_charting_project_return
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:rip::FLOAT,
    $1:rip_w::FLOAT,
    $1:retwnr::FLOAT,
    $1:fhperbh::STRING AS fh_per_bh,
    $1:rdi::STRING,
    $1:slice::FLOAT,
    
    $1:"1st_rip"::FLOAT AS first_rip,
    $1:"rip_w.1"::FLOAT AS first_rip_w,
    $1:"retwnr.1"::FLOAT AS first_retwnr,
    $1:"rdi.1"::STRING AS first_rdi,
    $1:"slice.1"::FLOAT AS first_slice,
    $1:"2nd_rip"::FLOAT AS second_rip,
    $1:"rip_w.2"::FLOAT AS second_rip_w,
    $1:"retwnr.2"::FLOAT AS second_retwnr,
    $1:"rdi.2"::STRING AS second_rdi,
    $1:"slice.2"::FLOAT AS second_slice,

    metadata$filename AS source_file
  FROM @match_charting_project_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/mcp-return_.*\\.parquet';

COPY INTO tennis_data.processed.match_charting_project_serve
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:unret::FLOAT,
    $1:"<=3_w.1"::FLOAT AS leq3_w_1st,
    $1:"rip_w.1"::FLOAT AS rip_w_1st,
    $1:"svimpact.1"::FLOAT AS svimpact_1st,
    $1:"1st_unret"::FLOAT AS first_unret,
    $1:"<=3_w.1"::FLOAT AS leq3_w_first_unret,
    $1:"rip_w.1"::FLOAT AS rip_w_first_unret,
    $1:"svimpact.1"::FLOAT AS svimpact_first_unret,
    $1:d_wide::FLOAT,
    $1:a_wide::FLOAT,
    $1:bp_wide::FLOAT,
    $1:"2nd_unret"::FLOAT AS second_unret,
    $1:"<=3_w.2"::FLOAT AS leq3_w_second_unret,
    $1:"rip_w.2"::FLOAT AS rip_w_second_unret,
    $1:"2ndagg"::STRING AS secondagg,

    metadata$filename AS source_file
  FROM @match_charting_project_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/mcp-serve_.*\\.parquet';

COPY INTO tennis_data.processed.match_charting_project_tactics
FROM (
  SELECT
    REGEXP_SUBSTR(metadata$filename, '-(\\d+)', 1, 1, 'e') AS player_id,
    TO_DATE(LEFT(REGEXP_SUBSTR(metadata$filename, '_\\d{14}'), 9), '_YYYYMMDD') AS report_date,

    $1:match::STRING,
    $1:result::STRING,
    $1:snv_freq::FLOAT,
    $1:snv_w::FLOAT,
    $1:net_freq::FLOAT,
    $1:net_w::FLOAT,
    $1:fh_wnr::FLOAT,
    $1:dtl_wnr::FLOAT AS fh_dtl_wnr,
    $1:io_wnr::FLOAT AS fh_io_wnr,
    $1:bh_wnr::FLOAT,
    $1:"dtl_wnr.1"::FLOAT AS bh_dtl_wnr,
    $1:drop_freq::FLOAT,
    $1:wnr::FLOAT AS total_wnr,
    $1:rallyagg::INT,
    $1:returnagg::INT,

    metadata$filename AS source_file
  FROM @match_charting_project_stage
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/mcp-tactics_.*\\.parquet';