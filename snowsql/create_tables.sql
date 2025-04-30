-- TABLE: rankings
CREATE OR REPLACE TABLE tennis_data.processed.rankings (
  elo_rank BIGINT,
  player STRING,
  age FLOAT,
  elo FLOAT,
  hard_elo_rank BIGINT,
  hard_elo FLOAT,
  clay_elo_rank BIGINT,
  clay_elo FLOAT,
  grass_elo_rank BIGINT,
  grass_elo FLOAT,
  peak_month STRING,
  atp_rank BIGINT,
  log_diff FLOAT,
  player_id BIGINT,
  report_date DATE,
  source_file STRING
);

-- TABLE: player_statistics
CREATE OR REPLACE TABLE tennis_data.processed.player_statistics_pbp_points (
  player_name STRING,
  player_id STRING,
  report_date DATE,
  match STRING,
  result STRING,
  gp_conv FLOAT,   -- Game points converted
  bp_conv FLOAT,   -- Break points converted
  sp_conv FLOAT,   -- Set points converted
  mp_conv FLOAT,   -- Match points converted
  bp_saved FLOAT,  -- Break points saved
  sp_saved FLOAT,  -- Set points saved
  mp_saved FLOAT,  -- Match points saved
  tb_spw FLOAT,    -- Tiebreak serve points won
  tb_rpw FLOAT,    -- Tiebreak return points won
  source_file STRING
);


CREATE OR REPLACE TABLE tennis_data.processed.player_statistics_pbp_games (
  player_name STRING,
  player_id STRING,
  report_date DATE,
  match STRING,
  result STRING,
  bp_games FLOAT,         -- Break point games
  bp_convperbpg FLOAT,     -- Break point conversion per break point game
  breakback FLOAT,         -- Breakback percentage
  bpf_games FLOAT,         -- Break point faced games
  holdperbpfg FLOAT,       -- Holds per break point faced games
  consol FLOAT,            -- Consolidation rate
  svforset FLOAT,          -- Serve out set success
  svstayset FLOAT,         -- Stay in set success
  svformatch FLOAT,        -- Serve out match success
  svstaymatch FLOAT,       -- Stay in match success
  source_file STRING
);

CREATE OR REPLACE TABLE tennis_data.processed.player_statistics_pbp_stats (
  player_name STRING,
  player_id STRING,
  report_date DATE,
  match STRING,
  result STRING,
  blr FLOAT,        -- Balanced Leverage Ratio
  dr_plus FLOAT,    -- Dominance Ratio Plus (DR+)
  ei FLOAT,         -- Excitement Index
  cbf FLOAT,        -- Comeback Factor
  deuce_ace_pct FLOAT,       -- Deuce Ace %
  deuce_spw_pct FLOAT,       -- Deuce Serve Points Won %
  ad_ace_pct FLOAT,          -- Ad-court Ace %
  ad_spw_pct FLOAT,          -- Ad-court Serve Points Won %
  deuce_rpw_pct FLOAT,       -- Deuce Return Points Won %
  ad_rpw_pct FLOAT,          -- Ad-court Return Points Won %
  source_file STRING
);

CREATE OR REPLACE TABLE tennis_data.processed.player_statistics_serve_speed (
  player_name STRING,
  player_id STRING,
  report_date DATE,
  match STRING,
  result STRING,
  avg_speed FLOAT,
  first_avg FLOAT,
  first_stdev FLOAT,
  first_t_avg FLOAT,
  first_wide_avg FLOAT,
  max_first INT,
  min_first INT,
  second_avg FLOAT,
  second_stdev FLOAT,
  second_t_avg FLOAT,
  second_wide_avg FLOAT,
  max_second INT,
  min_second INT,
  source_file STRING
);

CREATE OR REPLACE TABLE tennis_data.processed.player_statistics_winners_errors (
  player_name STRING,
  player_id STRING,
  report_date DATE,
  match STRING,
  result STRING,
  winners BIGINT,
  ufes BIGINT,
  ratio FLOAT,
  wnrperpt FLOAT,
  ufeperpt FLOAT,
  rallywinners BIGINT,
  rallyufes BIGINT,
  rallyratio FLOAT,
  rally_wnrperpt FLOAT,
  rally_ufeperpt FLOAT,
  fh_wnrperpt FLOAT,
  bh_wnrperpt FLOAT,
  vs_ratio FLOAT,
  vs_wnrperpt FLOAT,
  vs_ufeperpt FLOAT,
  source_file STRING
);

-- TABLE: match_charting_project
CREATE OR REPLACE TABLE tennis_data.processed.match_charting_project_rally (
  player_name STRING,        
  player_id STRING,          
  report_date DATE,           
  match STRING,               
  result STRING,           
  rallylen FLOAT,             -- Average rally length (shots per point)
  rlen_serve FLOAT,           -- Average rally length on service points
  rlen_return FLOAT,          -- Average rally length on return points
  win_1_3 FLOAT,              -- Win percentage on rallies 1–3 shots
  win_4_6 FLOAT,              -- Win percentage on rallies 4–6 shots
  win_7_9 FLOAT,              -- Win percentage on rallies 7–9 shots
  win_10_plus FLOAT,          -- Win percentage on rallies 10+ shots
  fh_per_gs FLOAT,            -- Forehands per groundstroke (FH/GS)
  bh_slice FLOAT,             -- Backhand slice percentage (BH slice %)
  fhp FLOAT,                  -- Forehand Potency (per match)
  fhp_per_100 FLOAT,          -- Forehand Potency per 100 forehands
  bhp FLOAT,                  -- Backhand Potency (per match)
  bhp_per_100 FLOAT,          -- Backhand Potency per 100 backhands
  source_file STRING         
);


CREATE OR REPLACE TABLE tennis_data.processed.match_charting_project_return (
  player_name STRING,        
  player_id STRING,       
  report_date DATE,          
  match STRING,              
  result STRING,            
  rip FLOAT,                 -- Return in play percentage (RiP%)
  rip_w FLOAT,               -- Return in play winning percentage (RiP W%)
  retwnr FLOAT,              -- Return winner percentage (RetWnr%)
  fhperbh FLOAT,             -- Winner forehand percentage (Wnr FH%)
  rdi FLOAT,                 -- Return Depth Index (depth-weighted average)
  slice FLOAT,               -- Slice/chip return percentage
  first_rip FLOAT,           -- 1st serve return in play percentage
  first_rip_w FLOAT,         -- 1st serve return in play winning percentage
  first_retwnr FLOAT,        -- 1st serve return winner percentage
  first_rdi FLOAT,           -- 1st serve Return Depth Index
  first_slice FLOAT,         -- 1st serve slice/chip return percentage
  second_rip FLOAT,          -- 2nd serve return in play percentage
  second_rip_w FLOAT,        -- 2nd serve return in play winning percentage
  second_retwnr FLOAT,       -- 2nd serve return winner percentage
  second_rdi FLOAT,          -- 2nd serve Return Depth Index
  second_slice FLOAT,        -- 2nd serve slice/chip return percentage
  source_file STRING         
);


CREATE OR REPLACE TABLE tennis_data.processed.match_charting_project_serve (
  player_name STRING,       
  player_id STRING,         
  report_date DATE,         
  match STRING,             
  result STRING,            

  unret FLOAT,              -- Unreturnable serve percentage (aces, service winners, return errors)
  leq3_w_1st FLOAT,         -- % of points won within 3 shots after 1st serve (serve + 1 or +2 shot)
  rip_w_1st FLOAT,          -- % of points won when return came back in play (1st serve only)
  svimpact_1st FLOAT,       -- Serve Impact on 1st serve (see glossary for formula)
  first_unret FLOAT,            -- Unreturnable rate on first serves
  leq3_w_first_unret FLOAT,     -- 3-shot win rate on first unreturned serves
  rip_w_first_unret FLOAT,      -- Win rate when return came back after first serve
  svimpact_first_unret FLOAT,   -- Same as above but specific to unreturned 1st serves
  d_wide FLOAT,             -- Deuce-court wide serve percentage (both 1st and 2nd serves)
  a_wide FLOAT,             -- Ad-court wide serve percentage
  bp_wide FLOAT,            -- Ad-court wide serve percentage on break points
  second_unret FLOAT,           -- Unreturnable second serve rate
  leq3_w_second_unret FLOAT,    -- 3-shot win rate on second serves
  rip_w_second_unret FLOAT,     -- Win rate after return came back on 2nd serve
  secondagg STRING,             -- Second serve aggression category 

  source_file STRING        
);

CREATE OR REPLACE TABLE tennis_data.processed.match_charting_project_tactics (
  player_name STRING,          
  player_id STRING,            
  report_date DATE,         
  match STRING,           
  result STRING,                
  snv_freq FLOAT,                -- Serve-and-volley frequency (excluding aces)
  snv_w FLOAT,                   -- Serve-and-volley winning percentage
  net_freq FLOAT,                -- Net point frequency (serve + general approaches)
  net_w FLOAT,                   -- Net point winning percentage
  fh_wnr FLOAT,                  -- Forehand winner percentage
  fh_dtl_wnr FLOAT,               -- Forehand down-the-line winner percentage
  fh_io_wnr FLOAT,                -- Forehand inside-out winner percentage
  bh_wnr FLOAT,                  -- Backhand winner percentage
  bh_dtl_wnr FLOAT,               -- Backhand down-the-line winner percentage
  drop_freq FLOAT,               -- Dropshot frequency (groundstroke dropshots only)
  drop_wnr FLOAT,                -- Dropshot winner percentage
  rallyagg STRING,               -- Rally aggression score (normalized vs tour average)
  returnagg STRING,              -- Return aggression score (normalized vs tour average)
  source_file STRING             
);

-- TABLE: h2h
CREATE OR REPLACE TABLE tennis_data.processed.h2h (
  player_name STRING,       
  player_id STRING,          
  opponent_name STRING,    
  opponent_id STRING,        
  report_date DATE,          
  date STRING,
  tournament STRING,
  surface STRING,
  rd STRING,
  rk STRING,
  vrk STRING,
  winner STRING,
  score STRING,
  tp STRING,
  aces STRING,
  dfs STRING,
  sp STRING,
  first_sp STRING,
  second_sp STRING,
  va STRING,
  time STRING,
  num_sets BIGINT,
  games_won_by_winner BIGINT,
  games_won_by_loser BIGINT,
  total_minutes BIGINT,
  index_level BIGINT,
  source_file STRING
);