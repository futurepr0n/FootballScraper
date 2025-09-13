-- Create Play-by-Play Tables for NFL Historical Data
-- This schema supports comprehensive play-by-play tracking for historical analysis

-- Drives table to track offensive possessions
CREATE TABLE IF NOT EXISTS drives (
    id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(id),
    drive_number INTEGER,
    quarter INTEGER,
    start_time TIME,
    end_time TIME,
    start_field_position VARCHAR(10),  -- Own 20, OPP 45, etc.
    end_field_position VARCHAR(10),
    start_yard_line INTEGER,
    end_yard_line INTEGER,
    plays_count INTEGER DEFAULT 0,
    yards_gained INTEGER DEFAULT 0,
    time_of_possession INTERVAL,
    first_downs INTEGER DEFAULT 0,
    result VARCHAR(50),  -- Touchdown, Field Goal, Punt, Turnover, etc.
    scoring_play BOOLEAN DEFAULT FALSE,
    points_scored INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, drive_number)
);

-- Plays table for individual play data
CREATE TABLE IF NOT EXISTS plays (
    id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(id) ON DELETE CASCADE,
    drive_id INTEGER REFERENCES drives(id) ON DELETE CASCADE,
    play_number INTEGER,
    quarter INTEGER,
    time_remaining TIME,
    down INTEGER,
    distance INTEGER,
    yard_line INTEGER,
    field_position VARCHAR(10),  -- Own/Opp territory
    play_type VARCHAR(50),  -- Pass, Run, Punt, Field Goal, etc.
    formation VARCHAR(50),  -- Shotgun, I-Formation, Singleback, etc.
    personnel VARCHAR(20),  -- 11, 12, 21, etc.
    play_description TEXT,
    yards_gained INTEGER,
    first_down BOOLEAN DEFAULT FALSE,
    touchdown BOOLEAN DEFAULT FALSE,
    safety BOOLEAN DEFAULT FALSE,
    penalty BOOLEAN DEFAULT FALSE,
    penalty_yards INTEGER,
    penalty_type VARCHAR(100),
    turnover BOOLEAN DEFAULT FALSE,
    turnover_type VARCHAR(50),  -- Interception, Fumble
    scoring_play BOOLEAN DEFAULT FALSE,
    points_scored INTEGER DEFAULT 0,
    passer_player_id INTEGER REFERENCES players(id),
    rusher_player_id INTEGER REFERENCES players(id),
    receiver_player_id INTEGER REFERENCES players(id),
    kicker_player_id INTEGER REFERENCES players(id),
    returner_player_id INTEGER REFERENCES players(id),
    tackler1_player_id INTEGER REFERENCES players(id),
    tackler2_player_id INTEGER REFERENCES players(id),
    pass_length VARCHAR(20),  -- Short, Medium, Deep
    pass_location VARCHAR(20),  -- Left, Middle, Right
    run_location VARCHAR(20),  -- Left, Middle, Right
    run_gap VARCHAR(20),  -- A, B, C, D gaps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, play_number)
);

-- Situational stats for analytics
CREATE TABLE IF NOT EXISTS situational_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(id),
    team_id INTEGER REFERENCES teams(id),
    season INTEGER,
    season_type VARCHAR(20),
    
    -- Red Zone Stats (inside 20)
    redzone_attempts INTEGER DEFAULT 0,
    redzone_completions INTEGER DEFAULT 0,
    redzone_passing_yards INTEGER DEFAULT 0,
    redzone_passing_tds INTEGER DEFAULT 0,
    redzone_rushing_attempts INTEGER DEFAULT 0,
    redzone_rushing_yards INTEGER DEFAULT 0,
    redzone_rushing_tds INTEGER DEFAULT 0,
    redzone_targets INTEGER DEFAULT 0,
    redzone_receptions INTEGER DEFAULT 0,
    redzone_receiving_yards INTEGER DEFAULT 0,
    redzone_receiving_tds INTEGER DEFAULT 0,
    
    -- Third Down Stats
    third_down_attempts INTEGER DEFAULT 0,
    third_down_conversions INTEGER DEFAULT 0,
    third_down_passing_attempts INTEGER DEFAULT 0,
    third_down_passing_conversions INTEGER DEFAULT 0,
    third_down_rushing_attempts INTEGER DEFAULT 0,
    third_down_rushing_conversions INTEGER DEFAULT 0,
    
    -- Two Minute Drill Stats
    two_minute_attempts INTEGER DEFAULT 0,
    two_minute_completions INTEGER DEFAULT 0,
    two_minute_passing_yards INTEGER DEFAULT 0,
    two_minute_passing_tds INTEGER DEFAULT 0,
    two_minute_rushing_attempts INTEGER DEFAULT 0,
    two_minute_rushing_yards INTEGER DEFAULT 0,
    
    -- Fourth Quarter/OT Stats
    fourth_quarter_attempts INTEGER DEFAULT 0,
    fourth_quarter_completions INTEGER DEFAULT 0,
    fourth_quarter_passing_yards INTEGER DEFAULT 0,
    fourth_quarter_passing_tds INTEGER DEFAULT 0,
    fourth_quarter_comebacks INTEGER DEFAULT 0,
    fourth_quarter_game_winning_drives INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season, season_type)
);

-- Team situational stats
CREATE TABLE IF NOT EXISTS team_situational_stats (
    id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(id),
    season INTEGER,
    season_type VARCHAR(20),
    
    -- Red Zone Offense
    redzone_trips INTEGER DEFAULT 0,
    redzone_tds INTEGER DEFAULT 0,
    redzone_fgs INTEGER DEFAULT 0,
    redzone_scoring_pct DECIMAL(5,2),
    
    -- Red Zone Defense
    redzone_trips_allowed INTEGER DEFAULT 0,
    redzone_tds_allowed INTEGER DEFAULT 0,
    redzone_fgs_allowed INTEGER DEFAULT 0,
    redzone_stops INTEGER DEFAULT 0,
    
    -- Third Down
    third_down_attempts INTEGER DEFAULT 0,
    third_down_conversions INTEGER DEFAULT 0,
    third_down_pct DECIMAL(5,2),
    third_down_attempts_against INTEGER DEFAULT 0,
    third_down_conversions_allowed INTEGER DEFAULT 0,
    third_down_defense_pct DECIMAL(5,2),
    
    -- Time of Possession
    avg_time_of_possession TIME,
    avg_plays_per_game DECIMAL(5,1),
    avg_yards_per_play DECIMAL(4,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season, season_type)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_drives_game_id ON drives(game_id);
CREATE INDEX IF NOT EXISTS idx_drives_team_id ON drives(team_id);
CREATE INDEX IF NOT EXISTS idx_plays_game_id ON plays(game_id);
CREATE INDEX IF NOT EXISTS idx_plays_drive_id ON plays(drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_quarter ON plays(quarter);
CREATE INDEX IF NOT EXISTS idx_plays_play_type ON plays(play_type);
CREATE INDEX IF NOT EXISTS idx_plays_down_distance ON plays(down, distance);
CREATE INDEX IF NOT EXISTS idx_plays_scoring ON plays(scoring_play);
CREATE INDEX IF NOT EXISTS idx_situational_player_season ON situational_stats(player_id, season);
CREATE INDEX IF NOT EXISTS idx_team_situational_season ON team_situational_stats(team_id, season);

-- Add historical data tracking to games table if not exists
ALTER TABLE games ADD COLUMN IF NOT EXISTS has_play_by_play BOOLEAN DEFAULT FALSE;
ALTER TABLE games ADD COLUMN IF NOT EXISTS data_quality VARCHAR(20) DEFAULT 'basic'; -- basic, detailed, complete

-- Create a historical data import tracking table
CREATE TABLE IF NOT EXISTS historical_import_status (
    id SERIAL PRIMARY KEY,
    season INTEGER,
    season_type VARCHAR(20),
    week INTEGER,
    import_type VARCHAR(50), -- schedule, boxscore, playbyplay
    status VARCHAR(20), -- pending, in_progress, completed, failed
    games_processed INTEGER DEFAULT 0,
    games_total INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, season_type, week, import_type)
);

COMMENT ON TABLE drives IS 'Stores offensive drive information for each game';
COMMENT ON TABLE plays IS 'Stores individual play data with detailed information';
COMMENT ON TABLE situational_stats IS 'Aggregated situational statistics for players';
COMMENT ON TABLE team_situational_stats IS 'Aggregated situational statistics for teams';
COMMENT ON TABLE historical_import_status IS 'Tracks the status of historical data imports';