-- ── Sofascore schema ──────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS sofascore;

-- ── dim_matches ───────────────────────────────────────────────────────────────
-- One row per match. Populated from build_events_df() in scraping_script.py.

CREATE TABLE IF NOT EXISTS sofascore.dim_matches (
    event_id            bigint          PRIMARY KEY,
    season              varchar(10)     NOT NULL,
    round               int,
    home_team           varchar(100)    NOT NULL,
    away_team           varchar(100)    NOT NULL,
    home_score          int,
    away_score          int,
    winner_code         int,            -- 1=home, 2=away, 3=draw
    status              varchar(30)     NOT NULL,
    start_timestamp     bigint,
    has_player_stats    boolean,
    match_date          date            NOT NULL
);

-- ── fact_player_match_stats ───────────────────────────────────────────────────
-- One row per player per match. Populated from get_match_player_stats_full().

CREATE TABLE IF NOT EXISTS sofascore.fact_player_match_stats (
    event_id                        bigint          NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id                       bigint          NOT NULL,
    season                          varchar(10),
    match_date                      date,
    home_team                       varchar(100),
    away_team                       varchar(100),
    team                            varchar(100),
    side                            varchar(10),    -- 'home' or 'away'
    player_name                     varchar(100),
    position                        varchar(5),
    substitute                      boolean,

    -- Time
    minutes_played                  float,

    -- Ratings
    rating                          float,
    rating_alternative              float,

    -- Goals & Attacking
    goals                           int,
    assists                         int,
    key_passes                      int,
    total_shots                     int,
    shots_on_target                 int,
    shots_off_target                int,
    shots_blocked                   int,
    big_chance_missed               int,
    total_offside                   int,

    -- Passing
    total_pass                      int,
    accurate_pass                   int,
    total_long_balls                int,
    accurate_long_balls             int,
    total_cross                     int,
    accurate_cross                  int,
    own_half_passes                 int,
    accurate_own_half_passes        int,
    opp_half_passes                 int,
    accurate_opp_half_passes        int,

    -- Carries & Progression
    carries_count                   int,
    carries_distance                float,
    progressive_carries_count       int,
    progressive_carries_distance    float,
    total_progression               float,
    best_carry_progression          float,

    -- Touches & Possession
    touches                         int,
    unsuccessful_touch              int,
    possession_lost                 int,
    dispossessed                    int,

    -- Duels
    duel_won                        int,
    duel_lost                       int,
    aerial_won                      int,
    aerial_lost                     int,
    total_contest                   int,
    won_contest                     int,
    challenge_lost                  int,

    -- Defending
    total_tackle                    int,
    won_tackle                      int,
    interception_won                int,
    total_clearance                 int,
    ball_recovery                   int,

    -- Discipline
    fouls                           int,
    was_fouled                      int,

    -- Value metrics (normalized)
    shot_value                      float,
    pass_value                      float,
    dribble_value                   float,
    defensive_value                 float,

    PRIMARY KEY (event_id, player_id)
);
