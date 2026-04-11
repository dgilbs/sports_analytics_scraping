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

-- ── fact_shots ────────────────────────────────────────────────────────────────
-- One row per shot. Populated from get_shotmap_table() in shot_script.py.
-- No stable shot ID from the API, so rows are replaced wholesale per event.

CREATE TABLE IF NOT EXISTS sofascore.fact_shots (
    id                  bigserial       PRIMARY KEY,
    event_id            bigint          NOT NULL REFERENCES sofascore.dim_matches(event_id),
    season              varchar(10),
    match_date          date,
    home_team           varchar(100),
    away_team           varchar(100),
    team                varchar(100),
    side                varchar(10),
    player_id           bigint,
    player_name         varchar(100),
    position            varchar(10),

    -- Shot details
    shot_type           varchar(20),    -- goal, save, miss, block
    situation           varchar(30),    -- open-play, corner, free-kick, counter
    body_part           varchar(20),    -- right-foot, left-foot, head
    goal_mouth_location varchar(30),
    time                int,
    added_time          int,
    time_seconds        int,

    -- Pitch coordinates
    player_x            float,
    player_y            float,

    -- Goal mouth coordinates
    goal_mouth_x        float,
    goal_mouth_y        float,
    goal_mouth_z        float,

    -- Trajectory
    draw_start_x        float,
    draw_start_y        float,
    draw_end_x          float,
    draw_end_y          float,

    -- Derived flags
    is_goal             boolean,
    is_on_target        boolean,
    is_blocked          boolean
);

-- ── fact_pass_maps ────────────────────────────────────────────────────────────
-- One row per player per match. Populated from passing_map_script.py.

CREATE TABLE IF NOT EXISTS sofascore.fact_pass_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    passes_total            int,
    passes_accurate         int,
    passes_inaccurate       int,
    pass_accuracy           float,
    avg_pass_length         float,
    pct_forward             float,
    pct_backward            float,
    pct_lateral             float,
    acc_pct_forward         float,
    acc_pct_backward        float,
    acc_pct_lateral         float,
    origin_def_third        float,
    origin_mid_third        float,
    origin_att_third        float,
    origin_left_wing        float,
    origin_central          float,
    origin_right_wing       float,
    dest_def_third          float,
    dest_mid_third          float,
    dest_att_third          float,
    dest_left_wing          float,
    dest_central            float,
    dest_right_wing         float,
    acc_dest_def_third      float,
    acc_dest_mid_third      float,
    acc_dest_att_third      float,
    acc_dest_left_wing      float,
    acc_dest_central        float,
    acc_dest_right_wing     float,
    progressive_passes      int,
    progressive_pass_pct    float,
    PRIMARY KEY (event_id, player_id)
);

-- ── fact_drib_maps ────────────────────────────────────────────────────────────
-- One row per player per match. Populated from dribbling_map_script.py.

CREATE TABLE IF NOT EXISTS sofascore.fact_drib_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    dribbles_won            int,
    dribbles_lost           int,
    dribbles_total          int,
    dribble_success         float,
    carry_segments          int,
    drib_def_third          float,
    drib_mid_third          float,
    drib_att_third          float,
    drib_left_wing          float,
    drib_central            float,
    drib_right_wing         float,
    carry_def_third         float,
    carry_mid_third         float,
    carry_att_third         float,
    carry_left_wing         float,
    carry_central           float,
    carry_right_wing        float,
    drib_won_def_third      float,
    drib_won_mid_third      float,
    drib_won_att_third      float,
    PRIMARY KEY (event_id, player_id)
);

-- ── fact_def_maps ─────────────────────────────────────────────────────────────
-- One row per player per match. Populated from defensive_map_script.py.

CREATE TABLE IF NOT EXISTS sofascore.fact_def_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    tackle_won              int,
    missed_tackle           int,
    interception            int,
    clearance               int,
    block                   int,
    recovery                int,
    total_def_actions       int,
    tackle_success          float,
    pct_def_third           float,
    pct_mid_third           float,
    pct_att_third           float,
    pct_left_wing           float,
    pct_central             float,
    pct_right_wing          float,
    tackle_def_third        float,
    tackle_mid_third        float,
    tackle_att_third        float,
    intercept_def_third     float,
    intercept_mid_third     float,
    intercept_att_third     float,
    recovery_def_third      float,
    recovery_mid_third      float,
    recovery_att_third      float,
    PRIMARY KEY (event_id, player_id)
);
