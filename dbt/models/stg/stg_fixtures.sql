SELECT DISTINCT
    fixture_id,
    fixture_date,
    fixture_timestamp,
    league_id,
    league_name,
    league_season,
    league_round,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_goals,
    away_goals,
    score_fulltime_home,
    score_fulltime_away,
    CASE 
        WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_completed
FROM {{ ref('src_fixtures') }}

