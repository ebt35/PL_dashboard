SELECT DISTINCT
    rank,
    team_id,
    team_name,
    team_logo,
    points,
    goals_diff,
    all_played AS matches_played,
    all_win  AS wins,
    all_draw AS draws,
    all_lose AS losses,
    all_goals_for     AS goals_scored,
    all_goals_against AS goals_conceded,
    home_played,
    home_win,
    home_draw,
    home_lose,
    away_played,
    away_win,
    away_draw,
    away_lose,
    form
FROM {{ ref('src_standings') }}

