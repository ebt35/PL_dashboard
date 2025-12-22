SELECT DISTINCT
    player_id,
    player_name,
    player_firstname,
    player_lastname,
    player_nationality,
    team_id,
    team_name,
    league_id,
    league_season,
    games_appearances,
    games_minutes,
    games_position,
    goals_total AS goals,
    COALESCE(goals_assists, 0) AS assists,
    goals_total + COALESCE(goals_assists, 0) AS goal_involvement
FROM {{ ref('src_scorers') }}

