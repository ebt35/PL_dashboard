SELECT DISTINCT
    player_id,
    player_name,
    player_nationality,
    team_id,
    team_name,
    games_appearances,
    games_minutes,
    games_position,
    goals,
    assists,
    goal_involvement,
    ROUND(CAST(goals AS DOUBLE) / NULLIF(games_appearances, 0), 2) AS goals_per_game,
    ROUND(CAST(assists AS DOUBLE) / NULLIF(games_appearances, 0), 2) AS assists_per_game,
    ROUND(CAST(goal_involvement AS DOUBLE) / NULLIF(games_appearances, 0), 2) AS goal_involvement_per_game
FROM {{ ref('stg_scorers') }}
ORDER BY goal_involvement DESC, goals DESC

