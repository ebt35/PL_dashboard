SELECT DISTINCT
    team_id,
    team_name,
    team_logo,
    matches_played,
    points AS total_points,
    goals_scored,
    goals_conceded,
    goals_diff AS goal_difference,
    ROUND(CAST(all_win AS DOUBLE) / NULLIF(matches_played, 0) * 100, 2) AS win_rate,
    ROUND(CAST(points AS DOUBLE) / NULLIF(matches_played, 0), 2) AS points_per_match
FROM {{ ref('stg_standings') }}
ORDER BY total_points DESC, goal_difference DESC

