SELECT
    s.rank,
    s.team_id,
    s.team_name,
    t.team_logo,
    t.team_founded,
    t.venue_name,
    t.venue_city,
    t.venue_capacity,
    s.matches_played,
    s.points AS total_points,
    s.wins,
    s.draws,
    s.losses,
    s.goals_scored,
    s.goals_conceded,
    s.goals_diff AS goal_difference,
    ROUND(
        CAST(s.wins AS DOUBLE) / NULLIF(s.matches_played, 0) * 100,
        1
    ) AS win_rate,
    ROUND(
        CAST(s.points AS DOUBLE) / NULLIF(s.matches_played, 0),
        2
    ) AS points_per_match,
    s.form,
    COALESCE(p.yellow_cards, 0) AS yellow_cards,
    COALESCE(p.red_cards, 0)    AS red_cards
FROM {{ ref('stg_standings') }} s
LEFT JOIN {{ ref('stg_teams') }} t
    ON s.team_id = t.team_id
LEFT JOIN (
    SELECT
        team_id,
        SUM(COALESCE(yellow_cards, 0)) AS yellow_cards,
        SUM(COALESCE(red_cards, 0))    AS red_cards
    FROM {{ ref('stg_players') }}
    GROUP BY team_id
) p
    ON s.team_id = p.team_id
ORDER BY
    total_points DESC,
    goal_difference DESC
