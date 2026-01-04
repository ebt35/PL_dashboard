WITH source AS (
    SELECT
        player_id,
        player_name,
        player_firstname,
        player_lastname,
        player_age,
        player_nationality,
        team_id,
        team_name,
        games_appearances,
        games_minutes,
        games_position,
        goals       AS goals,
        assists     AS assists,
        COALESCE(goals, 0) + COALESCE(assists, 0) AS goal_involvement,
        COALESCE(yellow_cards, 0) AS yellow_cards,
        COALESCE(red_cards, 0)    AS red_cards
    FROM {{ source('raw', 'players') }}
)
SELECT
    *
FROM source
WHERE games_appearances IS NOT NULL
