SELECT DISTINCT
    team_id,
    team_name,
    team_code,
    team_country,
    team_founded,
    team_logo,
    venue_id,
    venue_name,
    venue_city,
    venue_capacity
FROM {{ ref('src_teams') }}

