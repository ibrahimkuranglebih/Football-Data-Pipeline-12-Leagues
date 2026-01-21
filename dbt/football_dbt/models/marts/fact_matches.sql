{{ config(materialized='table') }}

SELECT
    id,
    date,
    competition_id,
    season_id,
    duration_status,
    CASE
        WHEN result = 'HOME_TEAM' THEN home_team
        WHEN result = 'AWAY_TEAM' THEN away_team
        WHEN result = 'DRAW' THEN 'DRAW'
        ELSE NULL
    END AS winner,
    home_final_score,
    away_final_score
FROM {{ ref('dim_matches') }}
WHERE status = 'FINISHED'
