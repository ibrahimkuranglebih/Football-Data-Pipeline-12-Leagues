with sources as (
    select 
        id,
        date,
        area_id,
        competition_id,
        season_id,
        status,
        minute,
        injury_time,
        attendance,
        stage,
        group_name,
        home_team,
        away_team,
        score,
        goals,
        penalties,
        referees,
        inserted_at
    from {{ source('raw', 'matches') }}
)

select * from sources