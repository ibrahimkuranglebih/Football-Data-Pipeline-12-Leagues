{{ 
  config(
    materialized='incremental',
    unique_key='id'
  ) 
}}

with matches_sources as (
    select
        id,
        date,
        area_id,
        competition_id,
        season_id,
        status,
        duration_status,
        stage,
        group_name,
        home_team::jsonb ->> 'name' as home_team,
        away_team::jsonb ->> 'name' as away_team,
        home_team::jsonb ->> 'tla'  as home_team_code,
        away_team::jsonb ->> 'tla'  as away_team_code,
        score::jsonb ->> 'winner' as result,
        (score::jsonb -> 'fullTime' ->> 'home')::int
            as home_final_score,
        (score::jsonb -> 'fullTime' ->> 'away')::int
            as away_final_score,
        last_updated,
        inserted_at
    from {{ source('raw', 'matches') }}

    {% if is_incremental() %}
    where last_updated > (
        select coalesce(max(last_updated), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)

select * from matches_sources
