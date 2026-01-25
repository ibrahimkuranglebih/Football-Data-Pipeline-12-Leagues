{{ 
  config(
    materialized='incremental',
    unique_key='id'
  ) 
}}

with players_sources as (
    select 
        players.id,
        players.name,
        players.date_of_birth,
        players.nationality,
        players.position,
        team.name as current_team,
        players.last_updated,
        players.inserted_at
    from {{ source('raw', 'players') }} players
    left join {{ source('raw', 'teams') }} team
        on players.current_team = team.id

    {% if is_incremental() %}
    where players.last_updated > (
        select coalesce(max(last_updated), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)

select * from players_sources
