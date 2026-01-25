{{ 
  config(
    materialized='incremental',
    unique_key='id'
  ) 
}}

with teams_sources as (
    select 
        id,
        comp_id,
        name as team_name,
        code as team_code,
        flag,
        team_emblem,
        founded,
        venue,
        website,
        club_colors,
        address,
        last_updated,
        inserted_at
    from {{ source('raw', 'teams') }}

    {% if is_incremental() %}
    where last_updated > (
        select coalesce(max(last_updated), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)

select * from teams_sources
