{{ 
  config(
    materialized='incremental',
    unique_key='id'
  ) 
}}

with competitions_sources as (
    select 
        id,
        name,
        code,
        type,
        plan,
        area_id,
        area_name,
        season_id,
        season_start,
        season_end,
        emblem,
        (raw_payload -> 'currentSeason' ->> 'currentMatchday')::int
            as current_matchday,
        (raw_payload ->> 'numberOfAvailableSeasons')::int
            as number_available_seasons,
        last_updated,
        inserted_at
    from {{ source('raw', 'competitions') }}

    {% if is_incremental() %}
    where last_updated > (
        select coalesce(max(last_updated), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)

select * from competitions_sources
