
  create view "football_db"."analytics"."stg_competitions__dbt_tmp"
    
    
  as (
    with competitions_sources as (select 
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
    (raw_payload -> 'currentSeason' ->> 'currentMatchday'):: INT as current_matchday,
    (raw_payload -> 'numberOfAvailableSeasons'):: INT as number_available_seasons,
    last_updated,
    inserted_at
from "football_db"."raw"."competitions")

select * from competitions_sources
  );