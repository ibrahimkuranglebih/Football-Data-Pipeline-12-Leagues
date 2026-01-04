
  create view "football_db"."analytics"."stg_teams__dbt_tmp"
    
    
  as (
    with teams_sources as (select 
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
from "football_db"."raw"."teams")

select * from teams_sources
  );