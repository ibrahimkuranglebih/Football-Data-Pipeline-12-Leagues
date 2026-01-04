with players_sources as (select 
    players.id,
    players.name,
    players.date_of_birth,
    players.nationality,
    players.position,
    team.name as current_team,
    players.last_updated,
    players.inserted_at
from "football_db"."raw"."players" players
left join "football_db"."raw"."teams" team
on players.current_team = team.id)

select * from players_sources