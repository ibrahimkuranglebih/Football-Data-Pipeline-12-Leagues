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
    last_updated,
    inserted_at
from {{source('raw', 'competitions')}}