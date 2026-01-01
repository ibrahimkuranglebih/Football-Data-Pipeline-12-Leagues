select 
    id,
    name,
    first_name,
    last_name,
    date_of_birth,
    nationality,
    position,
    current_team,
    shirt_number,
    contract,
    last_updated,
    inserted_at
from {{ source('raw', 'players') }}