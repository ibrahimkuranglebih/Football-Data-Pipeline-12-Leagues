{{
    config(
        materialized='table'
    )
}}

select 
    *
from
{{ref('stg_competitions')}}
order by 
id