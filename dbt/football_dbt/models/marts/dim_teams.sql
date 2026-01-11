{{
    config(
        materialized='table'
    )
}}

select * from 
{{ref('stg_teams')}}
order by 
id