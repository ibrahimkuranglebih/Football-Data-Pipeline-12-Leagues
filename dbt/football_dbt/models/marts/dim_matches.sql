{{
    config(
        materialized='table'
    )
}}

select * from 
{{ref('stg_players')}}
order by 
id