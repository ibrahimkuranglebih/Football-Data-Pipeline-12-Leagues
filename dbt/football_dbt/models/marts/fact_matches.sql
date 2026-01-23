{{
    config(
        materialized='table'
    )
}}

select * from 
{{ref('stg_matches')}}
order by 
id