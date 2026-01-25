{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
    *
from {{ ref('stg_teams') }}

{% if is_incremental() %}
where last_updated > (
    select coalesce(max(last_updated), '1900-01-01')
    from {{ this }}
)
{% endif %}
