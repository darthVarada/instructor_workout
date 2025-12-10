-- models/silver/dim_exercises.sql

with src as (
    select *
    from {{ ref('silver_exercises') }}
)

select
    *
from src
