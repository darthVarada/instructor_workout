{{ config(enabled=false) }}

with workouts as (
    select *
    from {{ ref('stg_bronze_workouts') }}
)

select
    distinct
    exercise_id,
    instructions_text
from workouts
where instructions_text is not null

