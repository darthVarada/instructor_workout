{{ config(enabled=false) }}

with workouts as (
    select *
    from {{ ref('stg_bronze_workouts') }}
)

select
    distinct
    exercise_id,
    exercise_name,
    primary_muscle,
    secondary_muscle
from workouts

