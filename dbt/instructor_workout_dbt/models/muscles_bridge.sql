{{ config(enabled=false) }}

with workouts as (
    select *
    from {{ ref('stg_bronze_workouts') }}
)

select
    distinct
    exercise_id,
    primary_muscle   as muscle
from workouts

union all

select
    distinct
    exercise_id,
    secondary_muscle as muscle
from workouts
where secondary_muscle is not null

