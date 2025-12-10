{{ config(enabled=false) }}


with fact_workouts as (
    select *
    from {{ ref('fact_workouts') }}
),

exercises as (
    select *
    from {{ ref('exercises_dim') }}
),

muscles as (
    select *
    from {{ ref('muscles_bridge') }}
),

instructions as (
    select *
    from {{ ref('instructions_bridge') }}
)

select
    f.user_id,
    f.workout_date,
    f.exercise_id,
    e.exercise_name,
    m.muscle,
    i.instructions_text,
    f.reps,
    f.sets,
    f.weight
from fact_workouts f
left join exercises    e on f.exercise_id = e.exercise_id
left join muscles      m on f.exercise_id = m.exercise_id
left join instructions i on f.exercise_id = i.exercise_id

