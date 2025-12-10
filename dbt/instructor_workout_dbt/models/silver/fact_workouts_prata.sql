{{ config(enabled=false) }}

-- resto do arquivo pode ficar aí embaixo, não precisa apagar


with src as (
    select
        user_id,
        workout_id,
        workout_datetime,
        exercise_id,
        exercise_name,
        sets,
        reps,
        weight,
        volume,
        duration_minutes,
        intensity,
        dt                       as workout_date
    from {{ ref('stg_bronze_workouts') }}
)

select
    user_id,
    workout_id,
    workout_datetime,
    workout_date,
    exercise_id,
    exercise_name,
    sets,
    reps,
    weight,
    volume,
    duration_minutes,
    intensity
from src
