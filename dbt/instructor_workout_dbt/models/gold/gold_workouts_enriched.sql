{{ config(enabled=false) }}

-- resto do arquivo pode ficar aí embaixo, não precisa apagar


with fact as (
    select *
    from {{ ref('fact_workouts') }}
),

dim_users as (
    select *
    from {{ ref('dim_users') }}
),

dim_exercises as (
    select *
    from {{ ref('dim_exercises') }}
)

select
    f.user_id,
    u.full_name,
    u.email,
    u.gender,
    u.goal,
    u.experience_level,
    f.workout_id,
    f.workout_datetime,
    f.workout_date,
    f.exercise_id,
    f.exercise_name,
    e.primary_muscle,
    e.secondary_muscle,
    e.equipment,
    f.sets,
    f.reps,
    f.weight,
    f.volume,
    f.duration_minutes,
    f.intensity
from fact f
left join dim_users     u on f.user_id = u.user_id
left join dim_exercises e on f.exercise_id = e.exercise_id
