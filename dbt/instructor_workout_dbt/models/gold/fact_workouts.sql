{{ config(materialized='table') }}

select
    w.workout_id,
    w.user_id,
    w.date,
    w.muscle_group,
    w.total_sets,
    w.total_reps,
    w.duration_min
from {{ ref('stg_bronze_workouts') }} w;
