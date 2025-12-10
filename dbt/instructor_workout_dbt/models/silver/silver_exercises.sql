-- models/silver/silver_exercises.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/silver/kaggle/gym_exercises_silver_*.parquet')
)

select
    row_number() over (order by "Exercise Name") as exercise_id,
    "Exercise Name"       as exercise_name,
    "Main_muscle"         as primary_muscle,
    "Target_Muscles"      as target_muscles,
    "Synergist_Muscles"   as synergist_muscles,
    "Execution"           as execution
from src
