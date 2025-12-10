-- models/silver/silver_workouts.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/silver/synthetic_realistic_workout/merged.parquet')
)

select
    *
from src
