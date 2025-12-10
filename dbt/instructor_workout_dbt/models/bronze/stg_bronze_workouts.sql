-- models/bronze/stg_bronze_workouts.sql

with bronze as (
    select *
    from read_parquet('s3://instructor-workout-datas/bronze/raw/synthetic_realistic_workout/user=*/dt=*/workouts_*.parquet')
)

select
    *
from bronze
