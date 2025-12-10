-- models/gold/fact_workouts.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/fact_workouts/fact_workouts_*.parquet')
)

select
    *
from src
