-- models/gold/user_profiles.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/user_profiles/*.parquet')
)

select
    *
from src
