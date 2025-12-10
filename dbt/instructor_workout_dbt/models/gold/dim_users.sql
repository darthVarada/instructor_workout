-- models/gold/dim_users.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/dim_users/dim_users_*.parquet')
)

select
    *
from src
