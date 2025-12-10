-- models/gold/app_users.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/app_users/users_app.parquet')
)

select
    *
from src
