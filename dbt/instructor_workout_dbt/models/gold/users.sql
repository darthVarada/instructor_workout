-- models/gold/users.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/users/users_app.parquet')
)

select
    *
from src
