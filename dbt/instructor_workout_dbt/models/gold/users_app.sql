-- models/gold/users_app.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/gold/users_app/users_app.parquet')
)

select
    *
from src
