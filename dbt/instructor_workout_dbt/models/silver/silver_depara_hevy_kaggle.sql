-- models/silver/silver_depara_hevy_kaggle.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/silver/depara/depara_heavy_kaggle.parquet')
)

select
    *
from src
