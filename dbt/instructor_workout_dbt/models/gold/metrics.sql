-- models/gold/metrics.sql

with src as (
    select *
    from read_json_auto('s3://instructor-workout-datas/gold/metrics/metrics_*.json')
)

select
    *
from src
