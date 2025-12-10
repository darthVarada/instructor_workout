with bronze as (
    select *
    from read_parquet('s3://instructor-workout-datas/bronze/raw/kaggle/gym_exercises_*.parquet')
)

select
    *
from bronze
