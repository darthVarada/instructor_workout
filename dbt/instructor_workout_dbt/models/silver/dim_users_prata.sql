-- models/silver/dim_users_prata.sql

with src as (
    select *
    from read_parquet('s3://instructor-workout-datas/silver/users/users_silver_*.parquet')
),

dedup as (
    select
        user_id,
        objetivo,
        data_nascimento,
        data_registro,
        percentual_gordura,
        row_number() over (
            partition by user_id
            order by data_registro desc
        ) as rn
    from src
)

select
    user_id,
    objetivo,
    data_nascimento,
    data_registro,
    percentual_gordura
from dedup
where rn = 1
