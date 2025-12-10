-- models/bronze/stg_bronze_user_form.sql

with bronze as (
    select *
    from read_parquet('s3://instructor-workout-datas/bronze/raw/users_form_log_birthdate/*.parquet')
)

select
    *
from bronze
