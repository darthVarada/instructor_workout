{{ config(enabled=false) }}


{{ config(
    materialized = 'table'
) }}

with src as (

    select
        cast(user_id as integer)                                   as user_id,
        cast(nome as varchar)                                      as user_name,
        cast(data_nascimento as date)                              as birth_date,
        cast(objetivo as varchar)                                  as goal,
        cast(nivel as varchar)                                     as level,
        cast(restricoes as varchar)                                as restrictions,
        cast(frequencia as varchar)                                as workout_frequency_raw,
        cast(peso as double)                                       as weight_kg,
        cast(altura as double)                                     as height_cm,
        cast(sono as integer)                                      as sleep_hours,
        cast(score as double)                                      as health_score,
        cast(idade as integer)                                     as age,
        cast(updated_at as timestamp)                              as updated_at
    from {{ ref('stg_bronze_user_profile') }}

)

select *
from src;
