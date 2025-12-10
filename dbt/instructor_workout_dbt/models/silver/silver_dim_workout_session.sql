{{ config(enabled=false) }}


{{ config(
    materialized = 'table'
) }}

with src as (

    select
        cast(id as integer)                                        as workout_session_id,
        cast(user_id as integer)                                   as user_id,
        cast(routine_id as integer)                                as routine_id,
        cast(title as varchar)                                     as title,
        cast(duracao as integer)                                   as duration_minutes,
        cast(created_at as timestamp)                              as created_at,
        cast(updated_at as timestamp)                              as updated_at
    from {{ ref('stg_bronze_workout_sessions') }}

)

select *
from src;
