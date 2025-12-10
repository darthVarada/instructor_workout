{{ config(enabled=false) }}


{{ config(
    materialized = 'table'
) }}

with src as (

    select
        -- cria um id artificial de exercício se não existir
        {{ dbt_utils.generate_surrogate_key(['exercise_name']) }}  as exercise_id,
        cast("Exercise Name" as varchar)                           as exercise_name,
        cast(equipment as varchar)                                 as equipment,
        cast("Main_muscle" as varchar)                             as main_muscle,
        cast("Synergist_Muscles" as varchar)                       as synergist_muscles,
        cast("Exercise_Type" as varchar)                           as exercise_type,
        cast("Mechanics" as varchar)                               as mechanics,
        cast("Force" as varchar)                                   as force_type,
        cast("Level" as varchar)                                   as difficulty_level,
        cast("Sport" as varchar)                                   as sport_context,
        cast("Utility" as varchar)                                 as utility,
        cast("Commentary" as varchar)                              as commentary
    from {{ ref('stg_bronze_kaggle_exercises') }}

)

select *
from src;
