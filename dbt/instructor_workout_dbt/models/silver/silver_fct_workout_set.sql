{{ config(
    materialized = 'table'
) }}

with src as (

    select
        -- surrogate key da linha de série
        {{ dbt_utils.generate_surrogate_key(
            ['user_id', 'workout_session_id', 'set_id', 'exercise_name_kaggle']
        ) }}                                                       as workout_set_sk,

        cast(user_id as integer)                                   as user_id,
        cast(workout_session_id as integer)                        as workout_session_id,

        cast(dt as timestamp)                                      as workout_datetime,
        cast(set_id as integer)                                    as set_id,
        cast(set_order as integer)                                 as set_order,
        cast(exercise_order as integer)                            as exercise_order,
        cast(superset_id as integer)                               as superset_id,

        cast(title as varchar)                                     as workout_title,
        cast(exercise_title_heavy as varchar)                      as exercise_title_heavy,
        cast(exercise_name_kaggle as varchar)                      as exercise_name_kaggle,
        cast(set_type as varchar)                                  as set_type,

        cast(weight_kg as double)                                  as weight_kg,
        cast(reps as integer)                                      as reps,
        cast(distance_km as double)                                as distance_km,
        cast(duration_seconds as integer)                          as duration_seconds,
        cast(rpe as integer)                                       as rpe

    from {{ ref('stg_bronze_synth_workout_sets') }}

),

-- mapeia exercise_id a partir da dimensão de exercício
exercise_join as (

    select
        s.*,
        d.exercise_id
    from src s
    left join {{ ref('silver_dim_exercise') }} d
        on lower(s.exercise_name_kaggle) = lower(d.exercise_name)

)

select *
from exercise_join;
