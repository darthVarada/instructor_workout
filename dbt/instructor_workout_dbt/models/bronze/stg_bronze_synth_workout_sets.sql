{{ config(
    materialized = 'view'
) }}

-- Staging dos "workout sets" sintéticos (base_full.parquet)
select
    *
from read_parquet(
    's3://instructor-workout-datas/bronze/raw/synthetic_realistic_workout_base/base_full.parquet'
);
