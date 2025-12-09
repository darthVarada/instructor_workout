{{ config(materialized='view') }}

-- Lê diretamente os arquivos Parquet da BRONZE no S3
select
    user_id,
    data_registro,
    nome,
    data_nascimento,
    sexo,
    peso,
    altura,
    percentual_gordura,
    objetivo,
    nivel_treinamento,
    restricoes_fisicas,
    frequencia_semanal,
    horas_sono,
    nutricional_score
from read_parquet('s3://instructor-workout-datas/bronze/user_form/*.parquet');
