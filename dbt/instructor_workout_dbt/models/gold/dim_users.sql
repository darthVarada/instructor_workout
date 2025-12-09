{{ config(materialized='table') }}

with base as (

    select
        uf.user_id,
        max(uf.data_registro) as data_registro,
        any_value(uf.nome) as nome,
        any_value(uf.data_nascimento) as data_nascimento,
        any_value(uf.sexo) as sexo,
        any_value(uf.peso) as peso,
        any_value(uf.altura) as altura,
        any_value(uf.percentual_gordura) as percentual_gordura,
        any_value(uf.objetivo) as objetivo,
        any_value(uf.nivel_treinamento) as nivel_treinamento,
        any_value(uf.restricoes_fisicas) as restricoes_fisicas,
        any_value(uf.frequencia_semanal) as frequencia_semanal,
        any_value(uf.horas_sono) as horas_sono,
        any_value(uf.nutricional_score) as nutricional_score
    from {{ ref('stg_bronze_user_form') }} uf
    group by uf.user_id

)

select *
from base;
