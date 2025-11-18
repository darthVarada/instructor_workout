📘 README — Projeto Hands-On Engenharia de Dados
Plataforma de Treinos Personalizados Baseada em Dados
Integrantes:

Davi Sasso

Rodrigo Alex

Victor Barradas

🎯 1. Visão Geral

Este projeto integra conceitos de Engenharia de Dados, Big Data e Ciência de Dados para construir um MVP funcional de uma plataforma que recomenda treinos personalizados com base em múltiplas fontes de dados.

O objetivo é unir:

Ingestão e processamento escalável;

Arquitetura moderna (Medallion + Lambda);

Machine Learning aplicado ao comportamento de treino;

Camada analítica para insights;

Um bot agente capaz de responder e recomendar treinos com base em dados reais.

O projeto segue as diretrizes oficiais da disciplina, incluindo arquitetura, storytelling, governança e entrega de MVP.

🧩 2. Problema de Negócio

Apps de treino geralmente não utilizam dados reais — sono, nutrição, histórico de cargas, frequência semanal — para personalizar treinos.
Isso resulta em recomendações genéricas e pouco eficientes.

O projeto propõe:

Um pipeline completo que integra dados de treino (via API), dados pessoais, métricas de saúde e comportamento;

Um modelo de machine learning capaz de recomendar treinos adequados ao objetivo e nível do usuário;

Uma estrutura de dados que permite evolução, histórico e ajustes contínuos.

🏗️ 3. Arquitetura da Solução

A arquitetura utiliza uma abordagem moderna baseada em:

📥 Ingestão de Dados

API externa (treinos)

Bases adicionais (ex.: Kaggle)

Dados internos (NPS)

🥉 Bronze Layer – Dados Brutos

Conjunto original, não transformado

Armazenamento distribuído (MinIO)

🥈 Silver Layer – Dados Tratados

Limpeza

Padronização

Enriquecimento inicial

Preparação para camadas superiores

🥇 Gold Layer – Serving Layer

Dados refinados e prontos para consumo

Base final usada por ML, bot e dashboards

⚙️ Processamento

PySpark

Pipelines distribuídos

Transformações batch e em tempo quase real

🧠 Machine Learning

Construção de features a partir da camada Gold

Modelos preditivos para recomendação de treino

Ajuste automático baseado em padrões do usuário

📊 Analytics

Dashboard em Power BI / Streamlit

Indicadores sobre treinos, evolução e engajamento

🤖 Bot Agente

Consome a camada Gold

Acompanha e recomenda treinos personalizados

Serve como interface conversacional com o usuário

🔧 4. Tecnologias Utilizadas

PySpark

Python

MinIO (S3-like)

Docker

Pandas / Scikit-Learn

Power BI / Streamlit

API REST

🧾 5. Entregáveis Atendidos

Conforme o documento da disciplina, o projeto contempla:

Arquitetura Lambda e Medallion implementada

Pipelines de ingestão e transformação

Modelo preditivo funcional

Dashboard analítico com insights

Documentação e storytelling do processo

MVP operacional para apresentação final


MACK_HANDS_00

✅ 6. Conclusão

Este projeto demonstra uma solução completa de engenharia + ciência de dados aplicada ao contexto fitness, combinando:

Processamento escalável

Organização moderna de dados

Inteligência preditiva

Visualização orientada ao negócio

Aplicação prática de arquitetura de dados usada no mercado
