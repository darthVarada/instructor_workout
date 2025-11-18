# 📘 README – Projeto Hands-On Engenharia de Dados
## 💪 Plataforma de Treinos Personalizados Baseada em Dados

### 👥 Integrantes  
Davi Sasso  
Rodrigo Alex  
Victor Barradas  

---

## 🎯 1. Visão Geral

Este projeto integra conceitos de Engenharia de Dados, Big Data e Ciência de Dados para desenvolver um MVP de uma plataforma de treinos personalizados baseada em dados.  
A solução combina ingestão de múltiplas fontes, processamento distribuído, arquitetura em camadas e um modelo de machine learning voltado para recomendar treinos adaptados ao perfil do usuário.

---

## 🧩 2. Problema de Negócio

Aplicativos de treino geralmente oferecem recomendações genéricas, sem levar em conta dados reais como comportamento de treino, hábitos, características físicas e objetivos pessoais.  
A proposta é construir uma solução orientada por dados que gere treinos realmente personalizados, evolutivos e alinhados ao progresso diário do usuário.

---

## 🏗️ 3. Arquitetura da Solução

A arquitetura segue o padrão Medallion (Bronze, Silver, Gold), combinando elementos da Arquitetura Lambda para suportar tanto processamento em lote quanto respostas rápidas.

### 📥 Ingestão de Dados  
Coleta de informações por API de treinos, datasets complementares (ex.: Kaggle) e dados internos como NPS.

### 🥉 Bronze Layer  
Armazena os dados brutos no formato original, garantindo preservação e rastreabilidade.

### 🥈 Silver Layer  
Realiza limpeza, padronização e enriquecimento dos dados, preparando-os para análises e construção do dataset final.

### 🥇 Gold Layer  
Camada de consumo com dados refinados, utilizada por dashboards, modelos de machine learning e pelo bot inteligente.

### ⚙️ Processamento  
Pipelines implementados com PySpark para transformar e preparar as camadas.

### 🤖 Machine Learning  
Modelos construídos para recomendar treinos com base no perfil, comportamento e histórico do usuário.

### 📊 Analytics  
Dashboards desenvolvidos em Power BI ou Streamlit, permitindo acompanhar métricas de evolução e desempenho.

### 💬 Bot Agente  
Interface conversacional que utiliza dados da camada Gold e predições do modelo para sugerir treinos e interagir com o usuário de forma dinâmica.

---

## 🛠️ 4. Tecnologias Utilizadas

PySpark  
Python  
MinIO  
Docker  
Pandas  
Scikit-Learn  
Power BI ou Streamlit  
APIs REST  

---

## 📑 5. Entregáveis Atendidos

Este projeto cumpre todos os requisitos do Hands-On da disciplina, incluindo arquitetura em camadas, pipelines de ingestão e transformação, dataset preparado para ciência de dados, modelo preditivo, dashboard analítico e documentação completa para apresentação do MVP.

---

## ✅ 6. Conclusão

A solução demonstra a aplicação prática de engenharia de dados integrada com machine learning e visualização analítica.  
O MVP transforma dados brutos em recomendações inteligentes de treino, apresentando uma plataforma moderna, escalável e alinhada às melhores práticas do mercado fitness e de dados.


