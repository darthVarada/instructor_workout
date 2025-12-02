# 🏋️‍♂️ Instructor Workout — Plataforma Inteligente de Treinos com IA

Uma plataforma completa de **engenharia de dados + personal trainer com IA + recomendação de treinos**, utilizando **MinIO, arquitetura Bronze/Silver/Gold, Streamlit e Groq LLM**.

---

## 👥 Integrantes

- **Davi Sasso**  
- **Rodrigo Alex**  
- **Victor Barradas**

---

# 🎯 1. Visão Geral Atualizada

Este projeto implementa:

✅ Arquitetura **Medallion (Bronze → Silver → Gold)**  
✅ Ingestão via **API (Kaggle / Hevy / CSVs)**  
✅ Armazenamento no **MinIO (S3 local)**  
✅ Processamento com **Python + Pandas**  
✅ Camada **Gold analítica**  
✅ **Aplicação Streamlit** com:
- Cadastro de perfil
- Atualização de perfil
- Exclusão de perfil (cookies)
- Chat com **Personal Trainer IA (Groq)**
- Salvamento de treinos recomendados
- Página de **Treinos Recomendados em tabela**

---

# 🧩 2. Problema de Negócio

Aplicativos de treino oferecem recomendações genéricas.

Nosso objetivo é:

✅ Criar um **personal trainer virtual**  
✅ Integrar **dados reais + IA**  
✅ Gerar **treinos personalizados** com base no perfil do usuário  
✅ Persistência via **cookies (sem banco por enquanto)**

---

# 🏗️ 3. Arquitetura da Solução

## 🥉 Bronze

- Dados brutos da Kaggle / Hevy
- JSON / CSV
- Armazenados no MinIO

```
s3://bronze/
```

---

## 🥈 Silver

- Dados tratados
- Limpeza de campos
- Normalização
- CSV e Parquet

```
s3://silver/
```

---

## 🥇 Gold

- Tabelas analíticas:

| Tabela | Descrição |
|--------|-----------|
| exercises_dim | Exercícios principais |
| muscles_bridge | Exercício x músculos |
| instructions_bridge | Execução e preparação dos exercícios |

```
s3://gold/
```

---

## 🤖 IA

- Modelo Groq:  
```
llama-3.1-8b-instant
```

- Prompt estruturado como:

✅ Grupo 1 / 2 / 3  
✅ Exercício  
✅ Séries  
✅ Repetições  
✅ Dicas adicionais

---

## 🖥️ Interface

Criada em **Streamlit** com 3 telas principais:

✅ Chat  
✅ Treinos Recomendados (tabela)  
✅ Atualizar Perfil  

Cadastro inicial obrigatório.

---

# 🛠️ 4. Tecnologias Utilizadas

| Tecnologia | Uso |
|-----------|-----|
| Python 3.12 | Backend |
| uv | Gerenciador de ambiente |
| Pandas | Processamento Silver/Gold |
| MinIO | Data Lake |
| Groq API | Personal Trainer IA |
| Streamlit | Interface |
| Cookies Manager | Persistência do usuário |

---

# 🚀 5. Como Rodar o Projeto

---

## 📦 5.1 Instalar dependências

```powershell
pip install uv
uv sync
```

---

## 🪣 5.2 Rodar o MinIO (Windows)

No seu projeto já existe:

```
tools/minio.exe
```

Rode assim:

```powershell
cd tools
.\\minio.exe server C:\\minio\\data --console-address ":9001"
```

Acessos:
- Console: http://localhost:9001  
- API S3: http://localhost:9000

Login padrão:
```
minioadmin / minioadmin
```

---

## 🪣 5.3 Criar Buckets

Crie no painel do MinIO:

```
bronze
silver
gold
```

---

## 🔐 5.4 Variáveis de Ambiente

```powershell
setx GROQ_API_KEY "SUA_CHAVE_GROQ"

setx S3_ENDPOINT_URL "http://localhost:9000"
setx MINIO_ACCESS_KEY "minioadmin"
setx MINIO_SECRET_KEY "minioadmin"

setx MINIO_BRONZE_BUCKET "bronze"
setx MINIO_SILVER_BUCKET "silver"
setx MINIO_GOLD_BUCKET   "gold"
```

---

# 🥉 6. Ingestão Bronze

```powershell
uv run python src/instructor_workout/etl/ingestion/kaggle_ingest_minio.py
```

---

# 🥈 7. Bronze → Silver

```powershell
uv run python src/instructor_workout/etl/processing/gym_exercises_bronze_to_silver.py
```

---

# 🥇 8. Silver → Gold

```powershell
uv run python src/instructor_workout/etl/processing/gym_exercises_gold_full.py
```

---

# 🖥️ 9. Rodar o App Streamlit

```powershell
uv run streamlit run src/instructor_workout/streamlit_app/main.py
```

Acesse:

```
http://localhost:8501
```

---

# 🧑‍💻 10. Funcionalidades do App

✅ Cadastro inicial obrigatório  
✅ Persistência via cookies  
✅ Atualizar dados do usuário  
✅ Excluir perfil (limpa cookies)  
✅ Chat estilo ChatGPT  
✅ Treinos personalizados  
✅ Botão **Salvar treino recomendado**  
✅ Página **Treinos Recomendados em Tabela**

---

# 📂 11. Estrutura Atual do Projeto

```
instructor_workout/
├── data/
│   ├── bronze/
│   └── silver/
│
├── src/instructor_workout/
│   ├── etl/
│   │   ├── ingestion/
│   │   │   ├── kaggle_ingest_minio.py
│   │   │   └── minio_client.py
│   │   ├── processing/
│   │   │   ├── gym_exercises_bronze_to_silver.py
│   │   │   ├── gym_exercises_gold_full.py
│   │   │   └── upload_silver_to_minio.py
│   ├── streamlit_app/
│   │   └── main.py
│   └── observability/
│
├── tools/
│   └── minio.exe
│
├── pyproject.toml
└── README.md
```

---

# 🏁 12. Status Atual do Projeto

✅ Pipeline Bronze rodando  
✅ Silver funcionando  
✅ Gold consolidado  
✅ Streamlit funcionando  
✅ IA conectada ao Groq  
✅ Salvamento de treinos funcionando  
✅ Persistência via cookies funcionando

---

# 🤝 13. Como Contribuir

1. Criar branch:
```bash
git checkout -b feat/seu_nome
```

2. Commits pequenos  
3. Abrir PR  
4. Seguir padrão Bronze → Silver → Gold

---

# 🏆 14. Conclusão

Este projeto hoje já entrega:

✅ Engenharia de Dados completa  
✅ Personal Trainer com IA  
✅ Projeto pronto para escalar para:
- Banco de dados
- Autenticação
- ML real
- App Mobile

🚀 Projeto já está em nível de **portfólio avançado em Data + AI**.