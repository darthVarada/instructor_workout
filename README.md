🏋️ **Instructor Workout -- Full Data & AI Pipeline + Streamlit App**
====================================================================

Este projeto integra:

-   **Pipeline de dados completo (Ingestão → Bronze → Silver → Gold)**

-   **Airflow orquestrando todos os ETLs**

-   **Dashboard analítico**

-   **Integração com API Hevy**

-   **App Streamlit com login, formulário, chat IA e dashboard**

-   **Armazenamento em S3**

-   **Geração de dados fake realistas para testes**

* * * * *

📁 **Estrutura Geral do Projeto**
=================================

`instructor_workout/
│
├── airflow/
│   ├── dags/
│   │   └── instructor_workout_pipeline.py
│   ├── scripts/
│   │   ├── ingest_synthetic_base_to_bronze.py
│   │   ├── hevy_ingest_incremental_s3.py
│   │   ├── silver_kaggle_transform.py
│   │   ├── silver_users_transform.py
│   │   └── gold_metrics.py
│   ├── .env
│   └── docker-compose.yml
│
├── src/instructor_workout/
│   ├── streamlit_app/
│   │   ├── main.py
│   │   ├── login_service.py
│   │   ├── formulario.py
│   │   ├── chat.py
│   │   ├── dashboard.py
│   │   ├── groq_service.py
│   │   └── s3_utils.py
│   │
│   ├── etl/
│   │   └── ingestion/
│   │       ├── generate_fake_test_data.py
│   │       └── (outros scripts)
│
├── README.md
└── requirements.txt`

* * * * *

⚙️ **1\. REQUISITOS DO PROJETO**
================================

### ✔ Python 3.12+

### ✔ Docker + Docker Compose

### ✔ AWS CLI configurado

### ✔ Conta S3 ativa

### ✔ GROQ API KEY

* * * * *

📦 **2\. INSTALAÇÃO DO AMBIENTE LOCAL**
=======================================

### 📌 Criar ambiente virtual

`python -m venv .venv`

### 📌 Ativar ambiente

Windows:

`.\.venv\Scripts\activate`

Mac/Linux:

`source .venv/bin/activate`

### 📌 Instalar dependências

`pip install -r requirements.txt`

* * * * *

🔐 **3\. CONFIGURAÇÃO DO STREAMLIT**
====================================

Criar:

`src/instructor_workout/streamlit_app/.streamlit/secrets.toml`

Conteúdo:

`AWS_ACCESS_KEY="SUA_KEY"
AWS_SECRET_KEY="SUA_SECRET"
AWS_REGION="sa-east-1"

GROQ_API_KEY="SUA_GROQ_KEY"
BUCKET_NAME="instructor-workout-datas"`

* * * * *

🌐 **4\. COMO RODAR O STREAMLIT**
=================================

No diretório:

`src/instructor_workout/streamlit_app`

Rodar:

`streamlit run main.py`

O app abre em:

`http://localhost:8501`

* * * * *

🧠 **5\. FUNCIONALIDADES DO STREAMLIT APP**
===========================================

### 🔒 Tela de Login

-   Usuário e senha armazenados em S3 (`users_app.parquet`)

### 📝 Formulário do Usuário

-   Dados completos para personalização do treino

-   Salvo em `s3://.../user_profiles/`

### 💬 Chat com IA (Personal Trainer)

-   Modelo GROQ LLaMA 3.1

-   Uso de contexto do perfil

-   Histórico da conversa

-   IA gera treinos personalizados

### 📊 Dashboard

-   Evolução por exercícios

-   Progressão por carga / volume

-   Resumo semanal / mensal

-   Análise comparativa com média global

-   Funciona com:

    -   Gold real (API Hevy)

    -   Test dataset artificial

* * * * *

🛠️ **6\. CONFIGURAÇÃO DO AIRFLOW**
===================================

No diretório:

`instructor_workout/airflow`

Rodar:

`docker compose up -d`

Acessar:

`http://localhost:8080
login: admin
senha: admin`

* * * * *

🗄️ **7\. VARIÁVEIS DO .env DO AIRFLOW**
========================================

Arquivo:

`airflow/.env`

Exemplo:

`AWS_ACCESS_KEY_ID=SEU_ACESSO
AWS_SECRET_ACCESS_KEY=SUA_SECRET
AWS_DEFAULT_REGION=sa-east-1
BUCKET_NAME=instructor-workout-datas`

* * * * *

🚀 **8\. PIPELINE DO AIRFLOW**
==============================

### **Bronze**

-   Captura dados brutos

-   API HEVY

-   Kaggle

-   Campos sem tratamento

### **Silver**

-   Padronização

-   Normalização

-   Tipagem

-   Limpeza

### **Gold**

-   Métricas consolidadas

-   Fatos + dimensões

-   Pronto para dashboards

* * * * *

🧪 **9\. DADOS DE TESTE (FAKE)**
================================

Script:

`src/instructor_workout/etl/ingestion/generate_fake_test_data.py`

Gera:

`test/fact_workouts_test_user.parquet`

E o app consegue carregar automaticamente.

* * * * *

👤 **10\. USUÁRIO FAKE PARA TESTE**
===================================

Usuário:

`email: testuser@example.com
senha: 123456`

Esse usuário já possui treinos fake em:

`s3://instructor-workout-datas/test/fact_workouts_test_user.parquet`

E aparece no dashboard.

* * * * *

👨‍💻 **11\. COMO ATUALIZAR E SUBIR PARA O GITHUB**
===================================================

`git add .
git commit -m "Atualização completa do projeto"
git push origin sua-branch`

* * * * *

🛠 **12\. TROUBLESHOOTING**
===========================

### ❗ S3 Access Denied

→ Verificar keys no `secrets.toml` e `.env`

### ❗ Streamlit não encontra dados

→ Checar:

`test/fact_workouts_test_user.parquet`

### ❗ Airflow não sobe

→ Tentar:

`docker compose down
docker compose up --build -d`

### ❗ Login não funciona

→ Rodar script `generate_fake_test_data.py` novamente