
# 📘 Projeto Hands-On — Plataforma Inteligente de Treinos (Engenharia de Dados)

Uma solução completa de **engenharia de dados + machine learning + análise** aplicada ao domínio de treinos físicos.

---

## 👥 Integrantes

- **Davi Sasso**  
- **Rodrigo Alex**  
- **Victor Barradas**

---

# 🎯 1. Visão Geral

Este projeto implementa uma arquitetura moderna baseada no padrão **Medallion (Bronze → Silver → Gold)**, alimentada por uma API real de treinos (Hevy API).

O MVP oferece:

- Ingestão incremental de treinos via API  
- Armazenamento distribuído com MinIO (S3-like)  
- Processamento PySpark  
- Enriquecimento de dados (Silver → Gold)  
- Dataset final pronto para modelos de recomendação  
- Integração com dashboards (Power BI / Streamlit)

---

# 🧩 2. Problema de Negócio

Aplicativos de treino geralmente oferecem recomendações genéricas e pouco personalizadas.  
Nosso objetivo é criar uma solução orientada a dados capaz de **personalizar treinos** com base no comportamento real do usuário.

---

# 🏗️ 3. Arquitetura da Solução

### 🥉 Bronze  
Armazena os dados brutos vindos da Hevy API.

### 🥈 Silver  
Limpa, padroniza e estrutura as informações.

### 🥇 Gold  
Enriquece os dados, criando variáveis analíticas como:  
- duração do treino  
- trainingDay  
- métricas para ML  

### 💾 Storage  
- Data lake no **MinIO**  
- Buckets: `bronze`, `silver`, `gold`

### ⚙️ Processamento  
- ETL/ELT com Python  
- PySpark para transformações distribuídas  
- Pipelines idempotentes

---

# 🛠️ 4. Tecnologias Utilizadas

| Tecnologia | Uso |
|-----------|-----|
| **Python 3.12** | Pipelines/ETL |
| **uv** | Ambientes e dependências |
| **PySpark 4** | Processamento distribuído |
| **MinIO** | Data Lake |
| **Hevy API** | Fonte de dados |
| **Pandas** | Silver/Gold |
| **Power BI / Streamlit** | Dashboards |
| **Docker (opcional)** | MinIO portátil |

---

# 🚀 5. Como Rodar o Projeto

---

## 🔧 5.1 Instalar Java 21

Baixe em:  
https://www.oracle.com/java/technologies/downloads/

### Configure JAVA_HOME
```powershell
setx JAVA_HOME "C:\Program Files\Java\jdk-21"
setx PATH "%JAVA_HOME%\bin;%PATH%"
```

Verifique:
```
java -version
```

---

## 📦 5.2 Instalar e rodar MinIO (Windows)

### Baixar MinIO:
```powershell
Invoke-WebRequest -Uri "https://dl.min.io/server/minio/release/windows-amd64/minio.exe" -OutFile "minio.exe"
```

### Criar pasta de dados:
```powershell
mkdir C:\minio\data
```

### Rodar MinIO:
```powershell
.\minio.exe server C:\minio\data --console-address ":9001"
```

Acessos:
- Console: http://localhost:9001  
- API S3: http://localhost:9000  

Credenciais padrão:
```
minioadmin / minioadmin
```

---

## 🪣 5.3 Criar os buckets

Crie no console web:

```
bronze
silver
gold
```

---

## 🐍 5.4 Instalar dependências com uv

### Instalar uv:
```powershell
pip install uv
```

### Instalar dependências do projeto:
```powershell
uv sync
```

---

## 🔐 5.5 Configurar variáveis de ambiente

```powershell
setx HEVY_API_KEY "SUA_API_KEY_AQUI"

setx S3_ENDPOINT_URL "http://localhost:9000"
setx MINIO_ACCESS_KEY "minioadmin"
setx MINIO_SECRET_KEY "minioadmin"

setx MINIO_BRONZE_BUCKET "bronze"
setx MINIO_SILVER_BUCKET "silver"
setx MINIO_GOLD_BUCKET   "gold"
```

---

# 🥉 6. Rodar Ingestão Incremental (Bronze)

```powershell
uv run python src/instructor_workout/etl/ingestion/hevy_ingest_incremental_minio.py
```

O pipeline irá:

- Usar a `HEVY_API_KEY`
- Baixar apenas treinos novos
- Registrar no MinIO em:
  ```
  s3://bronze/hevy/workouts/<timestamp>.json
  ```
- Atualizar o arquivo `last_sync.json`

---

# 🥈 7. Upload da camada Silver

Dataset sintético disponível em:

```
data/silver/synthetic_realistic_workout.csv
```

Enviar para MinIO:

```powershell
uv run python src/instructor_workout/etl/processing/upload_silver_to_minio.py
```

---

# 🥇 8. Silver → Gold (Próxima etapa)

Será aplicada transformação:

- conversão de timezone  
- cálculo de duração (end - start)  
- criação de trainingDay  
- flatten de exercises  
- exportação para Parquet  

---

# 📂 9. Estrutura do Projeto

```
instructor_workout/
│
├── data/
│   ├── silver/
│   └── bronze/
│
├── src/
│   └── instructor_workout/
│       ├── etl/
│       │   ├── ingestion/
│       │   │   └── hevy_ingest_incremental_minio.py
│       │   ├── processing/
│       │   │   └── upload_silver_to_minio.py
│       │   ├── spark_session.py
│       │   └── schema.py
│       └── observability/
│
├── tests/
│
├── README.md
└── pyproject.toml
```

---

# 🤝 10. Como contribuir

1. Criar branch:
```
git checkout -b feat/seu_nome
```

2. Commits pequenos e descritivos  
3. Abrir PR para main  
4. Seguir princípios:
   - Modulação  
   - Logs claros  
   - Idempotência  
   - Respeitar o padrão Bronze → Silver → Gold  

---

# 🏁 11. Conclusão

O projeto demonstra como unir **engenharia de dados, bronze/silver/gold, processamento distribuído, API real e ML** para criar uma plataforma robusta e moderna de treinos personalizados.

O ambiente está pronto para que qualquer colega rode tudo em minutos.
