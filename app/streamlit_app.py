import streamlit as st
import duckdb

st.set_page_config(
    page_title="Instructor Workout – Dashboard Gold",
    layout="wide"
)

st.title("Instructor Workout – Dashboard Gold")

# -----------------------------
# Conexão com DuckDB + S3
# -----------------------------
con = duckdb.connect(database=':memory:', read_only=False)

# Carregar extensão httpfs para acessar S3
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configurações S3 (estas variáveis JÁ estão no container via environment)
import os
aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = "sa-east-1"

con.execute(f"SET s3_region='{aws_region}';")
con.execute(f"SET s3_access_key_id='{aws_key}';")
con.execute(f"SET s3_secret_access_key='{aws_secret}';")
con.execute("SET s3_session_token='';")
con.execute("SET s3_use_ssl=true;")

# -----------------------------
# Ler tabela FACT do GOLD com union_by_name
# -----------------------------

try:
    df = con.execute("""
        SELECT *
        FROM read_parquet(
            's3://instructor-workout-datas/gold/fact_workouts/fact_workouts_*.parquet',
            union_by_name=true
        )
    """).fetchdf()
    
    st.success("Dados carregados com sucesso!")
except Exception as e:
    st.error(f"Erro ao carregar dados do S3:\n\n{e}")
    st.stop()

# -----------------------------
# Mostrar preview dos dados
# -----------------------------
st.subheader("Prévia da Tabela Gold – fact_workouts")
st.dataframe(df.head(50))

# -----------------------------
# KPIs
# -----------------------------
st.subheader("Métricas Principais")

col1, col2, col3 = st.columns(3)

try:
    total_workouts = len(df)
    unique_users = df["user_id"].nunique() if "user_id" in df else 0
    avg_volume = df["volume"].mean() if "volume" in df else 0

    col1.metric("Total de Workouts", f"{total_workouts:,}")
    col2.metric("Usuários Únicos", f"{unique_users:,}")
    col3.metric("Volume Médio", f"{avg_volume:,.2f}")

except Exception as e:
    st.warning(f"Não foi possível calcular todas as métricas: {e}")

# -----------------------------
# Gráfico simples
# -----------------------------
import altair as alt
import pandas as pd

st.subheader("Volume total por dia")

if "date" in df.columns and "volume" in df.columns:
    chart_df = (
        df.groupby("date", as_index=False)["volume"].sum()
        .rename(columns={"volume": "total_volume"})
    )

    chart = (
        alt.Chart(chart_df)
        .mark_line(point=True)
        .encode(
            x="date:T",
            y="total_volume:Q",
        )
        .properties(width="container")
    )

    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Não há colunas 'date' e 'volume' para exibir o gráfico.")

# -----------------------------
# Fim
# -----------------------------
st.caption("Dashboard Gold – Powered by Streamlit + DuckDB + dbt + S3")
