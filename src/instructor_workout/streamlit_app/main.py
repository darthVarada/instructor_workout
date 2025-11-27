# src/instructor_workout/streamlit_app/main.py
import os

import pandas as pd
import streamlit as st
#run
#uv run streamlit run src/instructor_workout/streamlit_app/main.py

@st.cache_data
def load_silver():
    """
    Tenta ler a Silver do MinIO.
    Se der erro (ex.: MinIO desligado), cai pro arquivo local.
    """
    bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    key = "hevy/workouts/sets/synthetic_realistic_workout.csv"
    s3_url = f"s3://{bucket}/{key}"

    storage_options = {
        "client_kwargs": {"endpoint_url": endpoint},
        "key": access_key,
        "secret": secret_key,
    }

    try:
        df = pd.read_csv(s3_url, storage_options=storage_options)
        source = "MinIO"
    except Exception:
        local_path = "data/silver/synthetic_realistic_workout.csv"
        df = pd.read_csv(local_path)
        source = "arquivo local"

    return df, source


def main():
    st.set_page_config(
        page_title="Instructor Workout – Visão geral",
        layout="wide",
    )

    st.title("🏋️ Instructor Workout – Painel simples")
    st.caption("Primeira versão da interface, explorando a camada Silver.")
    # bloco a ser modificado ⬇️
    df, source = load_silver()
    st.info(f"Dados carregados de: **{source}** ({len(df)} linhas)")

    # Ajuste nomes de colunas conforme o seu CSV real
    cols = df.columns.tolist()

    # Painel lateral de filtros
    st.sidebar.header("Filtros")

    # Exemplo: se existir coluna de exercício
    exercise_col = None
    for c in cols:
        if "exercise" in c.lower() or "exercicio" in c.lower():
            exercise_col = c
            break

    if exercise_col:
        exercises = sorted(df[exercise_col].dropna().unique().tolist())
        selected_exercises = st.sidebar.multiselect(
            "Exercícios", exercises, default=exercises[:5]
        )
    else:
        selected_exercises = None

    # Exemplo: se existir coluna de dia/timestamp
    date_col = None
    for c in cols:
        if "start_time" in c.lower() or "date" in c.lower():
            date_col = c
            break

    # aplica filtros
    filtered = df.copy()
    if selected_exercises and exercise_col:
        filtered = filtered[filtered[exercise_col].isin(selected_exercises)]

    # Métricas simples
    st.subheader("📊 Resumo")
    col1, col2, col3 = st.columns(3)
    col1.metric("Linhas (filtradas)", len(filtered))
    if exercise_col:
        col2.metric("Exercícios únicos", filtered[exercise_col].nunique())
    if date_col:
        try:
            filtered["_date_tmp"] = pd.to_datetime(filtered[date_col]).dt.date
            col3.metric("Dias únicos", filtered["_date_tmp"].nunique())
        except Exception:
            pass

    # Tabela
    st.subheader("📋 Amostra dos dados")
    st.dataframe(filtered.head(200))


if __name__ == "__main__":
    main()
