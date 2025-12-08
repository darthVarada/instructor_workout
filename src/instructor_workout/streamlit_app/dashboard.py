import os
from io import BytesIO
from datetime import datetime

import boto3
import pandas as pd
import streamlit as st


# =========================
# CONFIG
# =========================
BUCKET = "instructor-workout-datas"
TEST_KEY = "test/fact_workouts/fact_workouts_test.parquet"


# =========================
# S3 + LEITURA DO PARQUET
# =========================
def get_s3_client_from_secrets():
    """
    Usa as credenciais que você já colocou no secrets.toml
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION
    """
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets.get("AWS_REGION", "sa-east-1"),
    )


def read_test_workouts():
    """
    Lê o arquivo de treinos falsos em:
    s3://instructor-workout-datas/test/fact_workouts/fact_workouts_test.parquet
    """
    s3 = get_s3_client_from_secrets()
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=TEST_KEY)
        data = obj["Body"].read()
        df = pd.read_parquet(BytesIO(data))

        # Garante que a coluna date é datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        return df
    except Exception as e:
        st.error(f"Erro ao ler dados de treino em TEST: {e}")
        return pd.DataFrame()


# =========================
# DASHBOARD
# =========================
def render(user: dict | None):
    st.title("📊 Dashboard de Evolução")

    st.info(
        "Este dashboard está usando **dados de treino falsos** "
        "da pasta `test/fact_workouts` no S3, apenas para validar o fluxo."
    )

    df = read_test_workouts()

    if df.empty:
        st.warning("Nenhum dado de treino encontrado em TEST. Rode o script de geração primeiro.")
        st.code("python src/instructor_workout/etl/ingestion/generate_fake_test_data.py")
        return

    # Filtra por usuário se possível
    user_id = None
    if isinstance(user, dict):
        user_id = user.get("user_id") or user.get("id")

    if user_id and "user_id" in df.columns:
        user_df = df[df["user_id"] == user_id].copy()
        if user_df.empty:
            st.warning(
                "Não encontrei treinos para o seu usuário nos dados de TEST. "
                "Mostrando métricas gerais de todos os treinos."
            )
            user_df = df.copy()
    else:
        user_df = df.copy()

    # =========================
    # MÉTRICAS GERAIS
    # =========================
    user_df["date"] = pd.to_datetime(user_df["date"])

    total_workouts = len(user_df)
    total_days = user_df["date"].dt.date.nunique()
    last_workout = user_df["date"].max().date() if total_workouts > 0 else None

    avg_sets = round(user_df["total_sets"].mean(), 1) if "total_sets" in user_df.columns else None
    avg_reps = round(user_df["total_reps"].mean(), 1) if "total_reps" in user_df.columns else None
    avg_duration = (
        round(user_df["duration_min"].mean(), 1) if "duration_min" in user_df.columns else None
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Treinos registrados", total_workouts)
    with col2:
        st.metric("Dias diferentes treinados", total_days)
    with col3:
        st.metric("Último treino", last_workout.strftime("%d/%m/%Y") if last_workout else "-")

    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric("Séries por treino (média)", avg_sets or "-")
    with col5:
        st.metric("Reps por treino (média)", avg_reps or "-")
    with col6:
        st.metric("Duração (min, média)", avg_duration or "-")

    st.markdown("---")

    # =========================
    # TREINOS POR SEMANA
    # =========================
    st.subheader("📅 Treinos por semana")

    weekly = (
        user_df.assign(week=user_df["date"].dt.isocalendar().week)
        .groupby("week")
        .size()
        .reset_index(name="workouts")
        .sort_values("week")
    )

    if not weekly.empty:
        st.bar_chart(
            data=weekly.set_index("week"),
            use_container_width=True,
        )
    else:
        st.info("Ainda não há dados suficientes para esta visualização.")

    # =========================
    # VOLUME POR GRUPO MUSCULAR
    # =========================
    st.subheader("💪 Volume por grupo muscular")

    if "muscle_group" in user_df.columns and "total_sets" in user_df.columns:
        muscle_vol = (
            user_df.groupby("muscle_group")["total_sets"]
            .sum()
            .reset_index()
            .sort_values("total_sets", ascending=False)
        )
        muscle_vol = muscle_vol.set_index("muscle_group")
        st.bar_chart(muscle_vol, use_container_width=True)
    else:
        st.info("Os dados de grupos musculares ou séries não estão disponíveis.")

    # =========================
    # TABELA DETALHADA
    # =========================
    with st.expander("Ver tabela detalhada de treinos"):
        show_cols = [c for c in user_df.columns if c not in ["workout_id"]]
        st.dataframe(
            user_df[show_cols].sort_values("date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
