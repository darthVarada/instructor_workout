import os
from io import BytesIO
from datetime import datetime

import boto3
import pandas as pd
import streamlit as st

# Tenta importar altair só se existir (para gráfico de pizza bonito)
try:
    import altair as alt
except ImportError:
    alt = None

# =========================
# CONFIG
# =========================
BUCKET = "instructor-workout-datas"
TEST_KEY = "test/fact_workouts/fact_workouts_test.parquet"
TEST_USER_ID = "test_user_001"  # 🔥 usado para isolar usuário de teste


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

        # NÃO forço mais a coluna "date" existir aqui
        # Só faço a conversão se de fato existir
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        return df
    except Exception as e:
        st.error(f"Erro ao ler dados de treino em TEST: {e}")
        return pd.DataFrame()


# =========================
# DASHBOARD PRINCIPAL
# =========================
def render(user: dict | None):
    st.title("📊 Dashboard de Evolução")

    st.info(
        "Este dashboard está usando **dados de treino de teste** "
        "da pasta `test/fact_workouts` no S3 (ou seus dados reais, "
        "se existirem para o seu `user_id`)."
    )

    df = read_test_workouts()

    if df.empty:
        st.warning("Nenhum dado de treino encontrado em TEST. Rode o script de geração primeiro.")
        st.code("python src/instructor_workout/etl/ingestion/generate_fake_test_data.py")
        return

    # =========================
    # OBTÉM USER ID DO PERFIL
    # =========================
    user_id = None
    if isinstance(user, dict):
        user_id = user.get("user_id") or user.get("id")

    # 🔥 1. Se for usuário especial de teste, usa TEST_USER_ID
    if user and user.get("email") == "testuser@example.com":
        user_id = TEST_USER_ID

    # 🔥 2. Filtra de acordo com user_id
    if user_id == TEST_USER_ID:
        # Usuário de teste → vê o dataset fake inteiro
        user_df = df.copy()
    else:
        # Usuário real → filtra por user_id
        if "user_id" in df.columns and user_id is not None:
            user_df = df[df["user_id"] == user_id].copy()
        else:
            user_df = pd.DataFrame()

        if user_df.empty:
            st.warning(
                "📭 Você ainda não possui treinos registrados no sistema real "
                "para o seu usuário. Quando houver dados, o dashboard será preenchido."
            )
            return

    # =========================
    # GARANTINDO COLUNA DE DATA (SE EXISTIR)
    # =========================
    has_date = "date" in user_df.columns

    if has_date:
        user_df["date"] = pd.to_datetime(user_df["date"], errors="coerce")
        user_df = user_df.dropna(subset=["date"])
        # Colunas derivadas
        user_df["date_only"] = user_df["date"].dt.date
        user_df["week"] = user_df["date"].dt.isocalendar().week
        user_df["year"] = user_df["date"].dt.year
        user_df["year_week"] = user_df["date"].dt.strftime("%Y-%U")
        user_df["month"] = user_df["date"].dt.to_period("M").dt.to_timestamp()
        user_df["dow"] = user_df["date"].dt.day_name()
    else:
        st.warning(
            "⚠️ O dataset de treino não possui coluna `date`. "
            "Gráficos de evolução temporal serão ocultados."
        )

    # =========================
    # MÉTRICAS GERAIS
    # =========================
    total_workouts = len(user_df)
    total_days = user_df["date_only"].nunique() if has_date else None
    last_workout = user_df["date"].max().date() if has_date and total_workouts > 0 else None

    avg_sets = (
        round(user_df["total_sets"].mean(), 1)
        if "total_sets" in user_df.columns
        else None
    )
    avg_reps = (
        round(user_df["total_reps"].mean(), 1)
        if "total_reps" in user_df.columns
        else None
    )
    avg_duration = (
        round(user_df["duration_min"].mean(), 1)
        if "duration_min" in user_df.columns
        else None
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Treinos registrados", total_workouts)
    with col2:
        st.metric("Dias diferentes treinados", total_days or "-")
    with col3:
        st.metric(
            "Último treino",
            last_workout.strftime("%d/%m/%Y") if last_workout else "-",
        )

    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric("Séries por treino (média)", avg_sets or "-")
    with col5:
        st.metric("Reps por treino (média)", avg_reps or "-")
    with col6:
        st.metric("Duração (min, média)", avg_duration or "-")

    st.markdown("---")

    # =========================
    # 1) TREINOS POR SEMANA (BAR)
    # =========================
    if has_date:
        st.subheader("📅 Frequência semanal de treinos")

        weekly = (
            user_df.groupby("year_week")
            .size()
            .reset_index(name="workouts")
            .sort_values("year_week")
        )

        if not weekly.empty:
            weekly = weekly.set_index("year_week")
            st.bar_chart(weekly["workouts"], use_container_width=True)
        else:
            st.info("Ainda não há dados suficientes para treinos por semana.")

    # =========================
    # 2) EVOLUÇÃO DIÁRIA DE VOLUME (LINE + MÉDIA MÓVEL)
    # =========================
    if has_date and "total_sets" in user_df.columns:
        st.subheader("📈 Volume diário (séries) com média móvel")

        daily = (
            user_df.groupby("date_only")["total_sets"]
            .sum()
            .reset_index()
            .rename(columns={"date_only": "day", "total_sets": "daily_sets"})
            .sort_values("day")
        )

        if not daily.empty:
            daily["rolling_7d"] = daily["daily_sets"].rolling(window=7, min_periods=1).mean()

            st.line_chart(
                daily.set_index("day")[["daily_sets", "rolling_7d"]],
                use_container_width=True,
            )
        else:
            st.info("Sem dados suficientes para evolução diária de volume.")

    # =========================
    # 3) VOLUME POR GRUPO MUSCULAR (BAR + PIE)
    # =========================
    st.subheader("💪 Volume por grupo muscular")

    if "muscle_group" in user_df.columns and "total_sets" in user_df.columns:
        muscle_vol = (
            user_df.groupby("muscle_group")["total_sets"]
            .sum()
            .reset_index()
            .sort_values("total_sets", ascending=False)
        )

        col_bar, col_pie = st.columns(2)

        with col_bar:
            st.markdown("**Distribuição em barras**")
            st.bar_chart(
                data=muscle_vol.set_index("muscle_group"),
                use_container_width=True,
            )

        with col_pie:
            st.markdown("**Participação percentual (%)**")
            if alt is not None:
                muscle_vol["percent"] = 100 * muscle_vol["total_sets"] / muscle_vol["total_sets"].sum()
                chart = (
                    alt.Chart(muscle_vol)
                    .mark_arc()
                    .encode(
                        theta="total_sets",
                        color="muscle_group",
                        tooltip=["muscle_group", "total_sets", alt.Tooltip("percent:Q", format=".1f")],
                    )
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info(
                    "Altair não está instalado para gráfico de pizza. "
                    "Rode `pip install altair` para habilitar."
                )
    else:
        st.info("Os dados de grupos musculares ou séries não estão disponíveis.")

    st.markdown("---")

    # =========================
    # 4) RANKING DE EXERCÍCIOS
    # =========================
    st.subheader("🏆 Top 10 exercícios por volume")

    if "exercise_name" in user_df.columns and "total_sets" in user_df.columns:
        exercise_rank = (
            user_df.groupby("exercise_name")["total_sets"]
            .sum()
            .reset_index()
            .sort_values("total_sets", ascending=False)
            .head(10)
        )

        col_rank_bar, col_rank_table = st.columns([2, 1])

        with col_rank_bar:
            st.bar_chart(
                exercise_rank.set_index("exercise_name"),
                use_container_width=True,
            )

        with col_rank_table:
            st.dataframe(
                exercise_rank,
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("Não há informações de exercícios detalhados para exibir o ranking.")

    # =========================
    # 5) HEATMAP DE DIAS DA SEMANA x GRUPO (se der)
    # =========================
    st.subheader("📆 Padrão de treino por dia da semana (séries)")

    if has_date and "muscle_group" in user_df.columns and "total_sets" in user_df.columns and alt is not None:
        pivot = (
            user_df.groupby(["dow", "muscle_group"])["total_sets"]
            .sum()
            .reset_index()
        )

        # Ordena dia da semana de forma "lógica"
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        pivot["dow"] = pd.Categorical(pivot["dow"], categories=dow_order, ordered=True)
        pivot = pivot.sort_values("dow")

        heatmap = (
            alt.Chart(pivot)
            .mark_rect()
            .encode(
                x=alt.X("muscle_group:N", title="Grupo muscular"),
                y=alt.Y("dow:N", title="Dia da semana"),
                color=alt.Color("total_sets:Q", title="Séries totais"),
                tooltip=["dow", "muscle_group", "total_sets"],
            )
        )

        st.altair_chart(heatmap, use_container_width=True)
    elif has_date:
        st.info(
            "Para ver o heatmap, garanta que há `muscle_group`, `total_sets` "
            "e o pacote `altair` instalado."
        )

    st.markdown("---")

    # =========================
    # 6) DISTRIBUIÇÃO DE INTENSIDADE / RPE (SE EXISTIR)
    # =========================
    st.subheader("🔥 Distribuição de intensidade / carga (se disponível)")

    intensity_candidates = ["avg_rpe", "intensity", "avg_load", "load_kg"]
    intensity_col = next((c for c in intensity_candidates if c in user_df.columns), None)

    if intensity_col is not None:
        st.markdown(f"Usando coluna **`{intensity_col}`** como proxy de intensidade.")

        if alt is not None:
            hist = (
                alt.Chart(user_df)
                .mark_bar()
                .encode(
                    x=alt.X(f"{intensity_col}:Q", bin=alt.Bin(maxbins=20), title="Intensidade"),
                    y=alt.Y("count():Q", title="Quantidade de séries/sets"),
                    tooltip=[intensity_col, "count():Q"],
                )
            )
            st.altair_chart(hist, use_container_width=True)
        else:
            st.bar_chart(user_df[intensity_col].value_counts().sort_index())
    else:
        st.info("Nenhuma coluna de intensidade / RPE encontrada no dataset.")

    # =========================
    # 7) RELAÇÃO DURAÇÃO x VOLUME
    # =========================
    st.subheader("⏱️ Relação duração do treino x volume (se disponível)")

    if "duration_min" in user_df.columns and "total_sets" in user_df.columns and alt is not None:
        scatter = (
            alt.Chart(user_df)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("duration_min:Q", title="Duração (min)"),
                y=alt.Y("total_sets:Q", title="Séries totais"),
                tooltip=["duration_min", "total_sets", "date"],
            )
        )
        st.altair_chart(scatter, use_container_width=True)
    elif "duration_min" in user_df.columns and "total_sets" in user_df.columns:
        st.dataframe(
            user_df[["date"] + ["duration_min", "total_sets"]] if has_date else user_df[["duration_min", "total_sets"]],
            use_container_width=True,
        )
    else:
        st.info("Não há colunas de duração ou volume suficientes para esta análise.")

    st.markdown("---")

    # =========================
    # 8) TABELA DETALHADA (RAW DATA)
    # =========================
    with st.expander("🔍 Ver tabela detalhada de treinos"):
        show_cols = [c for c in user_df.columns if c not in ["workout_id"]]
        if has_date:
            user_df_sorted = user_df.sort_values("date", ascending=False)
        else:
            user_df_sorted = user_df

        st.dataframe(
            user_df_sorted[show_cols],
            use_container_width=True,
            hide_index=True,
        )
