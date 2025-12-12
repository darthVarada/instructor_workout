from __future__ import annotations

from datetime import datetime, date
from io import BytesIO
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import streamlit as st
from groq import Groq
import boto3


MODEL = "llama-3.1-8b-instant"
client = Groq(api_key=st.secrets["GROQ_API_KEY"])

# ==========
# S3 CONFIG
# ==========
BUCKET = st.secrets.get("AWS_BUCKET_NAME", "instructor-workout-datas")
FACT_TRAIN_KEY = "gold/fact_train/fact_workout_sets.parquet"


# =========================
# UTILITÁRIOS DE PERFIL
# =========================
def calcular_idade(data_nascimento: str | None) -> int | None:
    if not data_nascimento:
        return None
    try:
        dt = datetime.strptime(data_nascimento, "%Y-%m-%d").date()
    except ValueError:
        try:
            dt = datetime.strptime(data_nascimento, "%d/%m/%Y").date()
        except ValueError:
            return None

    hoje = date.today()
    return hoje.year - dt.year - ((hoje.month, hoje.day) < (dt.month, dt.day))


def _safe(user: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Retorna o primeiro valor não-vazio encontrado nas chaves informadas."""
    for k in keys:
        v = user.get(k)
        if v not in (None, "", 0):
            return v
    return default


def _merge_current_profile(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Garante que o modelo SEMPRE use o perfil mais atualizado:
    - Começa com `user` recebido
    - Faz override com `st.session_state.user_profile` (mais atual após salvar o formulário)
    """
    merged: Dict[str, Any] = {}
    if isinstance(user, dict):
        merged.update(user)

    session_profile = st.session_state.get("user_profile")
    if isinstance(session_profile, dict):
        merged.update(session_profile)

    logged_user = st.session_state.get("logged_user", {}) or {}
    if not merged.get("email") and logged_user.get("email"):
        merged["email"] = logged_user.get("email")

    current_user_id = st.session_state.get("current_user_id")
    if current_user_id and not merged.get("user_id"):
        merged["user_id"] = current_user_id

    return merged


# =========================
# S3 + PARQUET (GOLD)
# =========================
def _get_s3_client_from_secrets():
    """
    Usa credenciais do secrets.toml:
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION
    """
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets.get("AWS_REGION", "sa-east-1"),
    )


@st.cache_data(show_spinner=False, ttl=300)
def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Lê parquet do S3 e retorna DataFrame.
    Cacheado (ttl=5min) pra não ficar caro a cada pergunta.
    """
    s3 = _get_s3_client_from_secrets()
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    return pd.read_parquet(BytesIO(raw))


def _pick_first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    # tentativa por "contains"
    for c in df.columns:
        low = c.lower()
        for cand in candidates:
            if cand.lower() in low:
                return c
    return None


def _coerce_datetime_series(s: pd.Series) -> pd.Series:
    # tenta converter com robustez; se já for datetime, ok
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce", utc=False)


def _build_training_summary_from_fact_train(df: pd.DataFrame, user_id: str) -> Tuple[str, Dict[str, Any]]:
    """
    Cria um resumo textual + stats do histórico de treinos do usuário a partir do fact_train.
    Tenta detectar colunas comuns (date, exercise, sets, reps, weight, muscle_group etc.).
    """
    stats: Dict[str, Any] = {}

    if df.empty or not user_id:
        return "", stats

    # coluna user_id
    user_col = _pick_first_existing_col(df, ["user_id", "userid", "id_user", "user"])
    if not user_col:
        return "", stats

    user_df = df[df[user_col].astype(str) == str(user_id)].copy()
    if user_df.empty:
        return "", stats

    # detectar colunas comuns
    date_col = _pick_first_existing_col(df, ["date", "workout_date", "dt", "timestamp", "time", "created_at"])
    ex_col = _pick_first_existing_col(df, ["exercise_name", "exercise", "exercicio", "movement", "name_exercise"])
    muscle_col = _pick_first_existing_col(df, ["muscle_group", "grupo_muscular", "muscle", "target_muscle"])
    sets_col = _pick_first_existing_col(df, ["sets", "set_count", "total_sets", "n_sets"])
    reps_col = _pick_first_existing_col(df, ["reps", "rep_count", "total_reps", "n_reps"])
    weight_col = _pick_first_existing_col(df, ["weight", "load", "carga", "kg", "peso_carga"])
    rpe_col = _pick_first_existing_col(df, ["rpe", "rir", "intensity", "esforco"])

    # datas
    if date_col:
        user_df[date_col] = _coerce_datetime_series(user_df[date_col])
        user_df = user_df.dropna(subset=[date_col])
        user_df = user_df.sort_values(date_col)

    # métricas globais
    stats["rows"] = int(len(user_df))
    if date_col:
        stats["first_date"] = user_df[date_col].min()
        stats["last_date"] = user_df[date_col].max()
        stats["unique_days"] = int(user_df[date_col].dt.date.nunique())
        stats["weeks"] = int(user_df[date_col].dt.isocalendar().week.nunique())

    # frequencia semanal média
    if date_col:
        weekly = (
            user_df.assign(_week=user_df[date_col].dt.to_period("W"))
            .groupby("_week")
            .size()
        )
        stats["avg_sets_per_week_rows"] = float(round(weekly.mean(), 2)) if len(weekly) else None

    # volume (sets/reps/weight)
    def _sum(col: Optional[str]) -> Optional[float]:
        if not col:
            return None
        s = pd.to_numeric(user_df[col], errors="coerce")
        v = float(s.sum(skipna=True)) if s.notna().any() else None
        return round(v, 2) if v is not None else None

    def _avg(col: Optional[str]) -> Optional[float]:
        if not col:
            return None
        s = pd.to_numeric(user_df[col], errors="coerce")
        v = float(s.mean(skipna=True)) if s.notna().any() else None
        return round(v, 2) if v is not None else None

    stats["total_sets"] = _sum(sets_col)
    stats["total_reps"] = _sum(reps_col)
    stats["avg_weight"] = _avg(weight_col)
    stats["avg_rpe_or_intensity"] = _avg(rpe_col)

    # top exercícios
    top_ex = []
    if ex_col:
        top_ex = (
            user_df[ex_col]
            .astype(str)
            .value_counts()
            .head(8)
            .reset_index()
            .values
            .tolist()
        )
        # formato: [[exercise, count], ...]
        stats["top_exercises"] = [(x[0], int(x[1])) for x in top_ex]

    # top grupos musculares
    top_muscle = []
    if muscle_col:
        top_muscle = (
            user_df[muscle_col]
            .astype(str)
            .value_counts()
            .head(8)
            .reset_index()
            .values
            .tolist()
        )
        stats["top_muscle_groups"] = [(x[0], int(x[1])) for x in top_muscle]

    # últimos registros (para contexto do chat)
    last_rows_preview = []
    preview_cols = [c for c in [date_col, ex_col, muscle_col, sets_col, reps_col, weight_col, rpe_col] if c]
    if preview_cols:
        tail = user_df[preview_cols].tail(10).copy()
        # normaliza data para string
        if date_col:
            tail[date_col] = tail[date_col].dt.strftime("%Y-%m-%d")
        last_rows_preview = tail.to_dict(orient="records")
        stats["last_rows"] = last_rows_preview

    # construir texto compacto e “útil” pro LLM
    def _fmt_dt(x):
        if isinstance(x, pd.Timestamp):
            return x.strftime("%Y-%m-%d")
        return str(x) if x else "—"

    txt_lines = []
    txt_lines.append("Histórico real de treinos (GOLD fact_train):")
    if date_col and stats.get("first_date") is not None:
        txt_lines.append(f"- Período: { _fmt_dt(stats['first_date']) } até { _fmt_dt(stats['last_date']) }")
        txt_lines.append(f"- Dias com treino: {stats.get('unique_days', '—')} | Semanas com registro: {stats.get('weeks', '—')}")

    if stats.get("total_sets") is not None:
        txt_lines.append(f"- Total de séries (soma): {stats['total_sets']}")
    if stats.get("total_reps") is not None:
        txt_lines.append(f"- Total de repetições (soma): {stats['total_reps']}")
    if stats.get("avg_weight") is not None:
        txt_lines.append(f"- Carga média registrada: {stats['avg_weight']}")
    if stats.get("avg_rpe_or_intensity") is not None:
        txt_lines.append(f"- Intensidade média (RPE/RIR/etc): {stats['avg_rpe_or_intensity']}")

    if stats.get("top_exercises"):
        top_str = ", ".join([f"{n} ({c}x)" for n, c in stats["top_exercises"][:6]])
        txt_lines.append(f"- Exercícios mais frequentes: {top_str}")

    if stats.get("top_muscle_groups"):
        topm_str = ", ".join([f"{n} ({c}x)" for n, c in stats["top_muscle_groups"][:6]])
        txt_lines.append(f"- Grupos musculares mais treinados: {topm_str}")

    # mini-log dos últimos treinos (compacto)
    if last_rows_preview:
        txt_lines.append("- Últimos registros (amostra):")
        for r in last_rows_preview[-5:]:
            parts = []
            if date_col and r.get(date_col):
                parts.append(str(r.get(date_col)))
            if ex_col and r.get(ex_col):
                parts.append(str(r.get(ex_col)))
            if sets_col and r.get(sets_col) not in (None, "", "nan"):
                parts.append(f"sets={r.get(sets_col)}")
            if reps_col and r.get(reps_col) not in (None, "", "nan"):
                parts.append(f"reps={r.get(reps_col)}")
            if weight_col and r.get(weight_col) not in (None, "", "nan"):
                parts.append(f"carga={r.get(weight_col)}")
            if muscle_col and r.get(muscle_col):
                parts.append(f"musculo={r.get(muscle_col)}")
            if rpe_col and r.get(rpe_col) not in (None, "", "nan"):
                parts.append(f"int={r.get(rpe_col)}")
            txt_lines.append(f"  • " + " | ".join(parts))

    return "\n".join(txt_lines).strip(), stats


def _get_training_context_text(merged_user: Dict[str, Any]) -> str:
    """
    Busca o dataset GOLD fact_train e gera um contexto textual por user_id.
    Se falhar, retorna string vazia (não quebra o chat).
    """
    user_id = str(merged_user.get("user_id") or merged_user.get("id") or "").strip()
    if not user_id:
        return ""

    try:
        df = _read_parquet_from_s3(BUCKET, FACT_TRAIN_KEY)
    except Exception as e:
        # Não quebra o app; apenas não injeta dados de treino
        st.session_state["__fact_train_error__"] = str(e)
        return ""

    try:
        text, stats = _build_training_summary_from_fact_train(df, user_id)
        st.session_state["__fact_train_stats__"] = stats
        return text
    except Exception as e:
        st.session_state["__fact_train_error__"] = str(e)
        return ""


# =========================
# PROMPT (COM PERFIL + TREINO)
# =========================
def format_user_profile(user: Dict[str, Any]) -> str:
    idade = calcular_idade(str(user.get("data_nascimento") or "")) if user.get("data_nascimento") else None

    nome = _safe(user, "nome", default="Não informado")
    sexo = _safe(user, "sexo", default="Não informado")
    objetivo = _safe(user, "objetivo", default="Não informado")

    peso = _safe(user, "peso", default="Não informado")
    altura = _safe(user, "altura", default="Não informada")
    gordura = _safe(user, "gordura", "percentual_gordura", default="Não informada")

    experiencia = _safe(user, "experiencia", "nivel_treinamento", default="Não informado")
    frequencia = _safe(user, "dias_semana", "frequencia_semanal", default="Não informada")
    minutos_treino = _safe(user, "minutos_por_treino", default="Não informado")

    foco = _safe(user, "foco_treino", default="Não informado")
    tipo_treino = _safe(user, "tipo_treino_principal", default="Não informado")
    equipamentos = _safe(user, "acesso_equipamentos", default="Não informado")

    passos = _safe(user, "passos_dia", default="Não informado")
    sono = _safe(user, "sono_horas", "horas_sono", default="Não informado")
    estresse = _safe(user, "estresse_nivel", default="Não informado")

    dores = _safe(user, "dores_articulacoes", "restricoes_fisicas", default="Nenhuma informada")
    restricoes_med = _safe(user, "restricoes_medicas", default="Nenhuma informada")
    observacoes = _safe(user, "observacoes_gerais", default="Nenhuma")

    # contexto de treino real (GOLD)
    training_context_text = _get_training_context_text(user)

    base = f"""
Você é um personal trainer profissional. Use SEMPRE os dados abaixo para personalizar as respostas.
Se algum dado estiver faltando, você deve perguntar de forma objetiva o que falta antes de montar um plano completo.

Perfil do aluno:
- Nome: {nome}
- Idade: {idade if idade is not None else "Não informada"}
- Data de nascimento: {user.get("data_nascimento") or "Não informada"}
- Sexo: {sexo}

Composição corporal:
- Peso (kg): {peso}
- Altura: {altura}
- % Gordura: {gordura}

Objetivo e nível:
- Objetivo: {objetivo}
- Nível de treinamento/experiência: {experiencia}
- Foco do programa: {foco}

Rotina:
- Frequência semanal (dias/semana): {frequencia}
- Minutos por treino: {minutos_treino}
- Passos/dia: {passos}
- Sono (horas/noite): {sono}
- Estresse (1 a 5): {estresse}

Preferências:
- Tipo de treino principal: {tipo_treino}
- Acesso a equipamentos: {equipamentos}

Limitações e observações:
- Dores/limitações articulares: {dores}
- Restrições médicas: {restricoes_med}
- Observações gerais: {observacoes}
""".strip()

    if training_context_text:
        base += "\n\n" + training_context_text

    base += """

Regras obrigatórias:
- Nunca ignore dores/limitações e restrições médicas.
- Nunca proponha mais dias de treino do que a frequência semanal informada (se estiver informada).
- Use o histórico real de treinos (se existir) para identificar padrões, pontos fracos, e sugerir progressão.
- Se o usuário corrigir alguma informação, ajuste suas recomendações.
- Responda SEMPRE em português do Brasil.
- Seja prático, com passos claros e justificativas curtas.
""".strip()

    return base


# =========================
# FUNÇÃO PRINCIPAL DO CHAT
# =========================
def generate_training_plan(user: Dict[str, Any], pergunta: str) -> str:
    """
    Gera resposta da IA usando:
    - perfil mais atualizado (session_state)
    - + histórico real de treino do GOLD fact_train (S3)
    """
    merged_user = _merge_current_profile(user)
    system_prompt = format_user_profile(merged_user)

    messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]

    history = st.session_state.get("chat_history", [])
    if isinstance(history, list) and history:
        messages.extend(history)

    messages.append({"role": "user", "content": pergunta})

    completion = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.6,
    )

    answer = completion.choices[0].message.content.strip()
    return answer
