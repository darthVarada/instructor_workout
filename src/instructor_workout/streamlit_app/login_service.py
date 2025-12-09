import uuid
from datetime import datetime
from typing import Tuple, Dict, Any

import bcrypt
import pandas as pd

import boto3
from io import BytesIO
import streamlit as st

from s3_utils import read_parquet_folder, write_parquet, append_parquet


BUCKET = "instructor-workout-datas"

# GOLD ORIGINAL
USERS_PATH = "gold/app_users/"
USERS_KEY = USERS_PATH + "users_app.parquet"

# GOLD DIM USERS  ✅ AJUSTE AQUI (no plural)
DIM_USER_PATH = "gold/dim_users/"

# BRONZE (INCREMENTAL)
BRONZE_USER_FORM_PATH = "bronze/user_form/"


# ============================================================
# HELPER: CLIENTE S3 USANDO CREDENCIAIS DO STREAMLIT
# ============================================================
def _get_s3_client_from_secrets():
    """
    Usa as credenciais do Streamlit (secrets.toml) para acessar o S3.
    """
    print("🔧 [DEBUG] Criando cliente S3 com credenciais do Streamlit...")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets.get("AWS_REGION", "sa-east-1"),
    )
    print("✅ [DEBUG] Cliente S3 criado.")
    return s3


# ============================================================
# LOAD DATAFRAME (GOLD users_app)
# ============================================================
def _load_users_df() -> pd.DataFrame:
    print("🔧 [DEBUG] Carregando GOLD users_app...")
    df = read_parquet_folder(BUCKET, USERS_PATH)
    if df is None or df.empty:
        print("⚠ [DEBUG] users_app vazio. Retornando DF com colunas padrão.")
        return pd.DataFrame(
            columns=[
                "user_id",
                "nome",
                "email",
                "password_hash",
                "created_at",
                "data_nascimento",
                "sexo",
                "peso",
                "altura",
                "percentual_gordura",
                "objetivo",
                "nivel_treinamento",
                "restricoes_fisicas",
                "frequencia_semanal",
                "horas_sono",
                "nutricional_score",
            ]
        )
    print(f"✅ [DEBUG] users_app carregado. Linhas: {len(df)}")
    return df


def _save_users_df(df: pd.DataFrame) -> None:
    print("💾 [DEBUG] Salvando GOLD users_app...")
    write_parquet(df, BUCKET, USERS_KEY)
    print("✅ [DEBUG] GOLD users_app salvo.")


# ============================================================
# UTIL — CONVERTE LINHA EM PERFIL
# ============================================================
def _row_to_user(row: pd.Series) -> Dict[str, Any]:
    return {
        "user_id": row.get("user_id"),
        "nome": row.get("nome"),
        "email": row.get("email"),
        "data_nascimento": row.get("data_nascimento"),
        "sexo": row.get("sexo"),
        "peso": row.get("peso"),
        "altura": row.get("altura"),
        "percentual_gordura": row.get("percentual_gordura"),
        "objetivo": row.get("objetivo"),
        "nivel_treinamento": row.get("nivel_treinamento"),
        "restricoes_fisicas": row.get("restricoes_fisicas"),
        "frequencia_semanal": row.get("frequencia_semanal"),
        "horas_sono": row.get("horas_sono"),
        "nutricional_score": row.get("nutricional_score"),
    }


# ============================================================
# REGISTER USER
# ============================================================
def register_user(name: str, email: str, password: str) -> Tuple[bool, str | Dict[str, Any]]:
    email = email.strip().lower()

    if not name or not email or not password:
        return False, "Nome, e-mail e senha são obrigatórios."

    df = _load_users_df()

    if not df.empty and email in df["email"].str.lower().values:
        return False, "Este e-mail já está cadastrado."

    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    user_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    new_user = {
        "user_id": user_id,
        "nome": name,
        "email": email,
        "password_hash": password_hash,
        "created_at": now,
        "data_nascimento": None,
        "sexo": None,
        "peso": None,
        "altura": None,
        "percentual_gordura": None,
        "objetivo": None,
        "nivel_treinamento": None,
        "restricoes_fisicas": None,
        "frequencia_semanal": None,
        "horas_sono": None,
        "nutricional_score": None,
    }

    df = pd.concat([df, pd.DataFrame([new_user])], ignore_index=True)
    _save_users_df(df)

    return True, _row_to_user(pd.Series(new_user))


# ============================================================
# AUTHENTICATE USER
# ============================================================
def authenticate(email: str, password: str) -> Tuple[bool, str | Dict[str, Any]]:
    email = email.strip().lower()
    df = _load_users_df()

    if df.empty:
        return False, "Usuário não encontrado."

    mask = df["email"].str.lower() == email
    if not mask.any():
        return False, "Usuário não encontrado."

    row = df[mask].iloc[0]
    stored_hash = row["password_hash"]

    if not bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
        return False, "Senha incorreta."

    return True, _row_to_user(row)


# ============================================================
# LOAD MAIS RECENTE DE GOLD/dim_users (COM PRINTS)
# ============================================================
def _load_latest_dim_user_df() -> pd.DataFrame:
    """
    Lista os arquivos em gold/dim_users/, pega o MAIS RECENTE
    (LastModified) e carrega o parquet em um DataFrame.
    """
    try:
        print("🌐 [DEBUG] Acessando AWS S3 para DIM_USER...")
        s3 = _get_s3_client_from_secrets()

        print(f"📂 [DEBUG] Listando objetos em s3://{BUCKET}/{DIM_USER_PATH}")
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=DIM_USER_PATH)
        contents = resp.get("Contents", [])

        if not contents:
            print("⚠ [DEBUG] Nenhum arquivo encontrado em GOLD/dim_users/")
            return pd.DataFrame()

        print("📄 [DEBUG] Arquivos encontrados em GOLD/dim_users/:")
        for obj in contents:
            print(f"   - {obj['Key']} | LastModified={obj['LastModified']}")

        # pega o mais recente pela data de modificação
        latest_obj = max(contents, key=lambda obj: obj["LastModified"])
        key = latest_obj["Key"]
        print(f"✅ [DEBUG] Arquivo DIM_USER mais recente selecionado: {key}")

        print(f"📥 [DEBUG] Lendo objeto {key} do S3...")
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj["Body"].read()
        df = pd.read_parquet(BytesIO(data))

        print("✅ [DEBUG] DIM_USER carregado. Linhas:", len(df))
        print("👀 [DEBUG] Primeiras linhas de DIM_USER:")
        try:
            print(df.head().to_string())
        except Exception as e:
            print("⚠ [DEBUG] Erro ao dar head() no DF DIM_USER:", e)

        # Normaliza nomes de colunas com acento, se existirem
        rename_map = {
            "nível_treinamento": "nivel_treinamento",
            "restrições_físicas": "restricoes_fisicas",
            "frequência_semanal": "frequencia_semanal",
        }
        df = df.rename(columns=rename_map)

        # Mostra as colunas disponíveis
        print("🧱 [DEBUG] Colunas de DIM_USER:", list(df.columns))

        return df

    except Exception as e:
        print(f"❌ [DEBUG] Erro ao carregar DIM_USER do S3: {e}")
        return pd.DataFrame()


# ============================================================
# GET PROFILE FROM GOLD DIM_USER
# ============================================================
def get_latest_profile_from_dim_user(user_id: str) -> Dict[str, Any] | None:
    """
    Lê o arquivo MAIS RECENTE em gold/dim_users/ e retorna
    o registro do user_id informado.
    """
    print(f"🔎 [DEBUG] Buscando perfil na DIM_USER para user_id={user_id}")
    if not user_id:
        print("⚠ [DEBUG] get_latest_profile_from_dim_user chamado sem user_id")
        return None

    df = _load_latest_dim_user_df()

    if df is None or df.empty:
        print("⚠ [DEBUG] DF DIM_USER vazio depois do load.")
        return None

    if "user_id" not in df.columns:
        print("⚠ [DEBUG] DIM_USER não contém coluna 'user_id'. Colunas atuais:", list(df.columns))
        return None

    # Mostra alguns user_id existentes para debug
    try:
        print("👀 [DEBUG] Alguns user_id presentes em DIM_USER:")
        print(df["user_id"].head().to_list())
    except Exception as e:
        print("⚠ [DEBUG] Não foi possível printar a coluna user_id:", e)

    user_rows = df[df["user_id"] == user_id]

    print(f"🔢 [DEBUG] Registros encontrados para esse user_id: {len(user_rows)}")

    if user_rows.empty:
        print(f"⚠ [DEBUG] Nenhum registro de DIM_USER para user_id={user_id}")
        return None

    # Se houver coluna de data, tenta ordenar
    for col in ["updated_at", "data_registro", "last_update"]:
        if col in user_rows.columns:
            print(f"📌 [DEBUG] Ordenando registros do user_id por coluna '{col}' (desc).")
            user_rows = user_rows.sort_values(col, ascending=False)
            break

    latest_row = user_rows.iloc[0].to_dict()
    print(f"✅ [DEBUG] Perfil DIM_USER encontrado para {user_id}:")
    print(latest_row)
    return latest_row


# ============================================================
# GET PROFILE (para o formulário)
# ============================================================
def get_user_profile_by_id(user_id: str) -> Dict[str, Any] | None:
    """
    A aplicação lê SOMENTE o GOLD/dim_users para este fluxo.
    """
    dim_profile = get_latest_profile_from_dim_user(user_id)

    if dim_profile:
        return dim_profile

    print("⚠ [DEBUG] Nenhum perfil encontrado na DIM_USER para:", user_id)
    return None


# ============================================================
# GET PROFILE (LEGADO – users_app)
# ============================================================
def get_user_profile(user_id: str) -> Dict[str, Any] | None:
    """FALLBACK antigo (mantido para compatibilidade)."""
    df = _load_users_df()
    mask = df["user_id"] == user_id
    if not mask.any():
        return None
    return _row_to_user(df[mask].iloc[0])


# ============================================================
# SAVE PROFILE (GOLD users_app + BRONZE incremental)
# ============================================================
def save_user_profile(profile: Dict[str, Any]) -> None:
    """
    Atualiza GOLD (users_app.parquet) e salva uma linha incremental
    na BRONZE (bronze/user_form/).
    """
    print("💾 [DEBUG] Salvando perfil (GOLD + BRONZE)...")
    df = _load_users_df()
    user_id = profile.get("user_id")

    if not user_id:
        print("❌ [DEBUG] Erro: perfil sem user_id")
        return

    mask = df["user_id"] == user_id
    if not mask.any():
        print("❌ [DEBUG] Erro: usuário não encontrado na GOLD (users_app)")
        return

    idx = df[mask].index[0]

    # Atualiza GOLD users_app
    for field in [
        "nome",
        "data_nascimento",
        "sexo",
        "peso",
        "altura",
        "percentual_gordura",
        "objetivo",
        "nivel_treinamento",
        "restricoes_fisicas",
        "frequencia_semanal",
        "horas_sono",
        "nutricional_score",
    ]:
        if field in profile:
            df.at[idx, field] = profile[field]

    _save_users_df(df)

    # BRONZE INCREMENTAL (mantido)
    bronze_record = profile.copy()
    bronze_record["updated_at"] = datetime.utcnow().isoformat()

    file_name = f"user_form_log_{datetime.today().strftime('%Y%m%d')}.parquet"

    append_parquet(
        pd.DataFrame([bronze_record]),
        BUCKET,
        BRONZE_USER_FORM_PATH + file_name,
    )
    print("✅ [DEBUG] Perfil salvo em GOLD e BRONZE.")
