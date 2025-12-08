import uuid
from datetime import datetime
from typing import Tuple, Dict, Any

import bcrypt
import pandas as pd

from s3_utils import read_parquet_folder, write_parquet


BUCKET = "instructor-workout-datas"
USERS_PATH = "gold/app_users/"
USERS_KEY = USERS_PATH + "users_app.parquet"


def _load_users_df() -> pd.DataFrame:
    df = read_parquet_folder(BUCKET, USERS_PATH)
    if df is None or df.empty:
        # Garante colunas mínimas
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
    return df


def _save_users_df(df: pd.DataFrame) -> None:
    write_parquet(df, BUCKET, USERS_KEY)


def _row_to_user(row: pd.Series) -> Dict[str, Any]:
    """
    Converte uma linha do DataFrame em dict de usuário/perfil.
    """
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


def register_user(name: str, email: str, password: str) -> Tuple[bool, str | Dict[str, Any]]:
    """
    Registra um novo usuário.
    Retorna (True, user_dict) em caso de sucesso,
            (False, mensagem_erro) em caso de falha.
    """
    email = email.strip().lower()
    if not email or not password or not name:
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
        # Perfil ainda não preenchido
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


def authenticate(email: str, password: str) -> Tuple[bool, str | Dict[str, Any]]:
    """
    Autentica usuário por e-mail e senha.
    Retorna (True, user_dict) em caso de sucesso,
            (False, mensagem_erro) em caso de falha.
    """
    email = email.strip().lower()
    df = _load_users_df()

    if df.empty:
        return False, "Usuário não encontrado."

    mask = df["email"].str.lower() == email
    if not mask.any():
        return False, "Usuário não encontrado."

    row = df[mask].iloc[0]

    stored_hash = row["password_hash"]
    if not stored_hash:
        return False, "Usuário sem senha cadastrada."

    if not bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
        return False, "Senha incorreta."

    return True, _row_to_user(row)


def get_user_profile(user_id: str) -> Dict[str, Any] | None:
    df = _load_users_df()
    if df.empty:
        return None

    mask = df["user_id"] == user_id
    if not mask.any():
        return None

    row = df[mask].iloc[0]
    return _row_to_user(row)


def save_user_profile(profile: Dict[str, Any]) -> None:
    """
    Atualiza os dados de perfil do usuário no S3.
    """
    df = _load_users_df()
    if df.empty:
        return

    user_id = profile.get("user_id")
    if not user_id:
        return

    mask = df["user_id"] == user_id
    if not mask.any():
        return

    idx = df[mask].index[0]

    # Atualiza campos de perfil
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
