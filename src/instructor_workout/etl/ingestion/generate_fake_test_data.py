import os
import uuid
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import numpy as np
import pandas as pd


# =========================
# CONFIGURAÇÕES
# =========================
BUCKET = "instructor-workout-datas"

# Pasta TEST, conforme você pediu (NÃO usa gold)
TEST_PREFIX = "test/fact_workouts"
LOCAL_FILE = "fact_workouts_test.parquet"

# E-mail do usuário de teste que você vai usar no app
TEST_USER_EMAIL = "testuser@example.com"

# Caminho onde o app salva os usuários (igual ao login_service)
USERS_APP_KEY = "gold/users_app/users_app.parquet"


# =========================
# CLIENTE S3 (usa variáveis de ambiente)
# =========================
def get_s3_client():
    """
    Usa as credenciais configuradas no ambiente:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "sa-east-1"),
    )


# =========================
# OBTÉM user_id DO testuser@example.com
# =========================
def get_test_user_id(s3):
    """
    Lê o arquivo de usuários no S3 e retorna o user_id
    do e-mail TEST_USER_EMAIL. Se não achar, cria um ID fixo.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=USERS_APP_KEY)
        data = obj["Body"].read()
        users_df = pd.read_parquet(BytesIO(data))

        if "email" not in users_df.columns or "user_id" not in users_df.columns:
            print("⚠ Arquivo de usuários não possui colunas esperadas (email, user_id).")
            return "test-user-id"

        row = users_df[users_df["email"] == TEST_USER_EMAIL]

        if row.empty:
            print(f"⚠ Usuário {TEST_USER_EMAIL} não encontrado em {USERS_APP_KEY}.")
            return "test-user-id"

        user_id = row.iloc[0]["user_id"]
        print(f"✅ user_id encontrado para {TEST_USER_EMAIL}: {user_id}")
        return str(user_id)

    except Exception as e:
        print(f"⚠ Erro ao ler usuários do S3: {e}")
        print("→ Usando user_id padrão 'test-user-id'.")
        return "test-user-id"


# =========================
# GERA DADOS FALSOS DE TREINO
# =========================
def generate_fake_workouts(user_id: str, days: int = 60) -> pd.DataFrame:
    """
    Gera dados de treino realistas para os últimos N dias.
    Uma linha = 1 treino no dia.
    """
    today = datetime.today().date()
    start_date = today - timedelta(days=days - 1)

    dates = []
    muscle_group = []
    duration = []
    total_sets = []
    total_reps = []
    avg_hr = []
    rpe = []

    muscles = ["Peito", "Costas", "Pernas", "Ombros", "Braços", "Full Body"]
    rng = np.random.default_rng(seed=42)

    for i in range(days):
        d = start_date + timedelta(days=i)

        # Probabilidade de treinar ~ 65% dos dias
        if rng.random() < 0.65:
            dates.append(d)
            muscle_group.append(rng.choice(muscles))
            duration.append(int(rng.normal(55, 10)))         # minutos
            total_sets.append(int(rng.normal(20, 4)))         # séries
            total_reps.append(int(rng.normal(160, 30)))       # repetições
            avg_hr.append(int(rng.normal(135, 15)))           # bpm
            rpe.append(int(np.clip(rng.normal(7.5, 1.5), 4, 10)))  # esforço percebido

    df = pd.DataFrame(
        {
            "user_id": user_id,
            "workout_id": [str(uuid.uuid4()) for _ in range(len(dates))],
            "date": dates,
            "weekday": [d.weekday() for d in dates],  # 0=segunda
            "muscle_group": muscle_group,
            "duration_min": duration,
            "total_sets": total_sets,
            "total_reps": total_reps,
            "avg_heart_rate": avg_hr,
            "perceived_exertion": rpe,
        }
    )

    print(f"✅ Gerados {len(df)} treinos falsos para o user_id={user_id}")
    return df


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    s3 = get_s3_client()

    # 1) descobre o user_id do testuser@example.com (se existir)
    user_id = get_test_user_id(s3)

    # 2) gera dados falsos
    df = generate_fake_workouts(user_id=user_id, days=60)

    # 3) salva localmente
    df.to_parquet(LOCAL_FILE, index=False)
    print(f"💾 Arquivo gerado localmente: {LOCAL_FILE}")

    # 4) envia pro S3 na pasta TEST
    s3_key = f"{TEST_PREFIX}/fact_workouts_test.parquet"
    try:
        s3.upload_file(LOCAL_FILE, BUCKET, s3_key)
        print(f"🚀 Arquivo enviado para s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Erro ao enviar para o S3: {e}")
