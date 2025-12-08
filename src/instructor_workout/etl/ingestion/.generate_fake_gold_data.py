import os
import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
import boto3
from dotenv import load_dotenv

# ==============================================================
# 1️⃣ Carregar variáveis AWS do .env (CAMINHO CORRETO!)
# ==============================================================

ENV_PATH = r"C:\Users\69pctechops\instructor_workout\airflow\.env"

if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f"❌ Arquivo .env não encontrado em: {ENV_PATH}")

load_dotenv(ENV_PATH)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise Exception("❌ ERRO: AWS_ACCESS_KEY_ID ou AWS_SECRET_ACCESS_KEY não foram carregadas do .env!")

print("✅ Credenciais AWS carregadas com sucesso.")


# ==============================================================
# 2️⃣ Configuração do S3
# ==============================================================

BUCKET = "instructor-workout-datas"
GOLD_FOLDER = "gold/fact_workouts/"
LOCAL_FILE = "fact_workouts_test_user.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)


# ==============================================================
# 3️⃣ GERAR DADOS REALISTAS
# ==============================================================

def generate_fake_workouts(user_id: str, days: int = 30):
    base_date = datetime.today()

    exercises = [
        "Supino reto", "Supino inclinado", "Agachamento livre", "Leg press",
        "Puxada frontal", "Remada baixa", "Remada curvada",
        "Bíceps rosca direta", "Tríceps corda", "Tríceps francês",
        "Elevação lateral", "Desenvolvimento militar"
    ]

    records = []

    for i in range(days):
        training_date = base_date - timedelta(days=i)
        num_exercises = random.randint(4, 8)

        for _ in range(num_exercises):
            exercise = random.choice(exercises)
            weight = random.randint(10, 120)
            reps = random.randint(6, 15)
            sets = random.randint(3, 5)
            volume = weight * reps * sets

            records.append({
                "workout_id": str(uuid.uuid4()),
                "user_id": user_id,
                "date": training_date.strftime("%Y-%m-%d"),
                "exercise": exercise,
                "weight": weight,
                "reps": reps,
                "sets": sets,
                "volume": volume
            })

    return pd.DataFrame(records)


# ==============================================================
# 4️⃣ Criar dados e salvar localmente
# ==============================================================

TEST_USER_ID = "test_user_dashboard_123"

df = generate_fake_workouts(TEST_USER_ID, days=30)

df.to_parquet(LOCAL_FILE)

print(f"📄 Arquivo gerado localmente: {LOCAL_FILE}")
print(df.head())


# ==============================================================
# 5️⃣ Enviar para o S3
# ==============================================================

s3_key = GOLD_FOLDER + LOCAL_FILE

try:
    s3.upload_file(LOCAL_FILE, BUCKET, s3_key)
    print(f"✅ Upload realizado com sucesso para: s3://{BUCKET}/{s3_key}")

except Exception as e:
    print("❌ ERRO AO ENVIAR PARA O S3:")
    print(e)
