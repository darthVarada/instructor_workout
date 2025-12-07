import boto3
import os
from dotenv import load_dotenv

# Carrega variáveis do .env (que já estão disponíveis dentro do container)
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

BUCKET = "instructor-workout-datas"

PREFIXES = [
    "bronze/",
    "bronze/raw/",
    "bronze/raw/synthetic_realistic_workout/",
    "bronze/raw/users_form_log_birthdate/",
    "bronze/raw/kaggle/",
    "silver/",
    "gold/",
]

def get_s3():
    """Retorna client S3 utilizando as credenciais do .env dentro do container."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

def create_prefixes():
    s3 = get_s3()

    print("\n=== Criando Data Lake no S3 ===\n")

    for prefix in PREFIXES:
        s3.put_object(Bucket=BUCKET, Key=prefix)
        print(f"✔ Criado: {prefix}")

if __name__ == "__main__":
    create_prefixes()
