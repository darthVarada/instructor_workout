import os
from dotenv import load_dotenv
import boto3

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise RuntimeError("❌ Credenciais AWS não encontradas! Verifique seu arquivo .env.")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

print("Credenciais carregadas com sucesso!")
