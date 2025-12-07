import boto3
import os
from dotenv import load_dotenv
from botocore.config import Config


dotenv_path = os.getenv("ENV_FILE_PATH")
if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path)

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise RuntimeError("❌ Credenciais AWS não encontradas no .env")

boto_config = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=10,
    read_timeout=60,
)


def get_s3():
    """Retorna client S3 padronizado para todo o projeto."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=boto_config
    )
