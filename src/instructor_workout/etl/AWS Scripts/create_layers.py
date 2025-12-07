import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

bucket_name = "instructor-workout-datas"
layers = ["bronze/", "silver/", "gold/"]

def create_layer_folders():
    for layer in layers:
        try:
            s3.put_object(Bucket=bucket_name, Key=layer)
            print(f"✓ Criado: {layer}")
        except ClientError as e:
            print(f"Erro criando {layer}: {e}")

if __name__ == "__main__":
    create_layer_folders()
