import boto3
import pandas as pd
from io import BytesIO
import streamlit as st
import os

# ============================================================
# GET CREDENTIALS (STREAMLIT OR ENVIRONMENT)
# ============================================================
def get_credentials():
    """
    Usa st.secrets quando possível (Streamlit).
    Caso contrário usa variáveis de ambiente (FastAPI, scripts).
    """
    if hasattr(st, "secrets") and "AWS_ACCESS_KEY" in st.secrets:
        return {
            "aws_access_key_id": st.secrets["AWS_ACCESS_KEY"],
            "aws_secret_access_key": st.secrets["AWS_SECRET_KEY"],
            "region_name": st.secrets.get("AWS_REGION", "sa-east-1"),
        }

    # fallback para API FastAPI
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_KEY"),
        "region_name": os.getenv("AWS_REGION", "sa-east-1"),
    }


# ============================================================
# CREATE S3 CLIENT
# ============================================================
def get_s3():
    creds = get_credentials()

    if not creds["aws_access_key_id"]:
        raise ValueError("❌ Nenhuma credencial AWS encontrada para S3.")

    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        region_name=creds["region_name"],
    )


# ============================================================
# READ PARQUET FOLDER
# ============================================================
def read_parquet_folder(bucket, prefix):
    s3 = get_s3()
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in objs:
        return pd.DataFrame()

    dfs = []
    for obj in objs["Contents"]:
        if obj["Key"].endswith(".parquet"):
            data = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            dfs.append(pd.read_parquet(BytesIO(data)))

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ============================================================
# WRITE PARQUET
# ============================================================
def write_parquet(df, bucket, key):
    s3 = get_s3()
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


# ============================================================
# APPEND PARQUET (INCREMENTAL)
# ============================================================
def append_parquet(df, bucket, key):
    s3 = get_s3()

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(BytesIO(obj["Body"].read()))
        final_df = pd.concat([existing, df], ignore_index=True)
    except:
        final_df = df

    write_parquet(final_df, bucket, key)
