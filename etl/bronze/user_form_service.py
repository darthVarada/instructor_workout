import os
from io import BytesIO
from datetime import datetime
import pandas as pd
import boto3
import streamlit as st

BUCKET = "instructor-workout-datas"
BRONZE_PREFIX = "bronze/raw/users_form_log_birthdate/"


def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets.get("AWS_REGION", "sa-east-1"),
    )


# ==================================================
# LER TODAS AS VERSÕES DO USER_FORM (INCREMENTAL)
# ==================================================
def load_user_profile_from_bronze(user_id: str):

    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX)

    if "Contents" not in response:
        return None

    dfs = []

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        parquet_obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(parquet_obj["Body"].read()))
        dfs.append(df)

    if not dfs:
        return None

    full_df = pd.concat(dfs, ignore_index=True)

    if "user_id" not in full_df.columns:
        return None

    df_user = full_df[full_df["user_id"] == user_id]

    if df_user.empty:
        return None

    df_user["updated_at"] = pd.to_datetime(df_user["updated_at"])
    df_user = df_user.sort_values("updated_at", ascending=False)

    return df_user.iloc[0].to_dict()



# ==================================================
# SALVAR NOVA VERSÃO DO USER_FORM
# ==================================================
def save_user_profile_to_bronze(profile_dict: dict):
    """
    NÃO SOBRESCREVE nada. Apenas cria um novo arquivo incremental.
    """

    df = pd.DataFrame([profile_dict])

    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"users_form_{today}.parquet"

    s3_key = BRONZE_PREFIX + filename

    s3 = get_s3()

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f"✔ Novo registro salvo em: s3://{BUCKET}/{s3_key}")
