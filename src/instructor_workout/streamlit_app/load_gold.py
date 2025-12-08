from s3_utils import read_parquet_folder
import pandas as pd
import json
import boto3
import streamlit as st

BUCKET = "instructor-workout-datas"


def load_dim_users():
    return read_parquet_folder(BUCKET, "gold/dim_users/")


def load_fact_workouts(user_id=None):
    df = read_parquet_folder(BUCKET, "gold/fact_workouts/")
    if user_id:
        df = df[df["user_id"] == user_id]
    return df


def load_metrics():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets["AWS_REGION"]
    )
    obj = s3.get_object(Bucket=BUCKET, Key="gold/metrics/metrics.json")
    content = obj["Body"].read().decode("utf-8")
    return json.loads(content)
