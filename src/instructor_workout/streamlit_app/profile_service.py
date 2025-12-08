import pandas as pd
from s3_utils import read_parquet_folder, write_parquet

BUCKET = "instructor-workout-datas"
PATH = "gold/user_profiles/"

def save_user_profile(profile):
    df = None

    try:
        df = read_parquet_folder(BUCKET, PATH)
    except:
        df = pd.DataFrame()

    df = pd.concat([df, pd.DataFrame([profile])], ignore_index=True)

    write_parquet(df, BUCKET, PATH + "profiles.parquet")
