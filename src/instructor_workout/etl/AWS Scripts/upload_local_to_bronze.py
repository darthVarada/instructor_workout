from instructor_workout.utils.aws_client import get_s3
from botocore.exceptions import ClientError

BUCKET = "instructor-workout-datas"

files_to_upload = {
    "data/silver/synthetic_realistic_workout.csv": 
        "bronze/raw/synthetic_realistic_workout/data.csv",

    "data/silver/users_form_log_birthdate.csv": 
        "bronze/raw/users_form_log_birthdate/data.csv",
}


def upload_files():
    s3 = get_s3()

    for local, key in files_to_upload.items():
        try:
            print(f"➡ Upload {local} → {key}")
            s3.upload_file(local, BUCKET, key)
            print("✔ OK")
        except ClientError as e:
            print("❌ Erro:", e)


if __name__ == "__main__":
    upload_files()
