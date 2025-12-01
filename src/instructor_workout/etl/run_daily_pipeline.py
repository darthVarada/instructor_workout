from __future__ import annotations
from dotenv import load_dotenv

load_dotenv()

def main() -> None:
    # Imports dentro da função para evitar problemas se algum módulo
    # depender de env vars que ainda não foram carregadas.
    from instructor_workout.etl.ingestion.hevy_ingest_incremental_minio import (
        main as ingest_hevy_incremental,
    )
    from instructor_workout.etl.ingestion.kaggle_ingest_minio import (
        ingest_kaggle_gym_exercises,
    )
    from instructor_workout.etl.processing.gym_exercises_bronze_to_silver import (
        bronze_to_silver_gym_exercises,
    )
    from instructor_workout.etl.ingestion.user_profiles_mongo_to_minio import (
        ingest_user_profiles_mongo_to_minio,
    )
    from instructor_workout.etl.processing.user_profiles_bronze_to_silver import (
        bronze_to_silver_user_profiles,
    )
    from instructor_workout.etl.processing.upload_silver_to_minio import (
        main as upload_silver_to_minio,
    )

    # 1) Hevy → Bronze
    print("=== Rodando ingestão Hevy → MinIO (Bronze) ===")
    ingest_hevy_incremental()

    # 2) Kaggle Gym Exercises → Bronze
    print("=== Rodando ingestão Kaggle Gym Exercises → MinIO (Bronze) ===")
    ingest_kaggle_gym_exercises()

    # 3) Gym Exercises Bronze → Silver
    print("=== Rodando transformação Gym Exercises Bronze → Silver ===")
    bronze_to_silver_gym_exercises()

    # 4) User Profiles (Mongo) → Bronze
    print("=== Rodando ingestão User Profiles (Mongo) → MinIO (Bronze) ===")
    ingest_user_profiles_mongo_to_minio()

    # 5) User Profiles Bronze → Silver
    print("=== Rodando transformação User Profiles Bronze → Silver ===")
    bronze_to_silver_user_profiles()

    # 6) Upload Silver (workouts / outros artefatos locais) → MinIO
    print("=== Rodando upload de Silver para MinIO (workouts etc.) ===")
    upload_silver_to_minio()

    print("✅ Pipeline diário concluído.")


if __name__ == "__main__":
    main()
