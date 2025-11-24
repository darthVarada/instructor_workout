# src/instructor_workout/etl/run_daily_pipeline.py

from instructor_workout.etl.ingestion.hevy_ingest_incremental_minio import main as ingest_main
from instructor_workout.etl.processing.upload_silver_to_minio import main as upload_silver_main


def main():
    print("=== Rodando ingestão Hevy → MinIO (Bronze) ===")
    ingest_main()

    print("\n=== Subindo Silver para MinIO ===")
    upload_silver_main()

    print("\n✅ Pipeline diário concluído.")


if __name__ == "__main__":
    main()
