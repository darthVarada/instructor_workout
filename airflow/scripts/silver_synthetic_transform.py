import os
import pandas as pd
import numpy as np
import tempfile
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

BASE_KEY = "bronze/raw/synthetic_realistic_workout_base/base_full.parquet"
INCREMENTAL_PREFIX = "bronze/raw/synthetic_realistic_workout/"
SILVER_KEY = "silver/synthetic_realistic_workout/merged.parquet"


def _normalize_list(value):
    """Garante que exercises/sets virem lista de dict, mesmo se vierem como np.array ou None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, np.ndarray):
        return list(value)
    # fallback conservador
    return []


def explode_hevy_incremental(df_raw: pd.DataFrame, hevy_user_id: str) -> pd.DataFrame:
    """
    Converte o formato bruto da API (treino + exercises + sets)
    para o mesmo grão da base sintética: 1 linha por set.
    """
    rows = []

    for _, w in df_raw.iterrows():
        # 🔹 garante que start/end já vêm como datetime
        start = pd.to_datetime(w.get("start_time"), errors="coerce", utc=True)
        end = pd.to_datetime(w.get("end_time"), errors="coerce", utc=True)

        # remove timezone (fica igual à base)
        if isinstance(start, pd.Timestamp) and start.tzinfo is not None:
            start = start.tz_convert(None)
        if isinstance(end, pd.Timestamp) and end.tzinfo is not None:
            end = end.tz_convert(None)

        exercises = _normalize_list(w.get("exercises"))

        for ex in exercises:
            if not isinstance(ex, dict):
                continue

            sets = _normalize_list(ex.get("sets"))

            for s in sets:
                if not isinstance(s, dict):
                    continue

                distance_meters = s.get("distance_meters")
                distance_km = (
                    distance_meters / 1000.0
                    if distance_meters is not None
                    else None
                )

                row = {
                    "user_id": hevy_user_id,              # 👈 sempre o user da API

                    "title": w.get("title"),
                    "start_time": start,                   # 👈 já datetime
                    "end_time": end,                       # 👈 já datetime
                    "description": w.get("description"),

                    "exercise_title": ex.get("title"),
                    "superset_id": ex.get("superset_id"),
                    "exercise_notes": ex.get("notes"),

                    "set_index": s.get("index"),
                    "set_type": s.get("type"),
                    "weight_kg": s.get("weight_kg"),
                    "reps": s.get("reps"),
                    "distance_km": distance_km,
                    "duration_seconds": s.get("duration_seconds"),
                    "rpe": s.get("rpe"),

                    "id": w.get("id"),
                    "routine_id": w.get("routine_id"),
                    "updated_at": w.get("updated_at"),
                    "created_at": w.get("created_at"),

                    "exercises": w.get("exercises"),
                }

                rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "user_id",
                "title",
                "start_time",
                "end_time",
                "description",
                "exercise_title",
                "superset_id",
                "exercise_notes",
                "set_index",
                "set_type",
                "weight_kg",
                "reps",
                "distance_km",
                "duration_seconds",
                "rpe",
                "id",
                "routine_id",
                "updated_at",
                "created_at",
                "exercises",
            ]
        )

    return pd.DataFrame(rows)



def ingest_and_merge_silver():
    print("\n=== 🚀 Criando Silver Synthetic Realistic Workout ===")

    s3 = get_s3()
    tmp = tempfile.mkdtemp()

    # ============================
    # 0) USER ID DA API (HEVY_API_KEY)
    # ============================
    hevy_user_id = os.getenv("HEVY_API_KEY")
    if not hevy_user_id:
        raise RuntimeError("❌ HEVY_API_KEY não definida no ambiente!")

    print(f"✅ HEVY_API_KEY detectada → user_id para API: {hevy_user_id}")

    # ============================
    # 1) BASE (já no grão 'set')
    # ============================
    base_path = os.path.join(tmp, "base.parquet")
    print("⬇️ Baixando base...")
    s3.download_file(BUCKET, BASE_KEY, base_path)
    df_base = pd.read_parquet(base_path)
    print(f"✅ Base carregada: {len(df_base)} linhas")

    # ============================
    # 2) INCREMENTAIS (API HEVY)
    # ============================
    print("⬇️ Buscando incrementais da API...")
    paginator = s3.get_paginator("list_objects_v2")

    dfs_inc = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=INCREMENTAL_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # ignora a base se estiver no mesmo prefixo
            if not key.endswith(".parquet") or key.endswith("base_full.parquet"):
                continue

            print(f"  • Encontrado incremental bruto: {key}")
            local = os.path.join(tmp, os.path.basename(key))
            s3.download_file(BUCKET, key, local)
            df_raw = pd.read_parquet(local)

            # transforma o formato bruto em grão de set
            df_inc = explode_hevy_incremental(df_raw, hevy_user_id)
            print(f"    → Incremental transformado: {len(df_inc)} linhas")
            dfs_inc.append(df_inc)

    if dfs_inc:
        df_inc_all = pd.concat(dfs_inc, ignore_index=True)
        print(f"✅ Incrementais (explodidos) carregados: {len(df_inc_all)} linhas")

        # Alinha colunas base x inc (caso alguma diferença)
        all_cols = list(dict.fromkeys(list(df_base.columns) + list(df_inc_all.columns)))
        df_base = df_base.reindex(columns=all_cols)
        df_inc_all = df_inc_all.reindex(columns=all_cols)

        df_all = pd.concat([df_base, df_inc_all], ignore_index=True)
    else:
        print("⚠️ Nenhum incremental encontrado — usando apenas a base.")
        df_all = df_base

    # ============================
    # 3) CONVERSÕES DE DATA
    # ============================
    print("🕒 Convertendo datas...")
    if "start_time" in df_all.columns:
        df_all["start_time"] = pd.to_datetime(df_all["start_time"], errors="coerce")
    if "end_time" in df_all.columns:
        df_all["end_time"] = pd.to_datetime(df_all["end_time"], errors="coerce")

    # ============================
    # 4) DURAÇÃO FINAL
    # ============================
    if "start_time" in df_all.columns and "end_time" in df_all.columns:
        print("⏱️ Calculando duração...")
        df_all["duracao"] = (
            (df_all["end_time"] - df_all["start_time"]).dt.total_seconds() / 60
        )

        # fallback se vier nulo e existir duration_seconds
        if "duration_seconds" in df_all.columns:
            df_all["duracao"] = df_all["duracao"].fillna(
                df_all["duration_seconds"] / 60
            )

    # ============================
    # 5) NOVA COLUNA id_exercice
    # ============================
    if "id" in df_all.columns:
        df_all["id_exercice"] = df_all["id"].astype("string")
        print("✅ Coluna 'id_exercice' criada a partir de 'id'")
    else:
        print("⚠️ Coluna 'id' não existe em df_all, não foi possível criar 'id_exercice'")

    # ============================
    # 6) SALVANDO SILVER
    # ============================
    silver_path = os.path.join(tmp, "silver.parquet")
    df_all.to_parquet(silver_path, index=False)

    print(f"⬆️ Enviando para S3 → s3://{BUCKET}/{SILVER_KEY}")
    s3.upload_file(silver_path, BUCKET, SILVER_KEY)

    print("✅ Silver criada com sucesso!\n")


if __name__ == "__main__":
    ingest_and_merge_silver()
