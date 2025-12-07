from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from instructor_workout.etl.utils.aws_client import get_s3
from instructor_workout.etl.ingestion.hevy_api_client import fetch_all_workouts, API_KEY

BRONZE_BUCKET = "instructor-workout-datas"

# ⭐ AJUSTE CRÍTICO: BASE_DIR não pode usar parents[4] dentro do Docker
BASE_DIR = Path("/opt/airflow")

META_DIR = BASE_DIR / "data" / "bronze" / "hevy"
META_DIR.mkdir(parents=True, exist_ok=True)
META_PATH = META_DIR / "last_sync.json"


def _load_last_sync() -> datetime | None:
    if not META_PATH.exists():
        return None
    try:
        data = json.loads(META_PATH.read_text())
        return datetime.fromisoformat(data["last_sync"])
    except Exception:
        return None


def _save_last_sync(dt: datetime) -> None:
    META_PATH.write_text(
        json.dumps({"last_sync": dt.isoformat()}, indent=2)
    )


def _user_hash_from_key(key: str) -> str:
    k = (key or "").strip().replace("-", "")
    return k[:12] or "anon"


def _write_bronze_parquet_to_s3(workouts: List[Dict[str, Any]], user_id: str) -> str:

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    key = (
        f"bronze/raw/synthetic_realistic_workout/"
        f"user={user_id}/dt={date_str}/workouts_{ts}.parquet"
    )

    table = pa.Table.from_pylist(workouts)

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    body = buf.getvalue().to_pybytes()

    s3 = get_s3()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/parquet",
    )

    return key


def main(**_kwargs):
    print("\n=== 🚀 Ingestão incremental Hevy → Bronze/Raw ===\n")

    if not API_KEY:
        print("❌ API_KEY não configurada.")
        return

    user_id = _user_hash_from_key(API_KEY)

    last_sync = _load_last_sync()
    print(f"🔁 Last sync: {last_sync}")

    workouts = fetch_all_workouts(since=last_sync)

    if not workouts:
        print("Nenhum workout novo. Nada a fazer. ✅")
        return

    key = _write_bronze_parquet_to_s3(workouts, user_id)

    print(f"✓ Bronze salvo em: s3://{BRONZE_BUCKET}/{key}")

    latest_dt = max(
        datetime.fromisoformat(w["updated_at"].replace("Z", "+00:00"))
        for w in workouts
    )
    _save_last_sync(latest_dt)

    print(f"🕒 last_sync atualizado para: {latest_dt}")
    print("\n✔ Finalizado.\n")


if __name__ == "__main__":
    main()
