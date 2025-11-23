# src/instructor_workout/etl/ingestion/hevy_ingest_incremental.py

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from instructor_workout.etl.ingestion.hevy_api_client import (
    fetch_all_workouts,
    API_KEY,
)
from instructor_workout.etl.utils.minio_setup import (
    ensure_buckets_exist,
    get_s3_client,
)


# Bucket bronze e paths
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "bronze")

# raiz do projeto (…/instructor_workout)
BASE_DIR = Path(__file__).resolve().parents[4]
META_DIR = BASE_DIR / "data" / "bronze" / "hevy"
META_DIR.mkdir(parents=True, exist_ok=True)
META_PATH = META_DIR / "last_sync.json"


# -------------------------------------------------------
# Helpers de estado incremental
# -------------------------------------------------------
def _load_last_sync() -> datetime | None:
    if not META_PATH.exists():
        return None
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("last_sync")
        if ts:
            return datetime.fromisoformat(ts)
    except Exception:
        return None
    return None


def _save_last_sync(dt: datetime) -> None:
    META_DIR.mkdir(parents=True, exist_ok=True)
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_sync": dt.isoformat()}, f, indent=2, ensure_ascii=False)


def _user_hash_from_key(key: str) -> str:
    # só pra não expor a key inteira
    k = (key or "").strip().replace("-", "")
    return k[:12] or "anon"


# -------------------------------------------------------
# Escrita no MinIO (bronze/raw)
# -------------------------------------------------------
def _write_bronze_jsonl_to_minio(
    workouts: List[Dict[str, Any]], user_id: str
) -> str:
    """
    Escreve JSONL em:
      s3://bronze/hevy/workouts/raw/user=<user>/dt=YYYYMMDD/workouts_<ts>.jsonl
    """
    if not workouts:
        raise ValueError("Nenhum workout para escrever no bronze.")

    s3 = get_s3_client()

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    key = f"hevy/workouts/raw/user={user_id}/dt={date_str}/workouts_{ts}.jsonl"

    lines = [json.dumps(w, ensure_ascii=False) for w in workouts]
    body = "\n".join(lines).encode("utf-8")

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    return key


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    print("\n=== 🚀 Ingestão incremental Hevy → Bronze (MinIO) ===\n")

    if not API_KEY:
        print("❌ HEVY_API_KEY não configurada. Abandonando ingestão.")
        return

    # garante buckets bronze/silver/gold
    ensure_buckets_exist()

    user_id = _user_hash_from_key(API_KEY)

    last_sync = _load_last_sync()
    print(f"🔁 last_sync atual: {last_sync!r}")

    print("\n→ Buscando workouts na Hevy API...")
    workouts = fetch_all_workouts(since=last_sync)

    if not workouts:
        print("Nenhum workout novo desde o último sync. Nada a fazer. ✅")
        return

    print(f"📦 Workouts novos encontrados: {len(workouts)}")

    # grava no bronze
    key = _write_bronze_jsonl_to_minio(workouts, user_id)
    print(f"\n✓ Bronze salvo em: s3://{BRONZE_BUCKET}/{key}")

    # atualiza last_sync com o MAIOR updated_at dos dados recebidos
    def _parse_dt(x: str) -> datetime:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))

    latest_updated = max(
        _parse_dt(w.get("updated_at", datetime.now(timezone.utc).isoformat()))
        for w in workouts
    )
    _save_last_sync(latest_updated)

    print(f"🕒 last_sync atualizado para: {latest_updated.isoformat()}")
    print("\n✅ Ingestão incremental finalizada.\n")


if __name__ == "__main__":
    main()
