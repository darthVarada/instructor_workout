import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import zoneinfo
from instructor_workout.etl.ingestion.hevy_api_client import fetch_all_workouts, API_KEY


# Diretórios locais (pode depois trocar para MinIO)
BASE_DIR = Path(__file__).resolve().parents[4]  # sobe até raiz do repo
BRONZE_DIR = BASE_DIR / "data" / "bronze" / "hevy"
META_PATH = BRONZE_DIR / "last_sync.json"


BR_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

def _parse_dt(x: str) -> datetime:
    """
    Converte timestamp UTC da Hevy para timezone do Brasil.
    Exemplo de entrada: 2025-11-18T22:04:26.389Z
    """
    # Remover 'Z' e interpretar como UTC
    dt_utc = datetime.fromisoformat(x.replace("Z", "+00:00"))

    # Converter para Brasil (GMT-3 ou GMT-2 no verão — automático)
    return dt_utc.astimezone(BR_TZ)


def load_last_sync() -> datetime | None:
    if not META_PATH.exists():
        return None
    data = json.loads(META_PATH.read_text(encoding="utf-8"))
    ts = data.get("last_updated_at")
    if not ts:
        return None
    return datetime.fromisoformat(ts)


def save_last_sync(dt: datetime) -> None:
    META_PATH.parent.mkdir(parents=True, exist_ok=True)
    META_PATH.write_text(
        json.dumps({"last_updated_at": dt.isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def filter_new_workouts(
    workouts: List[Dict[str, Any]], last_sync: datetime | None
) -> List[Dict[str, Any]]:
    if last_sync is None:
        return workouts

    new_list = []
    for w in workouts:
        updated_at = w.get("updated_at") or w.get("created_at")
        if not updated_at:
            # Se não tiver campo, consideramos novo
            new_list.append(w)
            continue

        dt = _parse_dt(updated_at)
        if dt > last_sync:
            new_list.append(w)

    return new_list


def write_bronze_jsonl(workouts: List[Dict[str, Any]], user_id: str) -> Path:
    """
    Salva um arquivo JSONL com os workouts novos.
    """
    if not workouts:
        raise ValueError("Nenhum workout novo para salvar.")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = BRONZE_DIR / f"workouts_{user_id}_{ts}.jsonl"

    with path.open("w", encoding="utf-8") as f:
        for w in workouts:
            json.dump(w, f, ensure_ascii=False)
            f.write("\n")

    return path


def main():
    print("=== Hevy incremental ingestion → Bronze ===")

    user_id = (API_KEY or "").strip()
    if not user_id:
        print("ERRO: HEVY_API_KEY não definida.")
        return

    print(f"user_id lógico (API_KEY mascarada): {user_id[:4]}...{user_id[-4:]}")

    last_sync = load_last_sync()
    if last_sync:
        print(f"Último last_sync: {last_sync.isoformat()}")
    else:
        print("Nenhum last_sync encontrado – primeira carga completa.")

    print("\n→ Buscando workouts na Hevy API...")
    workouts = fetch_all_workouts(limit=100)
    print(f"Total retornado pela API: {len(workouts)}")

    print("\n→ Filtrando apenas novos (incremental)...")
    new_workouts = filter_new_workouts(workouts, last_sync)
    print(f"Novos workouts a salvar: {len(new_workouts)}")

    if not new_workouts:
        print("Nenhum workout novo. Nada a fazer.")
        return

    # Atualiza last_sync com o maior updated_at
    max_dt: datetime | None = None
    for w in new_workouts:
        updated_at = w.get("updated_at") or w.get("created_at")
        if not updated_at:
            continue
        dt = _parse_dt(updated_at)
        if (max_dt is None) or (dt > max_dt):
            max_dt = dt

    if max_dt is None:
        print("Aviso: não encontrei updated_at/created_at nos novos registros.")
    else:
        print(f"Novo last_sync calculado: {max_dt.isoformat()}")

    # Grava Bronze
    bronze_path = write_bronze_jsonl(new_workouts, user_id=user_id[:8])
    print(f"\n✓ Bronze salvo em: {bronze_path}")

    # Salva metadata
    if max_dt is not None:
        save_last_sync(max_dt)
        print(f"✓ Metadata atualizada em: {META_PATH}")

    print("\n✅ FIM: ingestão incremental concluída.")


if __name__ == "__main__":
    main()
