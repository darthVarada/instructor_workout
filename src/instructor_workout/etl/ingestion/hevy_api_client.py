# src/instructor_workout/etl/ingestion/hevy_api_client.py

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

API_BASE = "https://api.hevyapp.com"
API_KEY = (os.getenv("HEVY_API_KEY") or "").strip()


def _mask_key(k: str) -> str:
    if not k:
        return "(vazia)"
    if len(k) <= 8:
        return "***"
    return f"{k[:4]}...{k[-4:]}"


def _parse_dt(x: str) -> datetime:
    # Exemplo: "2025-11-18T22:04:26.389Z"
    return datetime.fromisoformat(x.replace("Z", "+00:00"))


def fetch_all_workouts(since: datetime | None = None) -> List[Dict[str, Any]]:
    """
    Busca todos os workouts da Hevy, paginando, e aplica filtro incremental
    em memória usando o campo `updated_at`.

    :param since: se informado, retorna apenas updated_at > since (UTC).
    """
    if not API_KEY:
        raise RuntimeError(
            "HEVY_API_KEY não definida. Configure a variável de ambiente antes de rodar a ingestão."
        )

    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
    }

    workouts: List[Dict[str, Any]] = []

    page = 1
    limit = 100  # bom equilíbrio
    print(f"→ Chamando Hevy API com page={page}, limit={limit}")
    with httpx.Client(timeout=30.0) as client:
        while True:
            resp = client.get(
                f"{API_BASE}/v1/workouts",
                params={"page": page, "limit": limit},
                headers=headers,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Erro na Hevy API (status={resp.status_code}): {resp.text[:500]}"
                )

            data = resp.json()
            page_count = int(data.get("page_count", 1))
            current_page = int(data.get("page", page))

            page_workouts = data.get("workouts", []) or []
            print(
                f"  - Página {current_page}/{page_count} → {len(page_workouts)} workouts brutos"
            )

            for w in page_workouts:
                if since is not None:
                    try:
                        updated_at = _parse_dt(w["updated_at"])
                    except Exception:
                        # Se der algum problema no parse, mantemos o registro
                        workouts.append(w)
                        continue

                    if updated_at <= since:
                        # velho, ignora
                        continue

                workouts.append(w)

            if current_page >= page_count:
                break

            page += 1

    print(f"✓ Total de workouts retornados (após filtro incremental): {len(workouts)}")
    print(f"  API key usada (mascarada): {_mask_key(API_KEY)}")
    return workouts
