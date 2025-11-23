import os
import requests
from typing import List, Dict, Any


API_BASE = "https://api.hevyapp.com/v1"
API_KEY = (os.getenv("HEVY_API_KEY") or "").strip()


class HevyApiError(Exception):
    pass


def _headers() -> Dict[str, str]:
    if not API_KEY:
        raise HevyApiError("HEVY_API_KEY não definida no ambiente.")
    return {
        "accept": "application/json",
        "api-key": API_KEY,  # já vimos no teste que é esse header que funciona
    }


def fetch_workouts_page(limit: int = 100, page: int = 1) -> Dict[str, Any]:
    url = f"{API_BASE}/workouts"
    params = {"limit": limit, "page": page}
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    if not resp.ok:
        raise HevyApiError(
            f"Erro na Hevy API (status={resp.status_code}): {resp.text[:500]}"
        )
    return resp.json()


def fetch_all_workouts(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Faz paginação até page_count, retornando uma lista de workouts (dicts).
    """
    workouts: List[Dict[str, Any]] = []

    first = fetch_workouts_page(limit=limit, page=1)
    workouts.extend(first.get("workouts", []))
    page_count = first.get("page_count", 1)

    for page in range(2, page_count + 1):
        data = fetch_workouts_page(limit=limit, page=page)
        workouts.extend(data.get("workouts", []))

    return workouts
