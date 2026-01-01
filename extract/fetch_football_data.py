import os
import time
import requests
from typing import Optional, Dict

BASE_URL = "https://api.football-data.org/v4"

HEADERS = {
    "X-Auth-Token": os.getenv("FOOTBALL_API_KEY")
}

REQUEST_DELAY = 15        
RETRY_AFTER_429 = 60      
MAX_RETRY = 2


def fetch_data(endpoint: str, params: Optional[Dict] = None):
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(1, MAX_RETRY + 1):
        print(f"[FETCH] {endpoint} (attempt {attempt})")

        response = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=30
        )

        if response.status_code == 429:
            print("⚠️ 429 Rate limit hit → sleeping 60s")
            time.sleep(RETRY_AFTER_429)
            continue

        response.raise_for_status()

        time.sleep(REQUEST_DELAY)  # ⬅️ KUNCI UTAMA
        return response.json()

    raise RuntimeError(f"Failed fetching {endpoint} after {MAX_RETRY} retries")


def fetch_competitions():
    return fetch_data("competitions")


def fetch_matches_by_competition(competition_id: int):
    return fetch_data(f"competitions/{competition_id}/matches")


def fetch_teams_by_competition(competition_id: int):
    return fetch_data(f"competitions/{competition_id}/teams")
