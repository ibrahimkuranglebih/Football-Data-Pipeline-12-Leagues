import os
import requests

BASE_URL = "https://api.football-data.org/v4"

HEADERS = {
    "X-Auth-Token": os.getenv("FOOTBALL_API_KEY")
}

def fetch_data(endpoint: str, params: dict | None = None):
    print(f"Fetching data from {endpoint}")
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        headers=HEADERS,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()

def fetch_competitions():
    return fetch_data("competitions")

def fetch_matches_by_competition(competition_id: int):
    return fetch_data(f"competitions/{competition_id}/matches")

def fetch_teams_by_competition(competition_id: int):
    return fetch_data(f"competitions/{competition_id}/teams")

