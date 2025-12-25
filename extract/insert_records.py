import os
import psycopg2
from psycopg2.extras import Json
import time

def connect_to_db():
    print("Connecting to Postgres...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=5432,
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        print("Connected to Postgres successfully.")
        return conn
    except psycopg2.Error as e:
        raise RuntimeError(f"Database connection failed: {e}")


def create_tables(conn):
    print("Creating raw tables...")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;

        CREATE TABLE IF NOT EXISTS raw.competitions (
            competition_id INT PRIMARY KEY,
            name TEXT,
            code TEXT,
            type TEXT,
            plan TEXT,
            area_id INT,
            area_name TEXT,
            season_start DATE,
            season_end DATE,
            current_matchday INT,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS raw.matches (
            match_id INT PRIMARY KEY,
            competition_id INT,
            season_id INT,
            home_team_id INT,
            away_team_id INT,
            utc_date TIMESTAMP,
            status TEXT,
            matchday INT,
            stage TEXT,
            group_name TEXT,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS raw.teams (
            team_id INT PRIMARY KEY,
            name TEXT,
            short_name TEXT,
            tla TEXT,
            area_name TEXT,
            venue TEXT,
            founded INT,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    print("Tables ready.")

def insert_records(conn, data):
    print("Inserting competition records...")
    cursor = conn.cursor()

    competitions = data.get("competitions", [])

    for comp in competitions:
        season = comp.get("currentSeason") or {}

        cursor.execute("""
            INSERT INTO raw.competitions (
                competition_id,
                name,
                code,
                type,
                plan,
                area_id,
                area_name,
                season_start,
                season_end,
                current_matchday,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (competition_id) DO NOTHING;
        """, (
            comp.get("id"),
            comp.get("name"),
            comp.get("code"),
            comp.get("type"),
            comp.get("plan"),
            (comp.get("area") or {}).get("id"),
            (comp.get("area") or {}).get("name"),
            season.get("startDate"),
            season.get("endDate"),
            season.get("currentMatchday"),
            Json(comp)
        ))

    conn.commit()
    print(f"{len(competitions)} records inserted.")

def insert_matches_from_competitions(conn, competitions_data):
    cursor = conn.cursor()
    competitions = competitions_data.get("competitions", [])

    for comp in competitions:
        comp_id = comp.get("id")
        if not comp_id:
            continue

        try:
            data = fetch_matches_by_competition(comp_id)
            matches = data.get("matches", [])

            for match in matches:
                cursor.execute("""
                    INSERT INTO raw.matches (
                        match_id,
                        competition_id,
                        season_id,
                        home_team_id,
                        away_team_id,
                        utc_date,
                        status,
                        matchday,
                        stage,
                        group_name,
                        raw_payload
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (match_id) DO NOTHING;
                """, (
                    match.get("id"),
                    (match.get("competition") or {}).get("id"),
                    (match.get("season") or {}).get("id"),
                    (match.get("homeTeam") or {}).get("id"),
                    (match.get("awayTeam") or {}).get("id"),
                    match.get("utcDate"),
                    match.get("status"),
                    match.get("matchday"),
                    match.get("stage"),
                    match.get("group"),
                    Json(match)
                ))

            conn.commit()
            time.sleep(8)

        except Exception as e:
            print(f"Skipping matches for {comp_id}: {e}")
    
def insert_teams_from_competitions(conn, competitions_data):
    print("Fetching teams per competition...")
    cursor = conn.cursor()

    competitions = competitions_data.get("competitions", [])

    for comp in competitions:
        comp_id = comp.get("id")
        if not comp_id:
            continue

        try:
            teams_data = fetch_teams_by_competition(comp_id)
            teams = teams_data.get("teams", [])

            for team in teams:
                cursor.execute("""
                    INSERT INTO raw.teams (
                        team_id,
                        name,
                        short_name,
                        tla,
                        area_name,
                        venue,
                        founded,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO NOTHING;
                """, (
                    team.get("id"),
                    team.get("name"),
                    team.get("shortName"),
                    team.get("tla"),
                    (team.get("area") or {}).get("name"),
                    team.get("venue"),
                    team.get("founded"),
                    Json(team)
                ))

            conn.commit()
            print(f"Inserted teams for competition {comp_id}")

            time.sleep(8)  # ⏱ rate limit protection

        except Exception as e:
            print(f"Skipping competition {comp_id}: {e}")

from fetch_football_data import (
    fetch_competitions,
    fetch_matches_by_competition,
    fetch_teams_by_competition
)

def main():
    conn = None
    try:
        conn = connect_to_db()
        create_tables(conn)

        competitions_data = fetch_competitions()
        insert_records(conn, competitions_data)
        insert_matches_from_competitions(conn, competitions_data)
        insert_teams_from_competitions(conn, competitions_data)

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
