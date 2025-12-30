import os
import psycopg2
from psycopg2.extras import Json
import time

#load data
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

        CREATE TABLE raw.competitions (
            id INT PRIMARY KEY,
            name TEXT,
            code TEXT,
            type TEXT,
            plan TEXT,
            area_id INT,
            area_name TEXT,
            season_id INT,
            season_start DATE,
            season_end DATE,
            emblem TEXT,
            last_updated TIMESTAMP,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE raw.matches (
            id INT PRIMARY KEY,
            date TIMESTAMP,
            area_id INT,
            competition_id INT,
            season_id INT,
            status TEXT,
            minute INT,
            injury_time INT,
            attendance INT,
            stage TEXT,
            group_name TEXT,
            home_team JSONB,
            away_team JSONB,
            score JSONB,
            goals JSONB,
            penalties JSONB,
            referees JSONB,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE raw.teams (
            id INT PRIMARY KEY,
            comp_id INT[],
            name TEXT,
            code TEXT,
            flag TEXT,
            team_emblem TEXT,
            founded INT,
            venue TEXT,
            website TEXT,
            club_colors TEXT,
            address TEXT,
            last_updated TIMESTAMP,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE raw.players (
            id INT PRIMARY KEY,
            name TEXT,
            first_name TEXT,
            last_name TEXT,
            date_of_birth DATE,
            nationality TEXT,
            position TEXT,
            current_team JSONB,
            shirt_number INT,
            contract JSONB,
            last_updated TIMESTAMP,
            raw_payload JSONB,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    print("Tables ready.")

def insert_competitions(conn, data):
    cursor = conn.cursor()
    for comp in data.get("competitions", []):
        season = comp.get("currentSeason") or {}
        cursor.execute("""
            INSERT INTO raw.competitions
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            comp.get("id"),
            comp.get("name"),
            comp.get("code"),
            comp.get("type"),
            comp.get("plan"),
            comp["area"]["id"],
            comp["area"]["name"],
            season.get("id"),
            season.get("startDate"),
            season.get("endDate"),
            comp.get("emblem"),
            comp.get("lastUpdated"),
            Json(comp)
        ))
    conn.commit()

def insert_matches(conn, competitions):
    cursor = conn.cursor()
    for comp in competitions:
        comp_id = comp["id"]
        try:
            data = fetch_matches_by_competition(comp_id)
            for match in data.get("matches", []):
                cursor.execute("""
                    INSERT INTO raw.matches
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    match.get("id"),
                    match.get("utcDate"),
                    match["area"]["id"] if match.get("area") else None,
                    comp_id,
                    match["season"]["id"],
                    match.get("status"),
                    match.get("minute"),
                    match.get("injuryTime"),
                    match.get("attendance"),
                    match.get("stage"),
                    match.get("group"),
                    Json(match.get("homeTeam")),
                    Json(match.get("awayTeam")),
                    Json(match.get("score")),
                    Json(match.get("goals")),
                    Json(match.get("penalties")),
                    Json(match)
                ))
            conn.commit()
            time.sleep(8)
        except Exception as e:
            print(f"Skip matches {comp_id}: {e}")

def insert_teams_and_players(conn, competitions):
    cursor = conn.cursor()

    for comp in competitions:
        comp_id = comp["id"]
        try:
            data = fetch_teams_by_competition(comp_id)
            for team in data.get("teams", []):
                cursor.execute("""
                    INSERT INTO raw.teams
                    VALUES (%s, ARRAY[%s], %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        comp_id = array_append(raw.teams.comp_id, %s);
                """, (
                    team["id"],
                    comp_id,
                    team["name"],
                    team["tla"],
                    team["area"]["flag"],
                    team["crest"],
                    team["founded"],
                    team["venue"],
                    team["website"],
                    team["clubColors"],
                    team["address"],
                    team["lastUpdated"],
                    comp_id
                ))

                for p in team.get("squad", []):
                    cursor.execute("""
                        INSERT INTO raw.players
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        p.get("id"),
                        p.get("name"),
                        p.get("firstName"),
                        p.get("lastName"),
                        p.get("dateOfBirth"),
                        p.get("nationality"),
                        p.get("position"),
                        Json({"team_id": team["id"]}),
                        p.get("shirtNumber"),
                        Json(p.get("contract")),
                        p.get("lastUpdated"),
                        Json(p)
                    ))
            conn.commit()
            time.sleep(8)
        except Exception as e:
            print(f"Skip teams {comp_id}: {e}")

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
        competitions = competitions_data.get("competitions", [])

        insert_competitions(conn, competitions_data)
        insert_matches(conn, competitions)
        insert_teams_and_players(conn, competitions)

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()