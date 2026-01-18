from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import FootballDataClient
from utils.db_utils import create_standings_table, upsert_standing

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def create_table():
    create_standings_table()

def ingest_standings():
    client = FootballDataClient()
    data = client.get_standings(competition="PL", season=2024)
    
    # API returns multiple standing types, we want "TOTAL"
    total_standings = None
    for standing in data:
        if standing["type"] == "TOTAL":
            total_standings = standing["table"]
            break
    
    if not total_standings:
        raise ValueError("Could not find TOTAL standings")
    
    season_info = client.get_matches(competition="PL", season=2024)
    current_matchday = max(m.get("matchday", 0) for m in season_info if m.get("matchday"))
    
    for team in total_standings:
        standing_data = {
            "competition": "PL",
            "season": 2024,
            "matchday": current_matchday,
            "team_id": team["team"]["id"],
            "team_name": team["team"]["name"],
            "position": team["position"],
            "played": team["playedGames"],
            "won": team["won"],
            "draw": team["draw"],
            "lost": team["lost"],
            "goals_for": team["goalsFor"],
            "goals_against": team["goalsAgainst"],
            "goal_difference": team["goalDifference"],
            "points": team["points"],
        }
        upsert_standing(standing_data)
    
    print(f"Ingested standings for {len(total_standings)} teams")

with DAG(
    dag_id="ingest_pl_standings",
    default_args=default_args,
    description="Ingest Premier League standings daily",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["soccer", "ingest"],
) as dag:
    
    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )
    
    task_ingest = PythonOperator(
        task_id="ingest_standings",
        python_callable=ingest_standings,
    )
    
    task_create_table >> task_ingest