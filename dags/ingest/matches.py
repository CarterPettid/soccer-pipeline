from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import FootballDataClient
from utils.db_utils import create_matches_table, upsert_match

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def create_table():
    create_matches_table()

def ingest_matches():
    client = FootballDataClient()
    matches = client.get_matches(competition="PL", season=2024)
    
    for match in matches:
        match_data = {
            "match_id": match["id"],
            "competition": "PL",
            "season": 2024,
            "matchday": match.get("matchday"),
            "status": match["status"],
            "utc_date": match["utcDate"],
            "home_team_id": match["homeTeam"]["id"],
            "home_team_name": match["homeTeam"]["name"],
            "away_team_id": match["awayTeam"]["id"],
            "away_team_name": match["awayTeam"]["name"],
            "home_score": match["score"]["fullTime"]["home"],
            "away_score": match["score"]["fullTime"]["away"],
            "winner": match["score"]["winner"],
        }
        upsert_match(match_data)
    
    print(f"Ingested {len(matches)} matches")

with DAG(
    dag_id="ingest_pl_matches",
    default_args=default_args,
    description="Ingest Premier League match data daily",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["soccer", "ingest"],
) as dag:
    
    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )
    
    task_ingest = PythonOperator(
        task_id="ingest_matches",
        python_callable=ingest_matches,
    )
    
    task_create_table >> task_ingest