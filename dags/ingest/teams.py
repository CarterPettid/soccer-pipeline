from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import FootballDataClient
from utils.db_utils import create_teams_table, upsert_team

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def create_table():
    create_teams_table()

def ingest_teams():
    client = FootballDataClient()
    teams = client.get_teams(competition="PL", season=2024)
    
    for team in teams:
        team_data = {
            "team_id": team["id"],
            "team_name": team["name"],
            "short_name": team.get("shortName"),
            "tla": team.get("tla"),
            "crest_url": team.get("crest"),
            "address": team.get("address"),
            "website": team.get("website"),
            "founded": team.get("founded"),
            "venue": team.get("venue"),
        }
        upsert_team(team_data)
    
    print(f"Ingested {len(teams)} teams")

with DAG(
    dag_id="ingest_pl_teams",
    default_args=default_args,
    description="Ingest Premier League teams",
    schedule_interval="@weekly",
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
        task_id="ingest_teams",
        python_callable=ingest_teams,
    )
    
    task_create_table >> task_ingest