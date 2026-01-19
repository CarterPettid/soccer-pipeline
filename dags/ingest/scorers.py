from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import FootballDataClient
from utils.db_utils import create_scorers_table, upsert_scorer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def create_table():
    create_scorers_table()

def ingest_scorers():
    client = FootballDataClient()
    scorers = client.get_scorers(competition="PL", season=2024, limit=50)
    
    for scorer in scorers:
        scorer_data = {
            "competition": "PL",
            "season": 2024,
            "player_id": scorer["player"]["id"],
            "player_name": scorer["player"]["name"],
            "nationality": scorer["player"].get("nationality"),
            "team_id": scorer["team"]["id"],
            "team_name": scorer["team"]["name"],
            "goals": scorer.get("goals", 0),
            "assists": scorer.get("assists", 0),
            "penalties": scorer.get("penalties", 0),
        }
        upsert_scorer(scorer_data)
    
    print(f"Ingested {len(scorers)} top scorers")

with DAG(
    dag_id="ingest_pl_scorers",
    default_args=default_args,
    description="Ingest Premier League top scorers daily",
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
        task_id="ingest_scorers",
        python_callable=ingest_scorers,
    )
    
    task_create_table >> task_ingest