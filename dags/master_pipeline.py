from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="master_pl_pipeline",
    default_args=default_args,
    description="Master DAG that orchestrates the full pipeline",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["soccer", "master"],
) as dag:

    ingest_matches = TriggerDagRunOperator(
        task_id="trigger_ingest_matches",
        trigger_dag_id="ingest_pl_matches",
        wait_for_completion=True,
        poke_interval=30,
    )

    ingest_standings = TriggerDagRunOperator(
        task_id="trigger_ingest_standings",
        trigger_dag_id="ingest_pl_standings",
        wait_for_completion=True,
        poke_interval=30,
    )

    ingest_scorers = TriggerDagRunOperator(
        task_id="trigger_ingest_scorers",
        trigger_dag_id="ingest_pl_scorers",
        wait_for_completion=True,
        poke_interval=30,
    )

    ingest_teams = TriggerDagRunOperator(
        task_id="trigger_ingest_teams",
        trigger_dag_id="ingest_pl_teams",
        wait_for_completion=True,
        poke_interval=30,
    )

    run_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transforms",
        trigger_dag_id="run_dbt_transforms",
        wait_for_completion=True,
        poke_interval=30,
    )

    [ingest_matches, ingest_standings, ingest_scorers, ingest_teams] >> run_dbt