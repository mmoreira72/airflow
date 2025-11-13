from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="hello",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "noah", "retries": 1},
    tags=["smoke"],
):
    EmptyOperator(task_id="start") >> EmptyOperator(task_id="end")

