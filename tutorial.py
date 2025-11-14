from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="tutorial",
    description="A simple tutorial DAG",
    schedule="0 0 * * *",     # daily at midnight
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    t3 = BashOperator(
        task_id="print_hello",
        bash_command='echo "Hello World"',
    )

    t1 >> [t2, t3]
