from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def copy_transactions_to_core_tx(**context):
    # 1) Connect to MySQL
    #mysql = MySqlHook(mysql_conn_id="db2")

    # 2) Connect to Postgres
    #pg = PostgresHook(postgres_conn_id="lake")

    # 3) Read all rows from MySQL.transaction
    #    TODO: adjust column list if you don't want SELECT *.
    #rows = mysql.get_records("SELECT * FROM transaction limit 10;")

    # 4) Optional: clear target table before loading
    #pg.run("TRUNCATE TABLE core.tx;")

    # 5) Insert rows into Postgres core.tx
    #    Assumes column order in core.tx matches MySQL.transaction.
    #pg.insert_rows(table="core.tx", rows=rows)


with DAG(
    dag_id="sky_mysql_transaction_to_core_tx",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # or None if you only want manual runs
    catchup=False,
    tags=["example", "sky-transactions"],
) as dag:
    copy_task = PythonOperator(
        task_id="copy_transactions",
        python_callable=copy_transactions_to_core_tx,
    )
