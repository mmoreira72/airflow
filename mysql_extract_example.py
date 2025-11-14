from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook


# This DAG assumes you have a Connection in Airflow called "mysql_default"
# (Admin -> Connections) pointing to your MySQL instance.
#
# Conn Id: db2
# Conn Type: MySQL
# Host, Schema, Login, Password, Port = your DB details


with DAG(
    dag_id="mysql_extract_example",
    description="Community-style example DAG: read data from MySQL",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",       # run every day; set None for manual only
    catchup=False,
    tags=["example", "mysql"],
) as dag:

    @task(task_id="extract_from_mysql")
    def extract_from_mysql():
        """
        Example task that reads from a MySQL table using MySqlHook.
        Adjust conn_id, table name and query for your case.
        """
        hook = MySqlHook(mysql_conn_id="db2")

        # Simple example query – change to your table
        sql = "SELECT cpf, points FROM wp_transactions LIMIT 10;"
        rows = hook.get_records(sql)

        # You can process rows here or pass them downstream
        # For demo, just log them:
        from airflow.utils.log.logging_mixin import LoggingMixin

        logger = LoggingMixin().log
        logger.info("Fetched %d rows from MySQL", len(rows))
        for row in rows:
            logger.info("Row: %s", row)

        # Returning small data is fine (for XComs / downstream @task)
        return rows

    extract_from_mysql()
