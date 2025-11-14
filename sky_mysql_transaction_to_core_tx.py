from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# This DAG assumes you have TWO Connections in Airflow:
#
# 1) MySQL source:
#    Conn Id: db2
#    Conn Type: MySQL
#    Host, Schema, Login, Password, Port = your MySQL details
#
# 2) Postgres target:
#    Conn Id: lake
#    Conn Type: Postgres
#    Host, Schema, Login, Password, Port = your Postgres details
#
# And a target table in Postgres, for example:
#   core.wp_transactions (cpf TEXT, points NUMERIC)


with DAG(
    dag_id="sky_mysql_transaction_to_core_tx",
    description="Example DAG: extract from MySQL and load into Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # or None for manual only
    catchup=False,
    tags=["example", "mysql", "postgres", "etl"],
) as dag:

    @task(task_id="extract_from_mysql")
    def extract_from_mysql():
        """
        Read data from MySQL (source).
        Adjust the SQL and connection id (db2) to your needs.
        """
        mysql_hook = MySqlHook(mysql_conn_id="db2")

        sql = "SELECT id source_tx_id, cpf customer_id, codeword, points amount, 'SKY' source_system, ip , 'points' currency  FROM wp_transactions LIMIT 10;"
        rows = mysql_hook.get_records(sql)

        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        logger.info("Fetched %d rows from MySQL", len(rows))
        for row in rows:
            logger.info("MySQL row: %s", row)

        # For small datasets, returning the rows via XCom is fine
        return rows

    @task(task_id="load_into_postgres")
    def load_into_postgres(rows: list[tuple]):
        """
        Load data into Postgres (target).
        Assumes a table core.wp_transactions(cpf, points).
        """
        pg_hook = PostgresHook(postgres_conn_id="lake")

        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        if not rows:
            logger.info("No rows to load into Postgres.")
            return

        logger.info("Loading %d rows into Postgres core.wp_transactions", len(rows))

        # Insert rows. Make sure the target_fields match the order of columns
        # returned by your MySQL query.
        
        pg_hook.insert_rows(
            table="core.tx",
            rows=rows,
            target_fields=["source_tx_id", "customer_id", "codeword", "amount", "source_system", "ip", "currency"],
        )

        logger.info("Finished loading rows into Postgres.")

    # Task dependency: extract -> load
    extracted_rows = extract_from_mysql()
    load_into_postgres(extracted_rows)
