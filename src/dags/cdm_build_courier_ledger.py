from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dwh_helpers import DWH_CONN_ID, load_sql

with DAG(
    dag_id="cdm_courier_ledger",
    description="Витрина расчётов с курьерами за предыдущий месяц",
    default_args={
        "owner": "de-project",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["cdm", "courier_api"],
) as dag:

    build = PostgresOperator(
        task_id="build_dm_courier_ledger",
        postgres_conn_id=DWH_CONN_ID,
        sql=load_sql("cdm_build_courier_ledger.sql"),
    )

    build
