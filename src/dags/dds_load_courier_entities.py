from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dwh_helpers import DWH_CONN_ID, load_sql

CDM_DAG_ID = "cdm_courier_ledger"

DEFAULT_ARGS = {
    "owner": "de-project",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dds_courier",
    description="Наполнение DDS-слоя: курьеры, заказы, факты доставок",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dds", "courier_api"],
) as dag:

    dm_couriers = PostgresOperator(
        task_id="load_dm_couriers",
        postgres_conn_id=DWH_CONN_ID,
        sql=load_sql("dds_upsert_couriers.sql"),
    )

    dm_orders = PostgresOperator(
        task_id="load_dm_orders",
        postgres_conn_id=DWH_CONN_ID,
        sql=load_sql("dds_upsert_orders.sql"),
    )

    fct_deliveries = PostgresOperator(
        task_id="load_fct_deliveries",
        postgres_conn_id=DWH_CONN_ID,
        sql=load_sql("dds_upsert_fct_deliveries.sql"),
    )

    trigger_cdm = TriggerDagRunOperator(
        task_id="trigger_cdm_courier_ledger",
        trigger_dag_id=CDM_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    dm_couriers >> [dm_orders, fct_deliveries] >> trigger_cdm
