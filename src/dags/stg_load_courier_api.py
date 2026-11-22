from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from psycopg2.extras import Json, execute_values

from dwh_helpers import api_paged, dwh_conn, load_sql, run_params

DDS_DAG_ID = "dds_courier"

DEFAULT_ARGS = {
    "owner": "de-project",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _parse_ts(value):
    return datetime.fromisoformat(str(value).strip()) if value else None


def delivery_window(context):
    params = run_params(context)
    to_ts = _parse_ts(params.get("to_ts")) or context["data_interval_end"].replace(tzinfo=None)
    from_ts = _parse_ts(params.get("from_ts")) or (
        to_ts - timedelta(days=int(params.get("days_back") or 7))
    )
    return from_ts.strftime("%Y-%m-%d %H:%M:%S"), to_ts.strftime("%Y-%m-%d %H:%M:%S")


def load_couriers(**context):
    rows = list(api_paged("/couriers", sort_field="id"))
    if not rows:
        raise RuntimeError("GET /couriers вернул пустой список")

    with dwh_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            load_sql("stg_upsert_couriers.sql"),
            [(row["_id"], Json(row)) for row in rows],
        )

    print("stg.deliverysystem_couriers: загружено %s записей" % len(rows))
    return len(rows)


def load_deliveries(**context):
    from_ts, to_ts = delivery_window(context)
    rows = list(api_paged("/deliveries", "date", **{"from": from_ts, "to": to_ts}))

    with dwh_conn() as conn, conn.cursor() as cur:
        if rows:
            execute_values(
                cur,
                load_sql("stg_upsert_deliveries.sql"),
                [(
                    row.get("delivery_id"),
                    row.get("order_id"),
                    row.get("courier_id"),
                    _parse_ts(row.get("order_ts")),
                    Json(row),
                ) for row in rows],
            )

    print("stg.deliverysystem_deliveries: загружено %s записей за %s .. %s"
          % (len(rows), from_ts, to_ts))
    return len(rows)


with DAG(
    dag_id="stg_courier_api",
    description="Загрузка сырых данных API курьерской службы в STG-слой",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    params={"days_back": 7, "from_ts": "", "to_ts": ""},
    tags=["stg", "courier_api"],
) as dag:

    couriers = PythonOperator(
        task_id="load_couriers",
        python_callable=load_couriers,
    )

    deliveries = PythonOperator(
        task_id="load_deliveries",
        python_callable=load_deliveries,
    )

    trigger_dds = TriggerDagRunOperator(
        task_id="trigger_dds_courier",
        trigger_dag_id=DDS_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [couriers, deliveries] >> trigger_dds
