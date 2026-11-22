import os
from contextlib import contextmanager
from pathlib import Path

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE_URL = os.getenv(
    "COURIER_API_BASE_URL",
    "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net",
)
API_KEY = os.getenv("COURIER_API_KEY", "25c27781-8fde-4b30-a22e-524044a7580f")
API_NICKNAME = os.getenv("COURIER_API_NICKNAME", "test")
API_COHORT = os.getenv("COURIER_API_COHORT", "1")

PAGE_LIMIT = 50
DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def load_sql(name):
    return (SQL_DIR / name).read_text(encoding="utf-8")


def api_get(path, **params):
    params.setdefault("limit", PAGE_LIMIT)
    resp = requests.get(
        API_BASE_URL + path,
        headers={
            "X-Nickname": API_NICKNAME,
            "X-Cohort": API_COHORT,
            "X-API-KEY": API_KEY,
        },
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def api_paged(path, sort_field, **filters):
    offset = 0
    while True:
        chunk = api_get(
            path,
            sort_field=sort_field,
            sort_direction="asc",
            offset=offset,
            **filters
        )
        if not chunk:
            return
        yield from chunk
        offset += len(chunk)
        if len(chunk) < PAGE_LIMIT:
            return


@contextmanager
def dwh_conn():
    conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def run_params(context):
    params = dict(context["params"])
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    params.update({k: v for k, v in conf.items() if v not in (None, "")})
    return params
