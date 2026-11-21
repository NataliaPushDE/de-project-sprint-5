Репозиторий содержит многослойное хранилище (STG → DDS → CDM) и ETL-процессы на Apache Airflow
для расчёта выплат курьерам курьерской службы.

## Что реализовано

| Слой | Объекты | Источник |
|---|---|---|
| STG | `stg.deliverysystem_couriers`, `stg.deliverysystem_deliveries` | API курьерской службы |
| DDS | `dds.dm_couriers`, `dds.fct_deliveries` (новые), `dds.dm_orders` (+ `courier_id`), `dds.dm_users`, `dds.dm_restaurants` | STG-слой |
| CDM | `cdm.dm_courier_ledger` | DDS-слой |

Проектные решения и источники полей описаны в [`api_entities.md`](api_entities.md).

## Структура репозитория

```
src/
├── dags/
│   ├── dwh_helpers.py
│   ├── stg_load_courier_api.py
│   ├── dds_load_courier_entities.py
│   └── cdm_build_courier_ledger.py
└── sql/
    ├── stg_create_tables.sql
    ├── dds_create_tables.sql
    ├── cdm_create_courier_ledger.sql
    ├── stg_upsert_couriers.sql
    ├── stg_upsert_deliveries.sql
    ├── dds_upsert_couriers.sql
    ├── dds_upsert_orders.sql
    ├── dds_upsert_fct_deliveries.sql
    └── cdm_build_courier_ledger.sql
api_entities.md
```

## Как запустить контейнер

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

После старта доступны:

- Airflow — `localhost:3000/airflow`
- БД — `jovyan:jovyan@localhost:15432/de`

Альтернатива — `docker compose up -d`: поднимается тот же контейнер вместе с Metabase,
а `src/dags` и `src/sql` монтируются в `/lessons/dags` и `/lessons/sql`
(пути переопределяются переменными `AIRFLOW_DAGS_DIR` и `AIRFLOW_SQL_DIR`).
Шаг 2 (копирование файлов в контейнер) в этом случае не нужен.

**Создать подключение к БД** (один раз, для `PostgresOperator`):

```
docker exec <container> airflow connections add 'PG_WAREHOUSE_CONNECTION' \
  --conn-type postgres --conn-host localhost --conn-login jovyan \
  --conn-password jovyan --conn-port 5432 --conn-schema de
```

## Порядок запуска

**1. Применить DDL** (один раз):

```
docker exec -i <container> psql -U jovyan -h localhost -d de -v ON_ERROR_STOP=1 -f - < src/sql/stg_create_tables.sql
docker exec -i <container> psql -U jovyan -h localhost -d de -v ON_ERROR_STOP=1 -f - < src/sql/dds_create_tables.sql
docker exec -i <container> psql -U jovyan -h localhost -d de -v ON_ERROR_STOP=1 -f - < src/sql/cdm_create_courier_ledger.sql
```

**2. Положить DAG-и и SQL-скрипты в контейнер**:

```
for f in src/dags/*.py; do docker cp "$f" <container>:/lessons/dags/; done
docker exec <container> mkdir -p /lessons/sql
docker cp src/sql/. <container>:/lessons/sql/
```

DAG-и читают SQL через `load_sql()` из `dwh_helpers.py`; в контейнере единый
каталог — `/lessons/sql/` (рядом с `/lessons/dags/`).

**3. Снять с паузы и запустить:**

```
docker exec <container> airflow dags unpause stg_courier_api
docker exec <container> airflow dags unpause dds_courier
docker exec <container> airflow dags unpause cdm_courier_ledger
docker exec <container> airflow dags trigger stg_courier_api
```

`stg_courier_api` по завершении сам триггерит `dds_courier`, а тот — `cdm_courier_ledger`,
поэтому достаточно запустить первый DAG.

**Историческая загрузка за нужный период** (например, за октябрь):

```
docker exec <container> airflow dags trigger stg_courier_api \
  -c '{"from_ts": "2026-10-01 00:00:00", "to_ts": "2026-11-01 00:00:00"}'
```

Витрина всегда пересчитывается за месяц, предшествующий дате запуска:
`DELETE` + `INSERT` выполняются одним SQL-скриптом, поэтому повторный запуск
безопасен.

## Как устроены DAG-и

### `stg_courier_api` — API → STG (расписание `@daily`)

- `load_couriers` — забирает справочник курьеров целиком (постранично);
- `load_deliveries` — забирает доставки за 7 предыдущих дней (постранично, с сортировкой);
- `trigger_dds_courier` — запускает следующий слой.

Данные пишутся в JSON (`object_value`) + выделенные ключи. `ON CONFLICT (object_id)`
делает загрузку идемпотентной: повторный запуск обновляет записи, а не дублирует.

### `dds_courier` — STG → DDS

Все шаги — `PostgresOperator`, SQL лежит в `src/sql/dds_upsert_*.sql`.

- `load_dm_couriers` — измерение «Курьер»;
- `load_dm_orders` — заказы + ссылка `courier_id`;
- `load_fct_deliveries` — рейтинг и чаевые по заказу;
- `trigger_cdm_courier_ledger` — запускает витрину.

Слой «докачивается»: каждый запуск подхватывает из STG всё, чего ещё нет в DDS.
Если заказ уже есть в `dds.dm_orders` (пришёл из подсистемы заказов), DAG не
перезаписывает его `order_ts` и `order_sum`, а только проставляет `courier_id`.

В эталонной сборке измерения наполняет отдельный DAG, а факты — DAG спринта.
В репозитории DAG спринта отсутствует, поэтому оба шага объединены в
`dds_courier`: цепочка самодостаточна и не зависит от внешнего расписания.

### `cdm_courier_ledger` — DDS → CDM

Считает витрину за **предыдущий месяц** (в июне — за май). Один `PostgresOperator`
выполняет `DELETE` месяца и `INSERT` из `src/sql/cdm_build_courier_ledger.sql`;
границы месяца вычисляются в SQL от `data_interval_end`.

Формулы:

```
order_processing_fee = orders_total_sum * 0.25
courier_order_sum    = Σ по заказам max(order_sum * pct(rate_avg), min_payout(rate_avg))
courier_reward_sum   = courier_order_sum + courier_tips_sum * 0.95
```

Ставки по среднему рейтингу `r` за месяц:

| Условие | Процент | Минимум за заказ |
|---|---|---|
| `r < 4.0` | 5 % | 100 ₽ |
| `4.0 ≤ r < 4.5` | 7 % | 150 ₽ |
| `4.5 ≤ r < 4.9` | 8 % | 175 ₽ |
| `r ≥ 4.9` | 10 % | 200 ₽ |

## Конфигурация

API задаётся переменными окружения контейнера, доступ к БД — подключением Airflow
(значения по умолчанию — для локального стенда):

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `COURIER_API_NICKNAME` | `test` | заголовок `X-Nickname` |
| `COURIER_API_COHORT` | `1` | заголовок `X-Cohort` |
| `COURIER_API_KEY` | ключ из задания | заголовок `X-API-KEY` |
| подключение `PG_WAREHOUSE_CONNECTION` | `localhost:5432/de`, `jovyan` | доступ к БД для всех слоёв |

## Проверка результата

```
SELECT courier_id, courier_name, settlement_year, settlement_month,
       orders_count, orders_total_sum, rate_avg,
       order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
FROM cdm.dm_courier_ledger
ORDER BY settlement_year DESC, settlement_month DESC, courier_reward_sum DESC;
```

Витрина сверена с независимым расчётом по данным API: для 999 заказов за октябрь 2026
все 100 строк совпали по всем полям. В октябрьскую витрину входят 16 заказов, доставленных
уже в ноябре: они отнесены к октябрю по `order_ts`, как и требует задание.

## Ограничения текущей сборки

1. **Основной источник `order_sum` — подсистема заказов (`dds.dm_orders`).** Если строка
   заказа уже есть в хранилище, её стоимость и дата не перезаписываются. В автономном
   контейнере без MongoDB заказ создаётся из ответа `/deliveries`, и его `sum` используется
   как резервный источник. Структура `dds.dm_orders` готова к обогащению из Mongo
   (`user_id`, `restaurant_id`), для этого достаточно передать строку подключения.
2. **В заголовках стоит заглушка `X-Nickname: test`, `X-Cohort: 1`** — замените на свои
   значения через переменные окружения.
3. **Справочник ресторанов не загружается** из API: рестораны приходят из подсистемы заказов,
   для витрины они не нужны.
