CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id           serial PRIMARY KEY,
    object_id    varchar(32) NOT NULL UNIQUE,
    object_value jsonb       NOT NULL,
    load_dttm    timestamp   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id           serial PRIMARY KEY,
    object_id    varchar(32)   NOT NULL UNIQUE,
    order_id     varchar(32)   NOT NULL,
    courier_id   varchar(32)   NOT NULL,
    order_ts     timestamp,
    object_value jsonb         NOT NULL,
    load_dttm    timestamp     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS stg_deliverysystem_deliveries_order_ts_idx ON stg.deliverysystem_deliveries (order_ts);
CREATE INDEX IF NOT EXISTS stg_deliverysystem_deliveries_courier_idx  ON stg.deliverysystem_deliveries (courier_id);
