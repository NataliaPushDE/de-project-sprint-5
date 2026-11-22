CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id           serial PRIMARY KEY,
    courier_id   varchar(32)  NOT NULL UNIQUE,
    courier_name varchar(255) NOT NULL,
    load_dttm    timestamp    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id              serial PRIMARY KEY,
    restaurant_id   varchar(32)  NOT NULL UNIQUE,
    restaurant_name varchar(255) NOT NULL,
    active_from     timestamp    NOT NULL DEFAULT now(),
    active_to       timestamp    NOT NULL DEFAULT '2099-12-31 23:59:59',
    load_dttm       timestamp    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.dm_users (
    id            serial PRIMARY KEY,
    user_id       varchar(32)  NOT NULL UNIQUE,
    user_name     varchar(255),
    user_login    varchar(255),
    load_dttm     timestamp    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id            serial PRIMARY KEY,
    order_id      varchar(32)    NOT NULL UNIQUE,
    user_id       int            REFERENCES dds.dm_users (id),
    restaurant_id int            REFERENCES dds.dm_restaurants (id),
    courier_id    int            REFERENCES dds.dm_couriers (id),
    order_ts      timestamp      NOT NULL,
    order_sum     numeric(14, 2) NOT NULL DEFAULT 0,
    load_dttm     timestamp      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id          serial PRIMARY KEY,
    delivery_id varchar(32)    NOT NULL UNIQUE,
    order_id    varchar(32)    NOT NULL,
    courier_id  int            NOT NULL REFERENCES dds.dm_couriers (id),
    order_ts    timestamp      NOT NULL,
    delivery_ts timestamp,
    rate        smallint       CHECK (rate BETWEEN 1 AND 5),
    tip_sum     numeric(14, 2) NOT NULL DEFAULT 0,
    load_dttm   timestamp      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dds_fct_deliveries_order_ts_idx ON dds.fct_deliveries (order_ts);
CREATE INDEX IF NOT EXISTS dds_dm_orders_order_ts_idx      ON dds.dm_orders (order_ts);

ALTER TABLE dds.dm_orders
    ADD COLUMN IF NOT EXISTS courier_id int REFERENCES dds.dm_couriers (id);
