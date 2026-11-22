CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id                   serial PRIMARY KEY,
    courier_id           varchar(32)    NOT NULL,
    courier_name         varchar(255)   NOT NULL,
    settlement_year      smallint       NOT NULL,
    settlement_month     smallint       NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count         int            NOT NULL DEFAULT 0,
    orders_total_sum     numeric(14, 2) NOT NULL DEFAULT 0,
    rate_avg             numeric(3, 2)  NOT NULL DEFAULT 0,
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
    courier_order_sum    numeric(14, 2) NOT NULL DEFAULT 0,
    courier_tips_sum     numeric(14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum   numeric(14, 2) NOT NULL DEFAULT 0,

    CONSTRAINT dm_courier_ledger_pk UNIQUE (courier_id, settlement_year, settlement_month)
);
