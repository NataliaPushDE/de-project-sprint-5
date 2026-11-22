{% set period_start = "date_trunc('month', DATE '" ~ data_interval_end.strftime('%Y-%m-%d') ~ "' - interval '1 month')" %}

DELETE FROM cdm.dm_courier_ledger
WHERE settlement_year  = EXTRACT(YEAR  FROM {{ period_start }})
  AND settlement_month = EXTRACT(MONTH FROM {{ period_start }});

WITH orders AS (
    SELECT
        dc.courier_id,
        dc.courier_name,
        o.order_id,
        o.order_sum,
        d.tip_sum,
        COALESCE(AVG(d.rate) OVER (PARTITION BY dc.courier_id), 0) AS rate_avg
    FROM dds.fct_deliveries AS d
    JOIN dds.dm_orders    AS o  ON o.order_id = d.order_id
    JOIN dds.dm_couriers  AS dc ON dc.id = d.courier_id
    WHERE o.order_ts >= {{ period_start }}
      AND o.order_ts <  {{ period_start }} + interval '1 month'
),
calc AS (
    SELECT
        *,
        CASE WHEN rate_avg < 4.0 THEN 0.05
             WHEN rate_avg < 4.5 THEN 0.07
             WHEN rate_avg < 4.9 THEN 0.08
             ELSE 0.10 END AS rate_pct,
        CASE WHEN rate_avg < 4.0 THEN 100
             WHEN rate_avg < 4.5 THEN 150
             WHEN rate_avg < 4.9 THEN 175
             ELSE 200 END AS min_payout
    FROM orders
)
INSERT INTO cdm.dm_courier_ledger (
    courier_id, courier_name, settlement_year, settlement_month,
    orders_count, orders_total_sum, rate_avg,
    order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
)
SELECT
    courier_id,
    courier_name,
    EXTRACT(YEAR  FROM {{ period_start }})::smallint,
    EXTRACT(MONTH FROM {{ period_start }})::smallint,
    COUNT(DISTINCT order_id),
    SUM(order_sum),
    ROUND(rate_avg, 2),
    ROUND(SUM(order_sum) * 0.25, 2),
    ROUND(SUM(GREATEST(order_sum * rate_pct, min_payout)), 2),
    SUM(tip_sum),
    ROUND(SUM(GREATEST(order_sum * rate_pct, min_payout))
          + SUM(tip_sum) * 0.95, 2)
FROM calc
GROUP BY courier_id, courier_name, rate_avg
