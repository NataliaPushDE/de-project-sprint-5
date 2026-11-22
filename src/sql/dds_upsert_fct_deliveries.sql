INSERT INTO dds.fct_deliveries (
    delivery_id, order_id, courier_id, order_ts, delivery_ts, rate, tip_sum
)
SELECT s.object_id,
       s.order_id,
       c.id,
       COALESCE(s.order_ts, (s.object_value ->> 'order_ts')::timestamp),
       NULLIF(s.object_value ->> 'delivery_ts', '')::timestamp,
       NULLIF(s.object_value ->> 'rate', '')::smallint,
       COALESCE(NULLIF(s.object_value ->> 'tip_sum', '')::numeric, 0)
FROM stg.deliverysystem_deliveries AS s
JOIN dds.dm_couriers AS c ON c.courier_id = s.courier_id
ON CONFLICT (delivery_id) DO UPDATE
   SET order_id    = EXCLUDED.order_id,
       courier_id  = EXCLUDED.courier_id,
       order_ts    = EXCLUDED.order_ts,
       delivery_ts = EXCLUDED.delivery_ts,
       rate        = EXCLUDED.rate,
       tip_sum     = EXCLUDED.tip_sum,
       load_dttm   = now()
