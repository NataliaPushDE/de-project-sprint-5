INSERT INTO dds.dm_orders (order_id, courier_id, order_ts, order_sum)
SELECT s.order_id,
       c.id,
       s.order_ts,
       COALESCE(NULLIF(s.object_value ->> 'sum', '')::numeric, 0)
FROM stg.deliverysystem_deliveries AS s
JOIN dds.dm_couriers AS c ON c.courier_id = s.courier_id
WHERE s.order_ts IS NOT NULL
ON CONFLICT (order_id) DO UPDATE
   SET courier_id = EXCLUDED.courier_id,
       load_dttm  = now()
