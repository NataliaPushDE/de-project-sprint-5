INSERT INTO stg.deliverysystem_deliveries AS t
    (object_id, order_id, courier_id, order_ts, object_value)
VALUES %s
ON CONFLICT (object_id) DO UPDATE
   SET order_id     = EXCLUDED.order_id,
       courier_id   = EXCLUDED.courier_id,
       order_ts     = EXCLUDED.order_ts,
       object_value = EXCLUDED.object_value,
       load_dttm    = now()
