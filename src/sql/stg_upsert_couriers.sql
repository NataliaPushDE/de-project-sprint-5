INSERT INTO stg.deliverysystem_couriers AS t (object_id, object_value)
VALUES %s
ON CONFLICT (object_id) DO UPDATE
   SET object_value = EXCLUDED.object_value,
       load_dttm    = now()
