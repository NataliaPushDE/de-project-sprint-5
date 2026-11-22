INSERT INTO dds.dm_couriers (courier_id, courier_name)
SELECT s.object_id,
       COALESCE(NULLIF(s.object_value ->> 'name', ''), 'неизвестно')
FROM stg.deliverysystem_couriers AS s
ON CONFLICT (courier_id) DO UPDATE
   SET courier_name = EXCLUDED.courier_name,
       load_dttm    = now()
