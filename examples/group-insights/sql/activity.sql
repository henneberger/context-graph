SELECT workspace_id, resource_id, entity_id, event_id, event_time, ingested_at,
       JSON_VALUE(payload, '$.text') AS text,
       JSON_VALUE(payload, '$.author') AS author
FROM activity
