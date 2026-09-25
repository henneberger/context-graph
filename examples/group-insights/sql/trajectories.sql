SELECT workspace_id, resource_id, entity_id, event_id, event_time, ingested_at,
       JSON_VALUE(payload, '$.runId') AS run_id,
       JSON_VALUE(payload, '$.eventType') AS event_type,
       payload
FROM trajectories
