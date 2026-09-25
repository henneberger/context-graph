SELECT workspace_id, resource_id, entity_id, event_id, event_time, ingested_at, runId AS run_id, eventType AS event_type, details, _json_remainder
FROM trajectories
