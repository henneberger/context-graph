SELECT workspace_id, resource_id, entity_id,
       TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start,
       TUMBLE_END(event_time, INTERVAL '10' SECOND) AS window_end,
       COUNT(*) AS event_count
FROM activity
GROUP BY workspace_id, resource_id, entity_id, TUMBLE(event_time, INTERVAL '10' SECOND)
