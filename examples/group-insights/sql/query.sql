SELECT entity_id, window_start, window_end, event_count
FROM source
WHERE (:entityId IS NULL OR entity_id = :entityId)
ORDER BY window_end DESC LIMIT :limit
