SELECT entity_id, event_id, event_time, text, author
FROM source
WHERE (:entityId IS NULL OR entity_id = :entityId)
ORDER BY event_time DESC LIMIT :limit
