SELECT label, count, profile, tags, attributes, _json_remainder
FROM source
ORDER BY event_time DESC
LIMIT 100
