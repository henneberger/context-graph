package io.contextgraph.ingestion;
import java.time.Instant;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

/** Source-independent event identity/time, separate from payload and access labels. */
record EventMetadata(UUID id, Instant time) {
    static EventMetadata from(String id, String time, String fallbackTime) {
        UUID uuid = id == null ? UUID.randomUUID() : UUID.fromString(id);
        if (id != null && !uuid.toString().equalsIgnoreCase(id)) throw new IllegalArgumentException("X-Event-Id must be a canonical UUID");
        return new EventMetadata(uuid, Instant.parse(time == null ? fallbackTime : time));
    }
    EventMetadata chunk(int sequence, double elapsedSeconds) {
        UUID chunkId=UUID.nameUUIDFromBytes((id+":"+sequence).getBytes(StandardCharsets.UTF_8));
        return new EventMetadata(chunkId,time.plusNanos(Math.round(elapsedSeconds*1_000_000_000d)));
    }
}
