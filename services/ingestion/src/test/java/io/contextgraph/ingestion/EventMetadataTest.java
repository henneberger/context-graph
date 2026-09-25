package io.contextgraph.ingestion;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
class EventMetadataTest {
 @Test void preservesSourceIdentityAndTimestamp() {
  String id=UUID.randomUUID().toString();var m=EventMetadata.from(id,"2020-01-01T03:00:00+03:00","2026-01-01T00:00:00Z");
  assertEquals(id,m.id().toString());assertEquals(Instant.parse("2020-01-01T00:00:00Z"),m.time());
  assertEquals(Instant.parse("2020-01-01T00:00:02Z"),m.chunk(1,2).time());
  assertEquals(m.chunk(1,2).id(),EventMetadata.from(id,"2020-01-01T00:00:00Z","").chunk(1,2).id());
  assertNotEquals(m.chunk(0,0).id(),m.chunk(1,2).id());
 }
 @Test void generatesDefaultsAndRejectsInvalidHeaders() {
  String now=Instant.now().toString();assertEquals(now,EventMetadata.from(null,null,now).time().toString());
  assertNotEquals(EventMetadata.from(null,null,now).id(),EventMetadata.from(null,null,now).id());
  assertThrows(IllegalArgumentException.class,()->EventMetadata.from("1-1-1-1-1",null,now));
  assertThrows(Exception.class,()->EventMetadata.from(null,"yesterday",now));
 }
}
