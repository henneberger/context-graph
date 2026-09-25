package io.contextgraph.query;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import io.contextgraph.security.*;
import java.time.Instant;
import java.util.*;
class DocumentSearchTest {
  final SecurityContext user=new SecurityContext("alice","knowledge",Instant.now().getEpochSecond()+300);
  final String resource=AccessControl.resourceId("knowledge","repo");
  class Grants implements PermissionChecks {boolean revoked=false;public void workspace(SecurityContext c,String p){if(revoked)throw new SecurityException();}public boolean allowed(SecurityContext c,String r){return !revoked&&resource.equals(r);}}
  Map<String,Object> row(String id,String title,String body,String time,boolean deleted){
    var row=new LinkedHashMap<String,Object>();
    row.putAll(Map.of("workspace_id","knowledge","entity_id","repo","resource_id",resource,"event_id",time,"ingested_at",time));
    row.putAll(Map.of("documentId",id,"source","github","title",title,"text",body,"url","https://github.com/MariHQ/mari/issues/1","observedAt",time,"deleted",deleted));return row;
  }
  @BeforeAll static void extensions()throws Exception{try(var c=IcebergQueries.connect(true)) {}}
  @Test void latestVersionAndTombstoneWin()throws Exception {
    var docs=List.of(row("one","secret old","obsolete","2026-01-01",false),row("one","current","latest","2026-02-01",false),row("two","deleted","remove","2026-01-01",false),row("two","removed","","2026-02-01",true));
    var hits=DocumentSearch.execute(docs,Map.of("ids",List.of("one","two")),true,user,new Grants());assertEquals(1,hits.size());assertEquals("current",hits.getFirst().get("title"));
  }
  @Test void bm25SearchUsesBoundParametersAndRevocationFailsClosed()throws Exception {
    var docs=List.of(row("one","Kafka ingestion","Kafka streams process events","2026-01-01",false),row("two","Search","Documents and citations","2026-01-01",false));
    var grants=new Grants();var hits=DocumentSearch.execute(docs,Map.of("query","Kafka","limit",10),false,user,grants);assertEquals("one",hits.getFirst().get("id"));assertTrue(((Number)hits.getFirst().get("score")).doubleValue()>0);
    assertDoesNotThrow(()->DocumentSearch.execute(docs,Map.of("query","' ; DROP TABLE documents; --"),false,user,grants));
    grants.revoked=true;assertThrows(SecurityException.class,()->DocumentSearch.execute(docs,Map.of("query","Kafka"),false,user,grants));
  }
  @Test void freshnessDecaysAndInvalidDatesDoNotBoost() {
    Instant now=Instant.parse("2026-09-24T00:00:00Z");assertEquals(1,DocumentSearch.freshness("2026-09-24T00:00:00Z",14,now),1e-9);assertEquals(.5,DocumentSearch.freshness("2026-09-10T00:00:00Z",14,now),1e-9);assertEquals(0,DocumentSearch.freshness("invalid",14,now));assertEquals(0,DocumentSearch.freshness("2027-01-01T00:00:00Z",14,now));
  }
  @Test void identicalLexicalEvidencePrefersRecentDocument() throws Exception {
    var old=new HashMap<>(row("old","Recovery fix","Recovery fix detail", "2026-01-01",false));
    var fresh=new HashMap<>(row("new","Recovery fix","Recovery fix detail", "2026-01-01",false));
    for(var entry:List.of(old,fresh)) {
      entry.put("documentType","commit");entry.put("updatedAt",Instant.now().minusSeconds(entry==old?86400L*365:0).toString());
    }
    var hits=DocumentSearch.execute(List.of(old,fresh),Map.of("query","Recovery","limit",2),false,user,new Grants());
    assertEquals("new",hits.getFirst().get("id"));assertTrue(((Number)hits.getFirst().get("score")).doubleValue()>((Number)hits.getLast().get("score")).doubleValue());
  }
  @Test void forgedProvenanceIsRejected() {
    var forged=new HashMap<>(row("one","Kafka","Kafka","2026-01-01",false));forged.put("resource_id",AccessControl.resourceId("knowledge","other"));assertThrows(SecurityException.class,()->DocumentSearch.current(List.of(forged),user,new Grants()));
  }
}
