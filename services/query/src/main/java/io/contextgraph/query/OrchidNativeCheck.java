package io.contextgraph.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

/** Release check: load the packaged native compiler and execute both graph and ontology queries. */
public final class OrchidNativeCheck {
  public static void main(String[] args)throws Exception {
    var json=new ObjectMapper();
    var graph=json.readTree("""
      {"nodes":[{"label":"Person","source":"people","id":"id",
        "properties":{"name":"name","iri":"iri"}}],
       "ontology":{"classes":[{"iri":"https://example.org/Person","label":"Person","identity":"iri"}],
         "properties":[{"iri":"https://example.org/name","label":"Person","property":"name"}]}}
      """);
    for(String language:List.of("cypher","sparql"))try(var c=IcebergQueries.connect(false);var s=c.createStatement()) {
      s.execute("CREATE TABLE people AS SELECT 1::BIGINT AS id, 'Ada' AS name, 'https://example.org/ada' AS iri");
      s.execute("SET enable_external_access=false; SET lock_configuration=true");
      var definition=json.createObjectNode().put("language",language).put("query",language.equals("cypher")?
          "MATCH (p:Person) RETURN p.name AS name":
          "SELECT ?name WHERE { ?p a <https://example.org/Person> . ?p <https://example.org/name> ?name . }");
      var query=OrchidQueries.configured(graph,definition,Set.of("people"));
      var rows=query.execute(c,Map.of(),new IcebergQueries.Limits(10,10,"64MB",30),System.nanoTime()+30_000_000_000L);
      if(!rows.equals(List.of(Map.of("name","Ada"))))throw new IllegalStateException("Packaged OrchidDB check failed: "+language);
    }
    System.out.println("Packaged OrchidDB Cypher and SPARQL checks passed");
  }
}
