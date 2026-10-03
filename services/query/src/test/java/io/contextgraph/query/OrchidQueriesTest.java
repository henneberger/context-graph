package io.contextgraph.query;

import com.fasterxml.jackson.databind.JsonNode;
import io.contextgraph.security.*;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class OrchidQueriesTest {
  final SecurityContext context=new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()+300);
  final IcebergQueries.Limits limits=new IcebergQueries.Limits(100,100,"64MB",30);
  final JsonNode config;
  OrchidQueriesTest()throws Exception {config=QueryService.yaml(Path.of("../../examples/ontology/queries.yaml"));}
  String resource(String entity){return AccessControl.resourceId(context.workspaceId(),entity);}
  class Grants implements PermissionChecks {
    Set<String> allowed=new HashSet<>(Set.of(resource("ada"),resource("bob")));
    boolean revoked;
    public void workspace(SecurityContext c,String p){if(revoked)throw new SecurityException("Revoked");}
    public boolean allowed(SecurityContext c,String r){return allowed.contains(r);}
  }
  Map<String,IcebergQueries.Source> sources() {
    var people=new ArrayList<>(List.of(col("workspace_id","string"),col("resource_id","string"),col("entity_id","string"),col("person_id","long"),col("full_name","string"),col("person_iri","string")));
    // Unmapped complex data must not enter OrchidDB's JDBC schema discovery.
    people.add(col("extra","variant"));
    var knows=List.of(col("workspace_id","string"),col("resource_id","string"),col("source_entity_id","string"),col("target_resource_id","string"),col("target_entity_id","string"),col("edge_id","long"),col("from_id","long"),col("to_id","long"));
    return Map.of("people",new IcebergQueries.Source(new IcebergQueries.Table("context_secure.people","unused",people),new IcebergQueries.Policy("entity_id",null)),
        "knows",new IcebergQueries.Source(new IcebergQueries.Table("context_secure.knows","unused",knows),new IcebergQueries.Policy("source_entity_id","target_entity_id")));
  }
  IcebergQueries.Column col(String name,String type){return new IcebergQueries.Column(name,type,false);}
  Map<String,Object> person(String entity,long id,String name) {
    return Map.of("workspace_id",context.workspaceId(),"resource_id",resource(entity),"entity_id",entity,"person_id",id,"full_name",name,"person_iri","https://example.org/people/"+entity,"extra",Map.of("tags",List.of("example")));
  }
  Map<String,Object> edge(String target,long id,long to) {
    return Map.of("workspace_id",context.workspaceId(),"resource_id",resource("ada"),"source_entity_id","ada","target_resource_id",resource(target),"target_entity_id",target,"edge_id",id,"from_id",1L,"to_id",to);
  }
  Map<String,List<Map<String,Object>>> inputs(){return Map.of("people",List.of(person("ada",1,"Ada"),person("bob",2,"Bob")),"knows",List.of(edge("bob",1,2)));}
  OrchidQueries query(String name){return OrchidQueries.configured(config.path("graphs").path("people"),config.path("queries").path(name),sources().keySet());}
  List<Map<String,Object>> run(OrchidQueries query,Map<String,Object> args,Map<String,List<Map<String,Object>>> inputs,Grants grants,IcebergQueries.Limits limits)throws Exception {
    return IcebergQueries.joinAuthorized(sources(),inputs,(c,d)->query.execute(c,args,limits,d),context,grants,limits,System.nanoTime()+30_000_000_000L);
  }
  @Test void cypherAndOntologySparqlTraverseTheSameAuthorizedData()throws Exception {
    assertEquals(List.of(Map.of("name","Bob")),run(query("friends"),Map.of("name","Ada"),inputs(),new Grants(),limits));
    assertEquals(List.of(Map.of("name","Bob")),run(query("vocabularyFriends"),Map.of(),inputs(),new Grants(),limits));
    assertEquals(2L,((Number)run(query("personCount"),Map.of(),inputs(),new Grants(),limits).getFirst().get("total")).longValue());
  }
  @Test void astParametersCannotInjectGraphOrSql()throws Exception {
    assertTrue(run(query("friends"),Map.of("name","Ada' RETURN 1; DROP TABLE people; --"),inputs(),new Grants(),limits).isEmpty());
    assertEquals(List.of(Map.of("name","Bob")),run(query("friends"),Map.of("name","Ada"),inputs(),new Grants(),limits));
  }
  @Test void filteringPrecedesNativeTraversalAndAggregation()throws Exception {
    var grants=new Grants();var filtered=new LinkedHashMap<String,List<Map<String,Object>>>();
    try(var c=IcebergQueries.connect(false);var s=c.createStatement()) {
      s.execute("CREATE VIEW raw_source AS SELECT * FROM (VALUES "
          +"('workspace-a','"+resource("ada")+"','ada',1::BIGINT,'Ada','https://example.org/ada'),"
          +"('workspace-a','"+resource("bob")+"','bob',2::BIGINT,'Bob','https://example.org/bob'),"
          +"('workspace-a','"+resource("eve")+"','eve',3::BIGINT,'Eve','https://example.org/eve'),"
          +"('workspace-b','"+AccessControl.resourceId("workspace-b","foreign")+"','foreign',4::BIGINT,'Foreign','https://example.org/foreign')"
          +") AS t(workspace_id,resource_id,entity_id,person_id,full_name,person_iri)");
      filtered.put("people",IcebergQueries.authorizedQuery(c,new IcebergQueries.Policy("entity_id",null),"SELECT * FROM source",Map.of(),context,grants,limits));
    }
    try(var c=IcebergQueries.connect(false);var s=c.createStatement()) {
      s.execute("CREATE VIEW raw_source AS SELECT * FROM (VALUES "
          +"('workspace-a','"+resource("ada")+"','ada','"+resource("bob")+"','bob',1::BIGINT,1::BIGINT,2::BIGINT),"
          +"('workspace-a','"+resource("ada")+"','ada','"+resource("eve")+"','eve',2::BIGINT,1::BIGINT,3::BIGINT)"
          +") AS t(workspace_id,resource_id,source_entity_id,target_resource_id,target_entity_id,edge_id,from_id,to_id)");
      filtered.put("knows",IcebergQueries.authorizedQuery(c,new IcebergQueries.Policy("source_entity_id","target_entity_id"),"SELECT * FROM source",Map.of(),context,grants,limits));
    }
    assertEquals(List.of(Map.of("name","Bob")),run(query("friends"),Map.of("name","Ada"),filtered,grants,limits));
    assertEquals(2L,((Number)run(query("personCount"),Map.of(),filtered,grants,limits).getFirst().get("total")).longValue());
  }
  @Test void revokedEdgeEndpointAndForeignWorkspaceFailClosed()throws Exception {
    var grants=new Grants();grants.allowed.remove(resource("bob"));
    assertThrows(SecurityException.class,()->run(query("friends"),Map.of("name","Ada"),inputs(),grants,limits));
    var foreign=new HashMap<>(person("ada",1,"Ada"));foreign.put("workspace_id","workspace-b");
    assertThrows(SecurityException.class,()->run(query("personCount"),Map.of(),Map.of("people",List.of(foreign),"knows",List.of()),new Grants(),limits));
  }
  @Test void permissionRecheckAfterNativeExecutionRejectsRevocation()throws Exception {
    var grants=new Grants();
    assertThrows(SecurityException.class,()->IcebergQueries.joinAuthorized(sources(),inputs(),(c,d)->{
      var result=query("personCount").execute(c,Map.of(),limits,d);grants.revoked=true;return result;
    },context,grants,limits,System.nanoTime()+30_000_000_000L));
  }
  @Test void unknownSourcesWritesAndUnsupportedBindingsAreRejected()throws Exception {
    var unsafeSources=new HashMap<>(sources());
    unsafeSources.put("knows",new IcebergQueries.Source(sources().get("knows").table(),new IcebergQueries.Policy("source_entity_id",null)));
    assertThrows(SecurityException.class,()->query("friends").validatePolicies(unsafeSources));
    var graph=config.path("graphs").path("people").deepCopy();
    ((com.fasterxml.jackson.databind.node.ObjectNode)graph.path("nodes").get(0)).put("source","raw_source");
    assertThrows(IllegalArgumentException.class,()->OrchidQueries.configured(graph,config.path("queries").path("friends"),sources().keySet()));
    var write=config.path("queries").path("personCount").deepCopy();
    ((com.fasterxml.jackson.databind.node.ObjectNode)write).put("query","CREATE (p:Person {name: 'Mallory'})");
    var writeQuery=OrchidQueries.configured(config.path("graphs").path("people"),write,sources().keySet());
    assertThrows(RuntimeException.class,()->run(writeQuery,Map.of(),inputs(),new Grants(),limits));
    assertThrows(IllegalArgumentException.class,()->run(query("vocabularyFriends"),Map.of("name","Ada"),inputs(),new Grants(),limits));
  }
  @Test void gremlinRunsOnTheSameAuthorizedRelations()throws Exception {
    var definition=config.path("queries").path("personCount").deepCopy();
    ((com.fasterxml.jackson.databind.node.ObjectNode)definition).put("language","gremlin").put("query","g.V().hasLabel('Person').count()");
    var gremlin=OrchidQueries.configured(config.path("graphs").path("people"),definition,sources().keySet());
    var rows=run(gremlin,Map.of(),inputs(),new Grants(),limits);
    assertEquals(2L,((Number)rows.getFirst().values().iterator().next()).longValue());
  }
  @Test void graphCanGrowAcrossDomainsWithoutBreakingExistingQueries()throws Exception {
    var json=new com.fasterxml.jackson.databind.ObjectMapper();
    var expanded=config.path("graphs").path("people").deepCopy();
    ((com.fasterxml.jackson.databind.node.ArrayNode)expanded.path("nodes")).add(json.readTree("""
      {"label":"Project","source":"projects","id":"project_id",
       "properties":{"title":"title","iri":"project_iri"}}
      """));
    ((com.fasterxml.jackson.databind.node.ArrayNode)expanded.path("edges")).add(json.readTree("""
      {"label":"WORKS_ON","source":"assignments","id":"edge_id",
       "sourceColumn":"from_id","targetColumn":"to_id","sourceLabel":"Person","targetLabel":"Project"}
      """));
    ((com.fasterxml.jackson.databind.node.ArrayNode)expanded.path("ontology").path("classes")).add(json.readTree("""
      {"iri":"https://example.org/Project","label":"Project","identity":"iri"}
      """));
    ((com.fasterxml.jackson.databind.node.ArrayNode)expanded.path("ontology").path("properties")).add(json.readTree("""
      {"iri":"https://example.org/title","label":"Project","property":"title"}
      """));
    var expandedSources=new LinkedHashMap<>(sources());
    var projectColumns=List.of(col("workspace_id","string"),col("resource_id","string"),col("entity_id","string"),col("project_id","long"),col("title","string"),col("project_iri","string"));
    expandedSources.put("projects",new IcebergQueries.Source(new IcebergQueries.Table("context_secure.projects","unused",projectColumns),new IcebergQueries.Policy("entity_id",null)));
    expandedSources.put("assignments",new IcebergQueries.Source(new IcebergQueries.Table("context_secure.assignments","unused",sources().get("knows").table().columns()),new IcebergQueries.Policy("source_entity_id","target_entity_id")));
    var expandedInputs=new LinkedHashMap<>(inputs());
    expandedInputs.put("projects",List.of(Map.of("workspace_id",context.workspaceId(),"resource_id",resource("project"),"entity_id","project","project_id",10L,"title","Context Graph","project_iri","https://example.org/projects/context")));
    expandedInputs.put("assignments",List.of(edge("project",2,10)));
    var grants=new Grants();grants.allowed.add(resource("project"));
    var definitions=List.of(config.path("queries").path("friends"),json.readTree("""
      {"language":"cypher","query":"MATCH (p:Person)-[:WORKS_ON]->(project:Project) WHERE p.name = $name RETURN project.title AS name"}
      """),json.readTree("""
      {"language":"sparql","query":"SELECT ?name WHERE { ?p a <https://example.org/Project> . ?p <https://example.org/title> ?name . }"}
      """));
    for(int i=0;i<definitions.size();i++) {
      var query=OrchidQueries.configured(expanded,definitions.get(i),expandedSources.keySet());
      query.validatePolicies(expandedSources);
      Map<String,Object> args=i==2?Map.of():Map.of("name","Ada");
      var result=IcebergQueries.joinAuthorized(expandedSources,expandedInputs,(c,d)->query.execute(c,args,limits,d),context,grants,limits,System.nanoTime()+30_000_000_000L);
      assertEquals(List.of(Map.of("name",i==0?"Bob":"Context Graph")),result);
    }
    grants.allowed.remove(resource("project"));
    var query=OrchidQueries.configured(expanded,definitions.get(1),expandedSources.keySet());
    assertThrows(SecurityException.class,()->IcebergQueries.joinAuthorized(expandedSources,expandedInputs,(c,d)->query.execute(c,Map.of("name","Ada"),limits,d),context,grants,limits,System.nanoTime()+30_000_000_000L));
  }
  @Test void duplicateIdentitiesAndResultOverflowFailRatherThanTruncate()throws Exception {
    assertThrows(IllegalArgumentException.class,()->run(query("personCount"),Map.of(),Map.of("people",List.of(person("ada",1,"Ada"),person("bob",1,"Bob")),"knows",List.of()),new Grants(),limits));
    try(var c=IcebergQueries.connect(false)) {
      assertThrows(IllegalStateException.class,()->IcebergQueries.run(c,new IcebergQueries.BoundSql("SELECT * FROM range(2)",List.of()),1,System.nanoTime()+30_000_000_000L));
    }
  }
}
