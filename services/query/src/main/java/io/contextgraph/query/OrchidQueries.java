package io.contextgraph.query;

import com.fasterxml.jackson.databind.JsonNode;
import io.orchiddb.*;
import java.sql.Connection;
import java.util.*;

/** Trusted configuration maps only the already-authorized relations in a request-local database. */
final class OrchidQueries {
  private static final String ENGINE = "authorized";
  private record Projection(String view, String source, Set<String> columns, String id, boolean edge) {}
  private final GraphMapping mapping;
  private final Ontology ontology;
  private final List<Projection> projections;
  private final String language, text;

  private OrchidQueries(GraphMapping mapping, Ontology ontology, List<Projection> projections,
      String language, String text) {
    this.mapping=mapping;this.ontology=ontology;this.projections=List.copyOf(projections);
    this.language=language;this.text=text;
  }

  static OrchidQueries configured(JsonNode graph, JsonNode query, Set<String> sources) {
    IcebergQueries.validateSources(sources);
    String language=required(query,"language");
    if(!Set.of("cypher","gremlin","sparql").contains(language))throw new IllegalArgumentException("Unsupported OrchidDB language");
    String text=required(query,"query");
    if(text.length()>32000)throw new IllegalArgumentException("Graph query too large");
    if(!graph.path("nodes").isArray())throw new IllegalArgumentException("Graph nodes are required");
    var nodes=new ArrayList<NodeMapping>();var edges=new ArrayList<EdgeMapping>();
    var projections=new ArrayList<Projection>();
    for(JsonNode n:graph.path("nodes")) {
      String view="_orchid_node_"+nodes.size(),id=required(n,"id");
      Map<String,String> properties=properties(n);
      var columns=new LinkedHashSet<String>(properties.values());columns.add(id);
      projections.add(projection(n,view,columns,id,false,sources));
      nodes.add(new NodeMapping(required(n,"label"),Source.table(ENGINE,view),id,properties));
    }
    for(JsonNode e:graph.path("edges")) {
      String view="_orchid_edge_"+edges.size(),id=required(e,"id");
      String from=required(e,"sourceColumn"),to=required(e,"targetColumn");
      Map<String,String> properties=properties(e);
      var columns=new LinkedHashSet<String>(properties.values());columns.addAll(List.of(id,from,to));
      projections.add(projection(e,view,columns,id,true,sources));
      edges.add(new EdgeMapping(required(e,"label"),Source.table(ENGINE,view),id,from,to,
          required(e,"sourceLabel"),required(e,"targetLabel"),properties));
    }
    var classes=new ArrayList<Ontology.ClassMapping>();
    var properties=new ArrayList<Ontology.PropertyMapping>();
    var relationships=new ArrayList<Ontology.RelationshipMapping>();
    JsonNode o=graph.path("ontology");
    for(JsonNode c:o.path("classes"))classes.add(new Ontology.ClassMapping(required(c,"iri"),required(c,"label"),c.has("identity")?required(c,"identity"):null));
    for(JsonNode p:o.path("properties"))properties.add(new Ontology.PropertyMapping(required(p,"iri"),required(p,"label"),required(p,"property")));
    for(JsonNode r:o.path("relationships"))relationships.add(new Ontology.RelationshipMapping(required(r,"iri"),required(r,"label"),required(r,"sourceLabel"),required(r,"targetLabel")));
    return new OrchidQueries(new GraphMapping(nodes,edges),new Ontology(classes,properties,relationships),projections,language,text);
  }

  private static Projection projection(JsonNode node,String view,Set<String> columns,String id,boolean edge,Set<String> sources) {
    String source=required(node,"source");
    if(!sources.contains(source))throw new IllegalArgumentException("Graph mapping must use a configured source alias");
    return new Projection(view,source,Collections.unmodifiableSet(columns),id,edge);
  }
  void validatePolicies(Map<String,IcebergQueries.Source> sources) {
    for(var p:projections) {
      var source=sources.get(p.source());
      if(source==null||source.policy()==null||p.edge()&&source.policy().targetEntityColumn()==null)
        throw new SecurityException("Graph sources require registered policies; edges require both endpoint labels");
    }
  }
  private static Map<String,String> properties(JsonNode node) {
    var result=new LinkedHashMap<String,String>();
    if(node.has("properties")&&!node.path("properties").isObject())throw new IllegalArgumentException("Graph properties must be an object");
    node.path("properties").fields().forEachRemaining(e->result.put(e.getKey(),required(node.path("properties"),e.getKey())));
    return result;
  }
  private static String required(JsonNode node,String key) {
    JsonNode value=node.path(key);
    if(!value.isTextual()||value.asText().isBlank()||value.asText().indexOf('\0')>=0)throw new IllegalArgumentException("Missing or invalid graph field: "+key);
    return value.asText();
  }
  private static String quote(String name){return "\""+name.replace("\"","\"\"")+"\"";}
  // Load once, and only when an OrchidDB query is configured; SQL-only deployments remain usable.
  private static class Compiler {static final NativeSqlCompiler INSTANCE=NativeSqlCompiler.load();}
  static void checkNative(){Objects.requireNonNull(Compiler.INSTANCE);}

  List<Map<String,Object>> execute(Connection connection,Map<String,Object> arguments,
      IcebergQueries.Limits limits,long deadline)throws Exception {
    IcebergQueries.checkDeadline(deadline);
    // OrchidDB binds Cypher values in its AST; never substitute caller text into a query.
    Query query=new Query(language,text,arguments,ontology);
    try(var statement=connection.createStatement()) {
      statement.setQueryTimeout(limits.timeoutSeconds());
      for(var p:projections) {
        statement.execute("CREATE TEMP VIEW "+quote(p.view())+" AS SELECT "+String.join(",",p.columns().stream().map(OrchidQueries::quote).toList())+" FROM "+quote(p.source()));
        try(var invalid=statement.executeQuery("SELECT 1 FROM "+quote(p.view())+" GROUP BY "+quote(p.id())+" HAVING "+quote(p.id())+" IS NULL OR count(*) > 1 LIMIT 1")) {
          if(invalid.next())throw new IllegalArgumentException("Graph identities must be unique and non-null within each label");
        }
      }
    }
    var db=new OrchidDB(Compiler.INSTANCE,PlanCache.none(),JdbcEngine.borrowed(ENGINE,SqlDialect.DUCKDB,connection));
    String sql=db.graph(mapping).plan(query).sql();
    IcebergQueries.checkDeadline(deadline);
    // Only native compiler output reaches this path. The database has no raw inputs or credentials.
    var rows=IcebergQueries.run(connection,new IcebergQueries.BoundSql(sql,List.of()),limits.maxRows(),deadline);
    if(!language.equals("sparql"))return rows;
    // SPARQL result labels include '?'; GraphQL fields use the bare variable name.
    var result=new ArrayList<Map<String,Object>>();
    for(var row:rows) {
      var normalized=new LinkedHashMap<String,Object>();
      for(var entry:row.entrySet()) {
        String name=entry.getKey().startsWith("?")?entry.getKey().substring(1):entry.getKey();
        if(normalized.containsKey(name))throw new IllegalArgumentException("Duplicate graph result field");
        normalized.put(name,entry.getValue());
      }
      result.add(normalized);
    }
    return result;
  }
}
