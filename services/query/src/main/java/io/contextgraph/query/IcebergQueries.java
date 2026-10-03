package io.contextgraph.query;

import com.fasterxml.jackson.databind.*;
import io.contextgraph.security.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

/** Only committed Hadoop-catalog versions are exposed; no lexicographic metadata guessing. */
public final class IcebergQueries {
  static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  public record Column(String name, String type, boolean nullable) {}
  public record Table(String name, String metadata, List<Column> columns) {}
  public record BoundSql(String sql, List<Object> values) {}
  public record Policy(String entityColumn, String targetEntityColumn) {}
  public record Limits(int maxRows,int maxResources,String memoryLimit,int timeoutSeconds) {
    public Limits {if(maxRows<1||maxResources<1||timeoutSeconds<1||!memoryLimit.matches("[0-9]+(MB|GB)"))throw new IllegalArgumentException("Invalid query limits");}
  }
  private final Path warehouse;
  private final PolarisCatalog polaris=PolarisCatalog.fromEnvironment();
  public IcebergQueries(Path warehouse) { this.warehouse=warehouse; }
  public Map<String,Table> discover() throws Exception {
    if(polaris!=null)return polaris.discover();
    Map<String,Table> result=new TreeMap<>();
    if (!Files.exists(warehouse)) return result;
    try(var paths=Files.walk(warehouse,8)) {
      for(Path hint: paths.filter(p->p.getFileName().toString().equals("version-hint.text")).toList()) {
        String version=Files.readString(hint).trim();
        if(!version.matches("[0-9]+")) throw new IllegalStateException("Invalid Iceberg version hint: "+hint);
        Path metadata=hint.resolveSibling("v"+version+".metadata.json");
        // HadoopCatalog commits metadata by atomic rename before updating its version hint.
        if(!Files.isRegularFile(metadata)) throw new IllegalStateException("Committed metadata absent: "+metadata);
        JsonNode root=JSON.readTree(metadata.toFile());
        JsonNode schema=null;
        for(JsonNode candidate:root.path("schemas")) if(candidate.path("schema-id").asInt()==root.path("current-schema-id").asInt()) schema=candidate;
        if(schema==null) schema=root.path("schema");
        List<Column> columns=new ArrayList<>();
        for(JsonNode field:schema.path("fields")) columns.add(new Column(field.path("name").asText(),field.path("type").isTextual()?field.path("type").asText():field.path("type").toString(),!field.path("required").asBoolean()));
        String name=warehouse.relativize(hint.getParent().getParent()).toString().replace(java.io.File.separatorChar,'.');
        result.put(name,new Table(name,metadata.toString(),List.copyOf(columns)));
      }
    }
    return Map.copyOf(result);
  }
  public static Connection connect(boolean extensions) throws SQLException {
    Connection c=DriverManager.getConnection("jdbc:duckdb:");
    if(extensions) try(Statement s=c.createStatement()) {
      String directory=System.getenv().getOrDefault("DUCKDB_EXTENSION_DIRECTORY",Path.of(System.getProperty("java.io.tmpdir"),"context-graph-duckdb-extensions").toString());
      s.execute("SET extension_directory='"+directory.replace("'","''")+"'");
      s.execute("INSTALL iceberg; LOAD iceberg; INSTALL cache_httpfs FROM community; LOAD cache_httpfs; INSTALL fts; LOAD fts;");
    } catch(SQLException e) {c.close();throw e;}
    return c;
  }
  public static BoundSql bind(String sql,Map<String,Object> args) {
    StringBuilder out=new StringBuilder();List<Object> values=new ArrayList<>();
    boolean quoted=false;
    for(int i=0;i<sql.length();) {
      char ch=sql.charAt(i);
      if(ch=='\'') {out.append(ch);i++;if(quoted&&i<sql.length()&&sql.charAt(i)=='\''){out.append(sql.charAt(i++));continue;}quoted=!quoted;continue;}
      if(!quoted&&ch==':'&&i+1<sql.length()&&sql.charAt(i+1)==':'){out.append("::");i+=2;continue;}
      if(!quoted&&ch==':'&&i+1<sql.length()&&Character.isLetter(sql.charAt(i+1))) {
        int end=i+2;while(end<sql.length()&&(Character.isLetterOrDigit(sql.charAt(end))||sql.charAt(end)=='_'))end++;
        String name=sql.substring(i+1,end);Object value=args.get(name);
        if(name.equals("limit"))value=Math.min(10000,Math.max(1,value==null?100:((Number)value).intValue()));
        values.add(value);out.append('?');i=end;
      } else {out.append(ch);i++;}
    }
    return new BoundSql(out.toString(),values);
  }

  public static boolean registered(Table table, Policy policy) {
    if(policy==null)return false;
    var names=table.columns().stream().map(Column::name).collect(java.util.stream.Collectors.toSet());
    return names.containsAll(List.of("workspace_id","resource_id",policy.entityColumn())) &&
      (policy.targetEntityColumn()==null || names.containsAll(List.of("target_resource_id",policy.targetEntityColumn())));
  }
  public List<Map<String,Object>> execute(Table table,Policy policy,String sql,Map<String,Object> args,
      SecurityContext context,PermissionChecks permissions,Limits limits) throws Exception {
    SafeSql.validate(sql);permissions.workspace(context,"access");
    if(table==null)return List.of();
    if(!registered(table,policy))throw new SecurityException("Unregistered or unlabeled table");
    try(Connection c=connect(true);Statement view=c.createStatement()) {
      view.execute("SET memory_limit='"+limits.memoryLimit()+"'; SET threads=1; SET temp_directory='';");
      view.setQueryTimeout(limits.timeoutSeconds());
      if(polaris!=null) { polaris.attach(c); view.execute("CREATE VIEW raw_source AS SELECT * FROM lake."+PolarisCatalog.relation(table.name())); }
      else view.execute("CREATE VIEW raw_source AS SELECT * FROM iceberg_scan('"+table.metadata().replace("'","''")+"')");
      return authorizedQuery(c,policy,sql,args,context,permissions,limits);
    }
  }
  public record Source(Table table,Policy policy) {}
  @FunctionalInterface
  interface AuthorizedOperation { List<Map<String,Object>> run(Connection connection,long deadline) throws Exception; }
  /** Each connector materializes authorized rows before they enter the credential-free join engine. */
  public List<Map<String,Object>> federate(Map<String,Source> sources,String sql,Map<String,Object> args,
      SecurityContext context,PermissionChecks permissions,Limits limits) throws Exception {
    validateSources(sources.keySet()); SafeSql.validate(sql,sources.keySet());
    return withAuthorizedSources(sources,(c,deadline)->run(c,bind(sql,args)),context,permissions,limits);
  }
  List<Map<String,Object>> graph(Map<String,Source> sources,OrchidQueries query,Map<String,Object> args,
      SecurityContext context,PermissionChecks permissions,Limits limits)throws Exception {
    query.validatePolicies(sources);
    return withAuthorizedSources(sources,(c,deadline)->query.execute(c,args,limits,deadline),context,permissions,limits);
  }
  private List<Map<String,Object>> withAuthorizedSources(Map<String,Source> sources,AuthorizedOperation operation,
      SecurityContext context,PermissionChecks permissions,Limits limits)throws Exception {
    validateSources(sources.keySet());
    long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(limits.timeoutSeconds());
    Map<String,List<Map<String,Object>>> inputs=new LinkedHashMap<>();
    int total=0;
    for(var entry:sources.entrySet()) {
      checkDeadline(deadline);
      Source source=entry.getValue();
      if(source.table()==null||!registered(source.table(),source.policy()))throw new SecurityException("Federated source unavailable");
      var rows=execute(source.table(),source.policy(),"SELECT * FROM source",Map.of(),context,permissions,limits);
      total+=rows.size();if(total>limits.maxRows())throw new IllegalStateException("Federated row limit exceeded");
      inputs.put(entry.getKey(),rows);
    }
    checkDeadline(deadline);
    return joinAuthorized(sources,inputs,operation,context,permissions,limits,deadline);
  }
  static void validateSources(Set<String> names) {
    if(names.isEmpty()||names.size()>8)throw new IllegalArgumentException("One to eight sources required");
    for(String name:names)if(!name.matches("[a-z][a-z0-9_]{0,47}")||Set.of("main","temp","system","lake").contains(name))throw new IllegalArgumentException("Invalid source alias");
  }
  private static final class JsonType {static JsonNode parse(String value){try{return JSON.readTree(value);}catch(Exception e){throw new IllegalArgumentException(e);}}}
  static List<Map<String,Object>> joinAuthorized(Map<String,Source> sources,Map<String,List<Map<String,Object>>> inputs,
      String sql,Map<String,Object> args,SecurityContext context,PermissionChecks permissions,Limits limits,long deadline)throws Exception {
    SafeSql.validate(sql,sources.keySet());
    return joinAuthorized(sources,inputs,(c,end)->run(c,bind(sql,args)),context,permissions,limits,deadline);
  }
  static List<Map<String,Object>> joinAuthorized(Map<String,Source> sources,Map<String,List<Map<String,Object>>> inputs,
      AuthorizedOperation operation,SecurityContext context,PermissionChecks permissions,Limits limits,long deadline)throws Exception {
    validateSources(sources.keySet());permissions.workspace(context,"access");
    Set<String> contributors=new HashSet<>();int total=0;
    // A fresh database never receives catalog handles or storage credentials.
    try(Connection c=connect(false);Statement statement=c.createStatement()) {
      statement.execute("SET memory_limit='"+limits.memoryLimit()+"'; SET threads=1; SET temp_directory=''; SET enable_external_access=false;");
      for(var entry:sources.entrySet()) {
        String alias=entry.getKey();var source=entry.getValue();var columns=source.table().columns();
        if(!registered(source.table(),source.policy()))throw new SecurityException("Unregistered source");
        var definitions=new ArrayList<String>();
        for(var col:columns) {
          if(col.name().indexOf('\0')>=0)throw new SecurityException("Invalid column");
          var type=col.type().startsWith("{")?JSON.readTree(col.type()):JSON.getNodeFactory().textNode(col.type());
          definitions.add("\""+col.name().replace("\"","\"\"")+"\" "+ApiCompiler.sqlType(type));
        }
        statement.execute("CREATE TEMP TABLE "+alias+" ("+String.join(",",definitions)+")");
        var rows=inputs.get(alias);total+=rows.size();if(total>limits.maxRows())throw new IllegalStateException("Federated row limit exceeded");
        try(var insert=c.prepareStatement("INSERT INTO "+alias+" VALUES ("+String.join(",",columns.stream().map(col->(col.type().startsWith("{")||col.type().equals("variant"))?"CAST(?::JSON AS "+ApiCompiler.sqlType(col.type().startsWith("{")?JsonType.parse(col.type()):JSON.getNodeFactory().textNode(col.type()))+")":"?").toList())+")")) {
          for(var row:rows) {
            checkDeadline(deadline);
            if(!context.workspaceId().equals(row.get("workspace_id")))throw new SecurityException("Workspace mismatch");
            validateLabel(context,row.get("resource_id"),row.get(source.policy().entityColumn()));contributors.add((String)row.get("resource_id"));
            if(source.policy().targetEntityColumn()!=null){validateLabel(context,row.get("target_resource_id"),row.get(source.policy().targetEntityColumn()));contributors.add((String)row.get("target_resource_id"));}
            if(contributors.size()>limits.maxResources())throw new IllegalStateException("Federated resource limit exceeded");
            for(int i=0;i<columns.size();i++){Object value=row.get(columns.get(i).name());insert.setObject(i+1,(columns.get(i).type().startsWith("{")||columns.get(i).type().equals("variant"))?JSON.writeValueAsString(value):value);}insert.addBatch();
          }insert.executeBatch();
        }
      }
      for(String resource:contributors){checkDeadline(deadline);permissions.require(context,resource);}
      statement.execute("SET lock_configuration=true");
      var result=operation.run(c,deadline);
      permissions.workspace(context,"access");for(String resource:contributors){checkDeadline(deadline);permissions.require(context,resource);}
      checkDeadline(deadline);return result;
    }
  }
  /** The raw_source relation is trusted; it is destroyed before configurable SQL executes. */
  static List<Map<String,Object>> authorizedQuery(Connection c,Policy policy,String sql,Map<String,Object> args,
      SecurityContext context,PermissionChecks permissions,Limits limits) throws Exception {
    long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(limits.timeoutSeconds());
    SafeSql.validate(sql);permissions.workspace(context,"access");
    for(String identifier:List.of(policy.entityColumn(),policy.targetEntityColumn()==null?"unused":policy.targetEntityColumn()))
      if(!identifier.matches("[a-z_][a-z0-9_]*"))throw new SecurityException("Invalid security mapping");
    String target=policy.targetEntityColumn()==null?"NULL AS target_resource_id, NULL AS target_entity_id":"target_resource_id,"+policy.targetEntityColumn()+" AS target_entity_id";
    List<Map<String,Object>> candidates=run(c,new BoundSql("SELECT DISTINCT resource_id,"+policy.entityColumn()+" AS entity_id,"+target+" FROM raw_source WHERE workspace_id=? LIMIT ?",List.of(context.workspaceId(),limits.maxResources()+1)));
    if(candidates.size()>limits.maxResources())throw new IllegalStateException("Authorization resource limit exceeded");
    Set<String> resources=new HashSet<>();
    for(var row:candidates) {
      validateLabel(context,row.get("resource_id"),row.get("entity_id"));resources.add((String)row.get("resource_id"));
      if(policy.targetEntityColumn()!=null){validateLabel(context,row.get("target_resource_id"),row.get("target_entity_id"));resources.add((String)row.get("target_resource_id"));}
    }
    if(resources.size()>limits.maxResources())throw new IllegalStateException("Authorization resource limit exceeded");
    Set<String> authorized=new HashSet<>();for(String resource:resources){checkDeadline(deadline);if(permissions.allowed(context,resource))authorized.add(resource); }
    try(Statement statement=c.createStatement()) {
      statement.setQueryTimeout(limits.timeoutSeconds());statement.execute("CREATE TEMP TABLE authorized_resources(resource_id VARCHAR PRIMARY KEY)");
      try(PreparedStatement p=c.prepareStatement("INSERT INTO authorized_resources VALUES (?)")){for(String resource:authorized){p.setString(1,resource);p.addBatch();}p.executeBatch();}
      String filter="workspace_id=? AND resource_id IN (SELECT resource_id FROM authorized_resources)"+
        (policy.targetEntityColumn()==null?"":" AND target_resource_id IN (SELECT resource_id FROM authorized_resources)");
      var count=run(c,new BoundSql("SELECT count(*) AS n FROM (SELECT 1 FROM raw_source WHERE "+filter+" LIMIT ?)",List.of(context.workspaceId(),limits.maxRows()+1)));
      if(((Number)count.getFirst().get("n")).longValue()>limits.maxRows())throw new IllegalStateException("Authorized source row limit exceeded");
      try(PreparedStatement p=c.prepareStatement("CREATE TEMP TABLE source AS SELECT * FROM raw_source WHERE "+filter)){p.setQueryTimeout(limits.timeoutSeconds());p.setString(1,context.workspaceId());p.execute();}
      statement.execute("DROP VIEW raw_source; DROP TABLE authorized_resources;");
      boolean attached;
      try(var databases=statement.executeQuery("SELECT count(*) FROM duckdb_databases() WHERE database_name='lake'")){databases.next();attached=databases.getInt(1)>0;}
      if(attached)statement.execute("DETACH lake; DROP SECRET IF EXISTS polaris_session;");
      statement.execute("SET enable_external_access=false; SET lock_configuration=true;");
    }
    List<Map<String,Object>> result=run(c,bind(sql,args));
    permissions.workspace(context,"access");for(String resource:authorized){checkDeadline(deadline);permissions.require(context,resource); }
    checkDeadline(deadline);
    return result;
  }
  static void checkDeadline(long deadline){if(System.nanoTime()>deadline)throw new IllegalStateException("Authorization query deadline exceeded");}
  static void validateLabel(SecurityContext context,Object resource,Object entity) {
    if(!(entity instanceof String id)||!(resource instanceof String key)||!AccessControl.resourceId(context.workspaceId(),id).equals(key)) throw new SecurityException("Invalid resource provenance");
  }
  static Object normalizeValue(Object value) {
    if(value instanceof java.sql.Timestamp t) return t.toInstant().toString();
    if(value instanceof java.time.OffsetDateTime t) return t.toInstant().toString();
    if(value instanceof java.time.ZonedDateTime t) return t.toInstant().toString();
    if(value instanceof java.time.LocalDateTime t) return t.toInstant(java.time.ZoneOffset.UTC).toString();
    if(value instanceof java.time.temporal.TemporalAccessor) return value.toString();
    if(value instanceof org.duckdb.DuckDBStruct struct) {try{return normalizeValue(struct.getMap());}catch(SQLException e){throw new IllegalArgumentException(e);}}
    if(value instanceof java.sql.Array array) {try{return normalizeValue(array.getArray());}catch(SQLException e){throw new IllegalArgumentException(e);}}
    if(value instanceof Map<?,?> map){var out=new LinkedHashMap<String,Object>();map.forEach((k,v)->out.put(k.toString(),normalizeValue(v)));return out;}
    if(value instanceof Object[] array)return Arrays.stream(array).map(IcebergQueries::normalizeValue).toList();

    return value;
  }
  static List<Map<String,Object>> run(Connection c,BoundSql bound) throws Exception {
    return run(c,bound,Integer.MAX_VALUE,System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
  }
  static List<Map<String,Object>> run(Connection c,BoundSql bound,int maxRows,long deadline) throws Exception {
    checkDeadline(deadline);
    try(PreparedStatement p=c.prepareStatement(bound.sql())) {
      p.setQueryTimeout((int)Math.max(1,java.util.concurrent.TimeUnit.NANOSECONDS.toSeconds(deadline-System.nanoTime())+1));
      for(int i=0;i<bound.values().size();i++) p.setObject(i+1,bound.values().get(i));
      try(ResultSet rs=p.executeQuery()) {
        List<Map<String,Object>> rows=new ArrayList<>(); ResultSetMetaData meta=rs.getMetaData();
        while(rs.next()) {checkDeadline(deadline);if(rows.size()>=maxRows)throw new IllegalStateException("Query result row limit exceeded");Map<String,Object> row=new LinkedHashMap<>();for(int i=1;i<=meta.getColumnCount();i++) {
          Object value=rs.getObject(i);String name=meta.getColumnLabel(i);
          value=normalizeValue(value);
          if(value instanceof String s && (name.equals("payload")||name.equals("properties"))) {try {value=JSON.readValue(s,Object.class);}catch(Exception ignored){}}
          row.put(name,value);
        }rows.add(row);}return rows;
      }
    }
  }
}
