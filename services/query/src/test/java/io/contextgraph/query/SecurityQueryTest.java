package io.contextgraph.query;

import io.contextgraph.security.*;
import org.junit.jupiter.api.Test;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SecurityQueryTest {
  final SecurityContext alice=new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()+300);
  final String a=AccessControl.resourceId("workspace-a","a"),b=AccessControl.resourceId("workspace-a","b");
  final IcebergQueries.Limits limits=new IcebergQueries.Limits(100,100,"64MB",5);
  final IcebergQueries.Policy policy=new IcebergQueries.Policy("entity_id",null);
  class Grants implements PermissionChecks {
    Set<String> allowed=new HashSet<>(Set.of(a));boolean outage=false,revoked=false;int calls=0;boolean revokeAfterChecks=false;
    public void workspace(SecurityContext context,String permission){if(outage)throw new AuthException(503,"Unavailable");if(context.expiresAtEpochSecond()<=Instant.now().getEpochSecond())throw new AuthException(401,"Expired");if(revoked)throw new AuthException(403,"Membership revoked");}
    public boolean allowed(SecurityContext context,String resource){workspace(context,"access");calls++;return allowed.contains(resource)&&(!revokeAfterChecks||calls<=2);}
  }
  Connection data() throws Exception {
    Connection c=IcebergQueries.connect(false);
    try(Statement s=c.createStatement()){s.execute("CREATE VIEW raw_source AS SELECT * FROM (VALUES ('workspace-a','"+a+"','a',10),('workspace-a','"+b+"','b',100),('workspace-b','"+AccessControl.resourceId("workspace-b","a")+"','a',1000)) AS rows(workspace_id,resource_id,entity_id,value)");}
    return c;
  }
  @Test void authorizationPrecedesAggregationAndWorkspaceIsolation() throws Exception {
    var grants=new Grants();
    try(var c=data()) {
      var result=IcebergQueries.authorizedQuery(c,policy,"SELECT SUM(value) AS total, count(*) AS n FROM source",Map.of(),alice,grants,limits);
      assertEquals(10,((Number)result.getFirst().get("total")).intValue());assertEquals(1,((Number)result.getFirst().get("n")).intValue());
      try(var s=c.createStatement()){assertThrows(SQLException.class,()->s.execute("SELECT * FROM read_csv('/etc/passwd')"));}
    }
    grants.allowed=new HashSet<>(Set.of(b));
    try(var c=data()){assertEquals(100,((Number)IcebergQueries.authorizedQuery(c,policy,"SELECT SUM(value) AS total FROM source",Map.of(),alice,grants,limits).getFirst().get("total")).intValue());}
  }
  @Test void deniedEdgeTargetCannotContributeToAggregate() throws Exception {
    try(var c=IcebergQueries.connect(false);var s=c.createStatement()) {
      s.execute("CREATE VIEW raw_source AS SELECT 'workspace-a' workspace_id,'"+a+"' resource_id,'a' source_id,'"+b+"' target_resource_id,'b' target_id");
      var result=IcebergQueries.authorizedQuery(c,new IcebergQueries.Policy("source_id","target_id"),"SELECT count(*) AS n FROM source",Map.of(),alice,new Grants(),limits);
      assertEquals(0,((Number)result.getFirst().get("n")).intValue());
    }
  }
  @Test void revokeDuringQueryFailsBeforeResultReturns() throws Exception {
    var grants=new Grants();grants.revokeAfterChecks=true;
    try(var c=data()){assertThrows(SecurityException.class,()->IcebergQueries.authorizedQuery(c,policy,"SELECT SUM(value) AS total FROM source",Map.of(),alice,grants,limits));}
  }
  @Test void oversizedSourcesFailRatherThanTruncateAggregation() throws Exception {
    var grants=new Grants();grants.allowed.add(b);
    try(var c=data()){assertThrows(IllegalStateException.class,()->IcebergQueries.authorizedQuery(c,policy,"SELECT SUM(value) AS total FROM source",Map.of(),alice,grants,new IcebergQueries.Limits(1,100,"64MB",5)));}
    try(var c=data()){assertThrows(IllegalStateException.class,()->IcebergQueries.authorizedQuery(c,policy,"SELECT SUM(value) AS total FROM source",Map.of(),alice,grants,new IcebergQueries.Limits(100,1,"64MB",5)));}
  }
  @Test void configurableSqlCannotBypassAuthorizedSource() throws Exception {
    for(String sql:List.of("SELECT * FROM iceberg_scan('/secret')","SELECT * FROM read_csv('/etc/passwd')","SELECT * FROM source; SELECT 1","COPY source TO '/tmp/leak'","SELECT * FROM information_schema.tables","SELECT query('SELECT * FROM raw_source')"))assertThrows(IllegalArgumentException.class,()->SafeSql.validate(sql),sql);
    var config=QueryService.yaml(java.nio.file.Path.of("../../config/queries.yaml"));config.path("queries").forEach(q->{Set<String> aliases=new HashSet<>();if(q.has("sources"))q.path("sources").fieldNames().forEachRemaining(aliases::add);else aliases.add("source");SafeSql.validate(q.path("sql").asText(),aliases);});
    SafeSql.validate("WITH scoped AS (SELECT value FROM source) SELECT SUM(value) FROM scoped");
  }
  @Test void unlabeledUnregisteredTablesAreDenied(){
    var table=new IcebergQueries.Table("legacy.events","/private/path",List.of(new IcebergQueries.Column("entity_id","string",false)));
    assertFalse(IcebergQueries.registered(table,policy));assertFalse(IcebergQueries.registered(table,null));
  }
  @Test void perEmissionChecksHandleRevocationExpiryAndOutage(){
    var grants=new Grants();var event=new LiveBus.Event("cg.secure.metrics",Map.of("workspace_id","workspace-a","resource_id",a,"entity_id","a"));
    assertTrue(EventAuthorization.visible(event,alice,grants,null));grants.allowed.clear();assertFalse(EventAuthorization.visible(event,alice,grants,null));assertThrows(AuthException.class,()->EventAuthorization.visible(event,alice,grants,"a"));
    grants.allowed.add(a);var expired=new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()-1);assertThrows(AuthException.class,()->EventAuthorization.visible(event,expired,grants,null));
    grants.outage=true;assertThrows(AuthException.class,()->EventAuthorization.visible(event,alice,grants,null));grants.outage=false;
  }
  @Test void federatedJoinRechecksEverySourceAndRejectsBypasses()throws Exception {
    var columns=List.of(new IcebergQueries.Column("workspace_id","string",false),new IcebergQueries.Column("resource_id","string",false),new IcebergQueries.Column("entity_id","string",false),new IcebergQueries.Column("value","long",false));
    var table=new IcebergQueries.Table("context_secure.test","unused",columns);
    var sources=Map.of("nodes",new IcebergQueries.Source(table,policy),"metrics",new IcebergQueries.Source(table,policy));
    Map<String,Object> row=Map.of("workspace_id","workspace-a","resource_id",a,"entity_id","a","value",10);
    var inputs=Map.of("nodes",List.of(row),"metrics",List.of(row));
    String sql="SELECT sum(m.value) AS total FROM nodes n JOIN metrics m ON n.entity_id=m.entity_id";
    var grants=new Grants();long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
    assertEquals(10,((Number)IcebergQueries.joinAuthorized(sources,inputs,sql,Map.of(),alice,grants,limits,deadline).getFirst().get("total")).intValue());
    grants.allowed.clear();assertThrows(SecurityException.class,()->IcebergQueries.joinAuthorized(sources,inputs,sql,Map.of(),alice,grants,limits,deadline));
    grants.allowed.add(a);grants.revokeAfterChecks=true;grants.calls=1;
    assertThrows(SecurityException.class,()->IcebergQueries.joinAuthorized(sources,inputs,sql,Map.of(),alice,grants,limits,deadline));
    for(String bypass:List.of("SELECT * FROM lake.context_secure.events","SELECT * FROM duckdb_secrets()","SELECT * FROM read_parquet('s3://private/file')","ATTACH 'catalog' AS raw"))assertThrows(IllegalArgumentException.class,()->SafeSql.validate(bypass,sources.keySet()));
    assertThrows(IllegalStateException.class,()->IcebergQueries.joinAuthorized(sources,inputs,sql,Map.of(),alice,new Grants(),new IcebergQueries.Limits(1,100,"64MB",5),deadline));
  }

}
