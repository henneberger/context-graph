package io.contextgraph.query;
import io.contextgraph.security.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
/** Operator-only smoke probe. Emits counts, never catalog or object credentials. */
public final class CatalogProbe {
 public static void main(String[] args)throws Exception {
  var db=new IcebergQueries(Path.of("/unused"));var tables=db.discover();var checks=PermissionChecks.using(AccessControl.fromEnvironment());
  var limits=new IcebergQueries.Limits(100000,10000,"64MB",30);
  for(String user:List.of("alice","bob","admin")) {
   var context=new SecurityContext(user,"demo",Instant.now().getEpochSecond()+300);
   var table=tables.get("context_secure.events");
   var result=db.execute(table,new IcebergQueries.Policy("entity_id",null),"SELECT count(*) AS n FROM source",Map.of(),context,checks,limits);
   System.out.println(user+" "+result);
  }
  var sources=Map.of("nodes",new IcebergQueries.Source(tables.get("context_secure.nodes"),new IcebergQueries.Policy("node_id",null)),"metrics",new IcebergQueries.Source(tables.get("context_secure.metrics"),new IcebergQueries.Policy("entity_id",null)));
  System.out.println("federation "+db.federate(sources,"SELECT count(*) AS n FROM nodes n JOIN metrics m ON n.node_id=m.entity_id",Map.of(),new SecurityContext("alice","demo",Instant.now().getEpochSecond()+300),checks,limits));
 }
}
