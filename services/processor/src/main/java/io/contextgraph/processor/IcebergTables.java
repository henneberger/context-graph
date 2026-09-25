package io.contextgraph.processor;
import java.util.Map;
import org.apache.iceberg.flink.CatalogLoader;
/** Catalog access is infrastructure; table schemas come from JSON Schema and SQL. */
public final class IcebergTables {
  public static CatalogLoader loader(String warehouse) {
    String uri=System.getenv("POLARIS_URI");
    if(uri!=null) {
      if(!uri.startsWith("https://"))throw new IllegalArgumentException("Polaris requires TLS");
      return CatalogLoader.rest("context",new org.apache.hadoop.conf.Configuration(),Map.of(
        "uri",uri,"warehouse",System.getenv().getOrDefault("POLARIS_WAREHOUSE","context"),
        "credential",java.util.Objects.requireNonNull(System.getenv("POLARIS_CREDENTIAL")),
        "oauth2-server-uri",uri+"/v1/oauth/tokens","scope","PRINCIPAL_ROLE:ALL",
        "header.X-Iceberg-Access-Delegation","vended-credentials","io-impl","org.apache.iceberg.aws.s3.S3FileIO"));
    }
    return CatalogLoader.hadoop("context", new org.apache.hadoop.conf.Configuration(),
        Map.of("warehouse", warehouse));
  }
}
