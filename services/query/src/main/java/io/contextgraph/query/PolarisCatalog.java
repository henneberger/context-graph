package io.contextgraph.query;

import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;

/** Private service identity. No catalog tokens, locations or storage credentials are returned to users. */
final class PolarisCatalog {
  private static final ObjectMapper JSON=new ObjectMapper();
  private final String endpoint,warehouse,clientId,clientSecret,namespace,caFile;
  private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
  PolarisCatalog(String endpoint,String warehouse,String clientId,String clientSecret,String namespace,String caFile) {
    if(!endpoint.startsWith("https://"))throw new IllegalArgumentException("Polaris requires TLS");
    this.endpoint=endpoint.replaceAll("/+$","");this.warehouse=warehouse;this.clientId=clientId;this.clientSecret=clientSecret;this.namespace=namespace;this.caFile=caFile;
    relation(namespace+".table");
  }
  static PolarisCatalog fromEnvironment() {
    String endpoint=System.getenv("POLARIS_URI");if(endpoint==null)return null;
    return new PolarisCatalog(endpoint,required("POLARIS_WAREHOUSE"),required("POLARIS_CLIENT_ID"),required("POLARIS_CLIENT_SECRET"),System.getenv().getOrDefault("POLARIS_NAMESPACE","context_secure"),required("POLARIS_CA_FILE"));
  }
  static String required(String key){String value=System.getenv(key);if(value==null||value.isBlank())throw new IllegalStateException("Missing "+key);return value;}
  static String quote(String value){return "'"+value.replace("'","''")+"'";}
  static String relation(String name){if(!name.matches("[a-z_][a-z0-9_]*\\.[a-z_][a-z0-9_]*"))throw new SecurityException("Invalid catalog relation");return String.join(".",Arrays.stream(name.split("\\.")).map(s->"\""+s+"\"").toList());}
  static String encode(String s){return URLEncoder.encode(s,StandardCharsets.UTF_8);}
  private JsonNode response(HttpRequest request)throws Exception {
    var response=http.send(request,HttpResponse.BodyHandlers.ofInputStream());
    try(var body=response.body()){
      if(response.statusCode()!=200)throw new IllegalStateException("Polaris request failed ("+response.statusCode()+")");
      byte[] bytes=body.readNBytes(8*1024*1024+1);if(bytes.length>8*1024*1024)throw new IllegalStateException("Catalog response too large");return JSON.readTree(bytes);
    }
  }
  private String token()throws Exception {
    var request=HttpRequest.newBuilder(URI.create(endpoint+"/v1/oauth/tokens")).timeout(Duration.ofSeconds(10)).header("Content-Type","application/x-www-form-urlencoded")
      .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope=PRINCIPAL_ROLE%3AALL&client_id="+encode(clientId)+"&client_secret="+encode(clientSecret))).build();
    String token=response(request).path("access_token").asText();if(token.isBlank())throw new IllegalStateException("Missing catalog token");return token;
  }
  private JsonNode get(String path,String token)throws Exception {return response(HttpRequest.newBuilder(URI.create(endpoint+path)).timeout(Duration.ofSeconds(10)).header("Authorization","Bearer "+token).GET().build());}
  Map<String,IcebergQueries.Table> discover()throws Exception {
    String token=token();var config=get("/v1/config?warehouse="+encode(warehouse),token);
    String prefix=config.path("overrides").path("prefix").asText(config.path("defaults").path("prefix").asText(warehouse));
    String base="/v1/"+encode(prefix)+"/namespaces/"+encode(namespace)+"/tables";
    Map<String,IcebergQueries.Table> tables=new TreeMap<>();String page="";Set<String> pages=new HashSet<>();
    do {
      var listing=get(base+"?pageSize=100"+(page.isEmpty()?"":"&pageToken="+encode(page)),token);
      for(var id:listing.path("identifiers")) {
        String name=id.path("name").asText();relation(namespace+"."+name);
        JsonNode metadata=get(base+"/"+encode(name),token).path("metadata"),schema=null;
        for(var candidate:metadata.path("schemas"))if(candidate.path("schema-id").asInt()==metadata.path("current-schema-id").asInt())schema=candidate;
        if(schema==null)throw new IllegalStateException("Missing current schema");
        var columns=new ArrayList<IcebergQueries.Column>();
        for(var field:schema.path("fields"))columns.add(new IcebergQueries.Column(field.path("name").asText(),field.path("type").isTextual()?field.path("type").asText():field.path("type").toString(),!field.path("required").asBoolean()));
        String qualified=namespace+"."+name;tables.put(qualified,new IcebergQueries.Table(qualified,"polaris",List.copyOf(columns)));
        if(tables.size()>1000)throw new IllegalStateException("Catalog table limit exceeded");
      }
      page=listing.path("next-page-token").asText("");if(!page.isEmpty()&&!pages.add(page))throw new IllegalStateException("Repeated catalog page");
    }while(!page.isEmpty());
    return Map.copyOf(tables);
  }
  void attach(Connection connection)throws Exception {
    try(Statement s=connection.createStatement()) {
      s.execute("SET ca_cert_file="+quote(caFile));
      // Only a short-lived catalog token is installed; DuckDB requests table-scoped STS credentials itself.
      s.execute("CREATE SECRET polaris_session (TYPE ICEBERG, TOKEN "+quote(token())+")");
      s.execute("ATTACH "+quote(warehouse)+" AS lake (TYPE ICEBERG, ENDPOINT "+quote(endpoint)+", SECRET polaris_session, ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)");
    }
  }
}
