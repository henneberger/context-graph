package io.contextgraph.query;

import io.contextgraph.security.*;
import java.net.URI;
import java.time.*;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

/** An ephemeral BM25 index contains only this request's authorized, current documents. */
final class DocumentSearch {
  record Document(String id,String source,String kind,String title,String text,String url,String author,String container,String updated,String entity,String resource,String revision){}
  static List<Document> current(List<Map<String,Object>> rows,SecurityContext context,PermissionChecks permissions) throws Exception {
    Map<String,Map<String,Object>> latest=new HashMap<>();
    for(var row:rows) {
      IcebergQueries.validateLabel(context,row.get("resource_id"),row.get("entity_id"));
      if(!context.workspaceId().equals(row.get("workspace_id")))throw new SecurityException("Workspace mismatch");
      if(!(row.get("payload") instanceof Map<?,?> raw))continue;
      @SuppressWarnings("unchecked") var p=(Map<String,Object>)raw;
      String id=text(p,"documentId"),source=text(p,"source");
      if(id.isBlank()||!Set.of("github","slack").contains(source)||!source.equals(row.get("endpoint")))continue;
      String key=source+"\0"+id;
      var old=latest.get(key);
      if(old==null||version(row).compareTo(version(old))>0)latest.put(key,row);
    }
    List<Document> documents=new ArrayList<>();int bytes=0;
    for(var row:latest.values()) {
      @SuppressWarnings("unchecked") var p=(Map<String,Object>)row.get("payload");
      if(Boolean.TRUE.equals(p.get("deleted")))continue;
      String source=text(p,"source"),url=text(p,"url");URI uri;
      try{uri=URI.create(url);}catch(Exception e){throw new SecurityException("Invalid citation");}
      if(!"https".equals(uri.getScheme())||uri.getUserInfo()!=null||uri.getPort()!=-1||uri.getHost()==null||!(source.equals("github")?uri.getHost().equals("github.com"):uri.getHost().endsWith(".slack.com")))throw new SecurityException("Invalid citation");
      String body=text(p,"text"),title=text(p,"title");bytes+=body.getBytes(StandardCharsets.UTF_8).length+title.getBytes(StandardCharsets.UTF_8).length;
      if(documents.size()>=10000||bytes>16*1024*1024)throw new IllegalStateException("Search corpus bound exceeded");
      String revision=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(IcebergQueries.JSON.writeValueAsBytes(p)));
      documents.add(new Document(text(p,"documentId"),source,text(p,"documentType"),title,body,url,text(p,"author"),text(p,"container"),text(p,"updatedAt"),(String)row.get("entity_id"),(String)row.get("resource_id"),revision));
    }
    permissions.workspace(context,"access");for(String resource:documents.stream().map(Document::resource).collect(java.util.stream.Collectors.toSet()))permissions.require(context,resource);
    return documents;
  }
  private static String version(Map<String,Object> row){@SuppressWarnings("unchecked") var p=(Map<String,Object>)row.get("payload");return text(p,"observedAt")+"|"+Objects.toString(row.get("ingested_at"),"")+"|"+row.get("event_id");}
  private static String text(Map<String,Object> p,String key){return Objects.toString(p.get(key),"");}
  static List<Map<String,Object>> execute(List<Map<String,Object>> rows,Map<String,Object> args,boolean byIds,SecurityContext context,PermissionChecks permissions) throws Exception {
    return execute(rows,args,byIds,context,permissions,IcebergQueries.JSON.createObjectNode());
  }
  static double freshness(String timestamp,double halfLifeDays,Instant now) {
    try {Instant updated=Instant.parse(timestamp);if(updated.isAfter(now.plusSeconds(86400)))return 0;double age=Math.max(0,Duration.between(updated,now).toSeconds()/86400d);return Math.exp(-Math.log(2)*age/halfLifeDays);}catch(Exception e){return 0;}
  }
  static List<Map<String,Object>> execute(List<Map<String,Object>> rows,Map<String,Object> args,boolean byIds,SecurityContext context,PermissionChecks permissions,JsonNode ranking) throws Exception {
    var docs=current(rows,context,permissions);List<Map<String,Object>> results=new ArrayList<>();
    if(byIds){Object value=args.get("ids");if(!(value instanceof List<?> ids)||ids.size()>30)throw new IllegalArgumentException("At most 30 document IDs");for(var d:docs)if(ids.contains(d.id()))results.add(result(d,0,""));}
    else {
      String query=Objects.toString(args.get("query"),"").strip();if(query.isBlank()||query.length()>500)throw new IllegalArgumentException("Query must contain 1–500 characters");
      String source=Objects.toString(args.get("source"),"all");if(!Set.of("all","slack","github").contains(source))throw new IllegalArgumentException("Invalid source");
      int limit=Math.min(30,Math.max(1,((Number)args.getOrDefault("limit",10)).intValue()));
      int sinceDays=((Number)args.getOrDefault("sinceDays",0)).intValue();if(!Set.of(0,7,30,90,365).contains(sinceDays))throw new IllegalArgumentException("Invalid date filter");
      String sort=Objects.toString(args.get("sort"),"relevance");if(!Set.of("relevance","recent").contains(sort))throw new IllegalArgumentException("Invalid sort");
      Instant now=Instant.now();double recencyWeight=ranking.path("recencyWeight").asDouble(.35),titleWeight=ranking.path("titleBoost").asDouble(.2);
      if(recencyWeight<0||recencyWeight>1||titleWeight<0||titleWeight>1)throw new IllegalArgumentException("Invalid ranking weights");
      if(!docs.isEmpty())try(Connection c=IcebergQueries.connect(false);Statement s=c.createStatement()) {
        String directory=System.getenv().getOrDefault("DUCKDB_EXTENSION_DIRECTORY",Path.of(System.getProperty("java.io.tmpdir"),"context-graph-duckdb-extensions").toString());
        s.execute("SET extension_directory='"+directory.replace("'","''")+"'; LOAD fts; SET memory_limit='128MB'; SET threads=1; SET temp_directory=''; SET enable_external_access=false;");
        s.execute("CREATE TABLE documents(id INTEGER, title VARCHAR, body VARCHAR)");
        try(var insert=c.prepareStatement("INSERT INTO documents VALUES (?,?,?)")){for(int i=0;i<docs.size();i++){var d=docs.get(i);if(!source.equals("all")&&!source.equals(d.source()))continue;if(sinceDays>0){try{if(Instant.parse(d.updated()).isBefore(now.minusSeconds(sinceDays*86400L)))continue;}catch(Exception e){continue;}}insert.setInt(1,i);insert.setString(2,d.title());insert.setString(3,d.text());insert.addBatch();}insert.executeBatch();}
        // Even corpus statistics and document-frequency counts contain only authorized documents.
        s.execute("PRAGMA create_fts_index('documents','id','title','body',stemmer='porter',stopwords='english',overwrite=1)");
        s.execute("SET lock_configuration=true");
        try(var q=c.prepareStatement("SELECT id,fts_main_documents.match_bm25(id,?) AS score FROM documents WHERE score IS NOT NULL ORDER BY score DESC,id")){
          q.setQueryTimeout(20);q.setString(1,query);try(var r=q.executeQuery()){while(r.next()){
            var d=docs.get(r.getInt(1));double base=r.getDouble(2);
            double halfLife=ranking.path("halfLifeDays").path(d.kind()).asDouble(30),type=ranking.path("typeWeights").path(d.kind()).asDouble(1);
            if(halfLife<1||halfLife>3650||type<.1||type>3)throw new IllegalArgumentException("Invalid ranking configuration");
            double recency=recencyWeight*freshness(d.updated(),halfLife,now),title=d.title().toLowerCase(Locale.ROOT).contains(query.toLowerCase(Locale.ROOT))?titleWeight:0;
            var hit=result(d,base*(1+recency)*(1+title)*type,query);hit.put("bm25Score",base);hit.put("recencyBoost",recency);hit.put("titleBoost",title);hit.put("typeWeight",type);results.add(hit);
          }}
          Comparator<Map<String,Object>> byScore=Comparator.comparingDouble(h->-((Number)h.get("score")).doubleValue());
          if(sort.equals("recent"))results.sort(Comparator.<Map<String,Object>,String>comparing(h->h.get("updatedAt").toString()).reversed().thenComparing(byScore));else results.sort(byScore.thenComparing(h->h.get("id").toString()));
          if(results.size()>limit)results=new ArrayList<>(results.subList(0,limit));
        }
      }
    }
    // Check the entire authorized corpus again: ranking statistics must not retain revoked inputs.
    permissions.workspace(context,"access");for(String resource:docs.stream().map(Document::resource).collect(java.util.stream.Collectors.toSet()))permissions.require(context,resource);
    return results;
  }
  static Map<String,Object> result(Document d,double score,String query){
    String excerpt=d.text();int pos=0;
    for(String term:query.toLowerCase(Locale.ROOT).split("\\W+")){if(term.length()<3)continue;int found=excerpt.toLowerCase(Locale.ROOT).indexOf(term);if(found>=0){pos=Math.max(0,found-100);break;}}
    excerpt=(pos>0?"…":"")+excerpt.substring(pos,Math.min(excerpt.length(),pos+600))+(excerpt.length()>pos+600?"…":"");
    Map<String,Object> result=new LinkedHashMap<>();result.put("id",d.id());result.put("source",d.source());result.put("documentType",d.kind());result.put("title",d.title());result.put("snippet",excerpt);result.put("content",d.text().substring(0,Math.min(8000,d.text().length())));result.put("url",d.url());result.put("author",d.author());result.put("container",d.container());result.put("updatedAt",d.updated());result.put("score",score);result.put("revision",d.revision());return result;
  }
}
