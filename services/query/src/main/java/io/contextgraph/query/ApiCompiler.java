package io.contextgraph.query;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Build-time generation using engine metadata on empty typed tables, never sampled user data. */
public final class ApiCompiler {
  private static final ObjectMapper JSON=new ObjectMapper();
  static String name(String s){if(!s.matches("[A-Za-z][A-Za-z0-9_]{0,63}"))throw new IllegalArgumentException("Invalid API identifier");return s;}
  static String sqlType(String type){return switch(type){case "int"->"INTEGER";case "long"->"BIGINT";case "float","double"->"DOUBLE";case "boolean"->"BOOLEAN";case "timestamp","timestamptz"->"TIMESTAMP";case "string"->"VARCHAR";default->throw new IllegalArgumentException("Unsupported output type");};}
  static String graphType(String type){return switch(type){case "int"->"Int";case "long"->"Long";case "float","double"->"Float";case "boolean"->"Boolean";default->"String";};}
  public static void main(String[] args)throws Exception {
    JsonNode config=JSON.readTree(Path.of(args[0]).toFile());ObjectNode out=JSON.createObjectNode();
    ObjectNode registered=out.putObject("registeredTables"),queries=out.putObject("queries"),subscriptions=out.putObject("subscriptions"),mutations=out.putObject("mutations");
    String namespace=config.path("namespace").asText();StringBuilder types=new StringBuilder("scalar JSON\nscalar Long\ntype CommandReceipt { eventId: String! status: String! }\n");
    var views=config.path("views");
    for(var iter=views.fields();iter.hasNext();) {var view=iter.next();name(view.getKey());registered.putObject(namespace+"."+view.getKey()).put("entityColumn","entity_id");}
    for(var iter=config.path("api").path("queries").fields();iter.hasNext();) {
      var entry=iter.next();String field=name(entry.getKey());JsonNode q=entry.getValue(),view=views.get(q.path("view").asText());if(view==null)throw new IllegalArgumentException("Unknown query view");
      String sql=q.path("sql").asText();SafeSql.validate(sql);
      StringBuilder parameters=new StringBuilder();Map<String,Object> samples=new HashMap<>();
      for(var ps=q.path("parameters").fields();ps.hasNext();) {
        var param=ps.next();String pn=name(param.getKey()),pt=param.getValue().path("type").asText();boolean required=param.getValue().path("required").asBoolean();
        String gt=switch(pt){case "int"->"Int";case "long"->"Long";case "double"->"Float";case "boolean"->"Boolean";case "string"->"String";default->throw new IllegalArgumentException("Parameter type required");};
        if(parameters.length()>0)parameters.append(", ");parameters.append(pn).append(": ").append(gt).append(required?"!":"");
        if(param.getValue().has("default"))parameters.append(" = ").append(param.getValue().get("default").toString());
        samples.put(pn,switch(pt){case "int","long"->1;case "double"->1.0;case "boolean"->false;default->"";});
      }
      String outputType="Result_"+field;types.append("type ").append(outputType).append(" {\n");
      try(Connection c=IcebergQueries.connect(false);Statement s=c.createStatement()) {
        var defs=new ArrayList<String>();for(var f:view.path("schema").path("fields"))defs.add(name(f.path("name").asText())+" "+sqlType(f.path("type").asText()));
        s.execute("CREATE TABLE source ("+String.join(",",defs)+")");s.execute("SET enable_external_access=false; SET lock_configuration=true");
        var bound=IcebergQueries.bind(sql,samples);try(var p=c.prepareStatement(bound.sql())) {
          for(int i=0;i<bound.values().size();i++)p.setObject(i+1,bound.values().get(i));
          try(var result=p.executeQuery()) {var meta=result.getMetaData();Set<String> labels=new HashSet<>();
            for(int i=1;i<=meta.getColumnCount();i++){String label=name(meta.getColumnLabel(i));if(!labels.add(label))throw new IllegalArgumentException("Duplicate result label");
              String type=switch(meta.getColumnType(i)){case java.sql.Types.INTEGER,java.sql.Types.SMALLINT,java.sql.Types.TINYINT->"Int";case java.sql.Types.BIGINT->"Long";
                case java.sql.Types.FLOAT,java.sql.Types.REAL,java.sql.Types.DOUBLE->"Float";case java.sql.Types.BOOLEAN,java.sql.Types.BIT->"Boolean";
                case java.sql.Types.VARCHAR,java.sql.Types.TIMESTAMP,java.sql.Types.TIMESTAMP_WITH_TIMEZONE,java.sql.Types.DATE->"String";default->throw new IllegalArgumentException("Cast result type explicitly: "+label);};
              types.append(label).append(": ").append(type).append('\n');}
          }
        }
      }
      types.append("}\n");queries.putObject(field).put("signature",field+(parameters.length()>0?"("+parameters+")":"")+": ["+outputType+"!]!").put("table",namespace+"."+q.path("view").asText()).put("sql",sql);
    }
    for(var iter=config.path("api").path("subscriptions").fields();iter.hasNext();) {
      var entry=iter.next();String field=name(entry.getKey()),viewName=entry.getValue().path("view").asText();var view=views.get(viewName);if(view==null)throw new IllegalArgumentException("Unknown subscription view");
      String type="Change_"+field;types.append("type ").append(type).append(" {\n");
      for(var f:view.path("schema").path("fields"))types.append(name(f.path("name").asText())).append(": ").append(graphType(f.path("type").asText())).append('\n');types.append("}\n");
      var sub=subscriptions.putObject(field);sub.put("signature",field+"(entityId: String): "+type+"!").put("entityField","entity_id");sub.putArray("topics").add(config.path("topics").path(viewName).asText());
    }
    for(var iter=config.path("api").path("mutations").fields();iter.hasNext();) {
      var entry=iter.next();String field=name(entry.getKey()),input=entry.getValue().path("input").asText();var schema=config.path("inputs").get(input);if(schema==null)throw new IllegalArgumentException("Unknown mutation input");
      String inputType=inputType(schema,"Input_"+field,types);var mutation=mutations.putObject(field);
      mutation.put("signature",field+"(input: "+inputType+"!): CommandReceipt!").put("path","/ingest/"+input).set("schema",schema);
    }
    out.put("types",types.toString());new ObjectMapper(new YAMLFactory()).writeValue(Path.of(args[1]).toFile(),out);
  }
  static String inputType(JsonNode schema,String type,StringBuilder types) {
    var fields=new StringBuilder();Set<String> required=new HashSet<>();schema.path("required").forEach(v->required.add(v.asText()));
    for(var it=schema.path("properties").fields();it.hasNext();) {var f=it.next();if(f.getValue().isBoolean())continue;String field=name(f.getKey());JsonNode spec=f.getValue();String t=switch(spec.path("type").asText()) {
      case "string"->"String";case "integer"->"Long";case "number"->"Float";case "boolean"->"Boolean";
      case "object"->spec.has("properties")?inputType(spec,type+"_"+field,types):"JSON";default->throw new IllegalArgumentException("Unsupported GraphQL input construct: "+field);};
      fields.append(field).append(": ").append(t).append(required.contains(field)?"!":"").append('\n');
    }
    if(fields.length()==0)throw new IllegalArgumentException("Empty typed input");types.append("input ").append(type).append(" {\n").append(fields).append("}\n");return type;
  }
}
