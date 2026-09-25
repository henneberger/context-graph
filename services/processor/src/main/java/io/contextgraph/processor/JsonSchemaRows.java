package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.variant.Variant;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/** JSON Schema defines fields. Unmapped keys are native VARIANT, never inferred entities. */
public final class JsonSchemaRows implements Serializable {
  public static final String REMAINDER = "_json_remainder";
  private final String definition;
  private transient JsonNode schema;
  private transient Json.Validator validator;
  public JsonSchemaRows(String definition) {
    this.definition=definition;
    dataType();
  }
  private JsonNode root(){if(schema==null)schema=Json.read(definition);return schema;}
  private JsonNode resolve(JsonNode s, Set<String> refs) {
    if(!s.has("$ref"))return s;
    String ref=s.path("$ref").asText();
    if(!ref.startsWith("#/")||!refs.add(ref))throw new IllegalArgumentException("Only acyclic local schema references can become structured columns: "+ref);
    JsonNode target=root().at(ref.substring(1));if(target.isMissingNode())throw new IllegalArgumentException("Unresolved schema reference: "+ref);
    // Siblings still participate in full JSON Schema validation. Keep complex intersections VARIANT.
    if(s.size()>1)return s;
    return resolve(target,refs);
  }
  private static String kind(JsonNode s) {
    if(s.has("oneOf")||s.has("anyOf")||s.has("allOf")||s.has("$ref"))return "variant";
    if(!s.has("type")) {
      if(s.has("const")) {var c=s.get("const");if(c.isTextual())return "string";if(c.isBoolean())return "boolean";}
      if(s.path("enum").isArray()&&s.path("enum").size()>0) {boolean strings=true;for(var v:s.path("enum"))strings&=v.isTextual();if(strings)return "string";}
    }
    JsonNode t=s.path("type");
    if(t.isTextual())return t.asText();
    if(t.isArray()) {
      var types=new ArrayList<String>();t.forEach(v->{if(!v.asText().equals("null"))types.add(v.asText());});
      if(types.size()==1)return types.getFirst();
    }
    return "variant";
  }
  public DataType dataType(){return root().path("type").asText().equals("object")?struct(root(),new HashSet<>()):DataTypes.ROW(DataTypes.FIELD(REMAINDER,DataTypes.VARIANT().notNull()));}
  private DataType struct(JsonNode s,Set<String> refs) {
    List<DataTypes.Field> fields=new ArrayList<>();
    s.path("properties").fields().forEachRemaining(e->{
      if(e.getKey().equals(REMAINDER))throw new IllegalArgumentException(REMAINDER+" is reserved for unmapped JSON keys");
      fields.add(DataTypes.FIELD(e.getKey(),type(e.getValue(),new HashSet<>(refs))));
    });
    fields.add(DataTypes.FIELD(REMAINDER,DataTypes.VARIANT().notNull()));
    return DataTypes.ROW(fields.toArray(DataTypes.Field[]::new));
  }
  private DataType type(JsonNode original,Set<String> refs) {
    JsonNode s=resolve(original,refs);
    return switch(kind(s)) {
      case "string" -> DataTypes.STRING();
      // Unbounded JSON integers/numbers are VARIANT. Explicit bounded numeric schemas get SQL types.
      case "integer" -> boundedLong(s)?DataTypes.BIGINT():DataTypes.VARIANT();
      case "number" -> decimalType(s);
      case "boolean" -> DataTypes.BOOLEAN();
      case "object" -> s.has("properties")?struct(s,refs):DataTypes.VARIANT();
      case "array" -> s.has("prefixItems")?DataTypes.VARIANT():DataTypes.ARRAY(type(s.path("items"),refs));
      default -> DataTypes.VARIANT();
    };
  }
  private static boolean boundedLong(JsonNode s) {
    return s.has("minimum")&&s.has("maximum")&&s.get("minimum").isIntegralNumber()&&s.get("maximum").isIntegralNumber()
        &&s.get("minimum").canConvertToLong()&&s.get("maximum").canConvertToLong();
  }
  private static DataType decimalType(JsonNode s) {
    if(!s.has("multipleOf")||!s.has("minimum")||!s.has("maximum"))return DataTypes.VARIANT();
    BigDecimal step=s.get("multipleOf").decimalValue().stripTrailingZeros();int scale=Math.max(0,step.scale());
    BigDecimal extent=s.get("minimum").decimalValue().abs().max(s.get("maximum").decimalValue().abs());
    int integers=Math.max(1,extent.precision()-extent.scale());int precision=integers+scale;
    return precision<=38?DataTypes.DECIMAL(precision,scale):DataTypes.VARIANT();
  }
  public Row read(String json) {
    if(validator==null)validator=new Json.Validator(definition);
    var value=validator.validate(json);return root().path("type").asText().equals("object")?object(value,root(),new HashSet<>()):Row.of(variant(value));
  }
  private Row object(JsonNode value,JsonNode s,Set<String> refs) {
    Row row=new Row(s.path("properties").size()+1);ObjectNode extra=Json.object();
    value.fields().forEachRemaining(e->{if(!s.path("properties").has(e.getKey()))extra.set(e.getKey(),e.getValue());});
    int i=0;for(var it=s.path("properties").fields();it.hasNext();) {var f=it.next();row.setField(i++,readValue(value.get(f.getKey()),f.getValue(),new HashSet<>(refs)));}
    row.setField(i,variant(extra));return row;
  }
  private Object readValue(JsonNode value,JsonNode original,Set<String> refs) {
    if(value==null)return null;
    JsonNode s=resolve(original,refs);DataType type=type(original,new HashSet<>());
    if(type.getLogicalType().getTypeRoot()==org.apache.flink.table.types.logical.LogicalTypeRoot.VARIANT)return variant(value);
    if(value.isNull())return null;
    return switch(kind(s)) {
      case "string" -> value.textValue();case "boolean" -> value.booleanValue();
      case "integer" -> value.longValue();case "number" -> value.decimalValue();
      case "object" -> object(value,s,refs);
      case "array" -> {Object[] array=new Object[value.size()];for(int i=0;i<array.length;i++)array[i]=readValue(value.get(i),s.path("items"),new HashSet<>(refs));yield array;}
      default -> variant(value);
    };
  }
  public static Variant variant(JsonNode value) {
    var b=Variant.newBuilder();
    if(value==null||value.isNull())return b.ofNull();
    if(value.isObject()){var object=b.object();value.fields().forEachRemaining(e->object.add(e.getKey(),variant(e.getValue())));return object.build();}
    if(value.isArray()){var array=b.array();value.forEach(v->array.add(variant(v)));return array.build();}
    if(value.isTextual())return b.of(value.textValue());
    if(value.isBoolean())return b.of(value.booleanValue());
    if(value.isIntegralNumber()&&value.canConvertToLong())return b.of(value.longValue());
    if(value.isNumber()) {
      BigDecimal decimal=value.decimalValue();if(decimal.scale()<0)decimal=decimal.setScale(0);
      if(decimal.precision()>38||decimal.scale()>38)throw new IllegalArgumentException("JSON number exceeds native VARIANT decimal precision/scale; refusing lossy conversion");
      return b.of(decimal);
    }
    throw new IllegalArgumentException("Unsupported JSON value");
  }
}
