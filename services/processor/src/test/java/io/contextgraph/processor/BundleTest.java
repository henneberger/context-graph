package io.contextgraph.processor;
import org.junit.jupiter.api.Test;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import static org.junit.jupiter.api.Assertions.*;
class BundleTest {
  Schema schema=new Schema(Types.NestedField.required(1,"workspace_id",Types.StringType.get()),Types.NestedField.required(2,"resource_id",Types.StringType.get()),Types.NestedField.required(3,"entity_id",Types.StringType.get()),Types.NestedField.optional(4,"count",Types.LongType.get()));
  @Test void outputCannotRelabelWorkspaceOrEntity() {
    String resource=Scope.resource("demo","alpha");
    assertDoesNotThrow(()->GenericIceberg.encode(Row.of("demo",resource,"alpha",1L),schema,"demo"));
    assertThrows(IllegalArgumentException.class,()->GenericIceberg.encode(Row.of("demo",resource,"beta",1L),schema,"demo"));
    assertThrows(IllegalArgumentException.class,()->GenericIceberg.encode(Row.of("demo",resource,"alpha",1L),schema,"other"));
    assertThrows(IllegalArgumentException.class,()->GenericIceberg.encode(Row.of("demo",resource,"alpha","one"),schema,"demo"));
  }
  @Test void changelogCannotMasqueradeAsAppend() {
    Row row=Row.of("demo",Scope.resource("demo","alpha"),"alpha",1L);row.setKind(org.apache.flink.types.RowKind.DELETE);
    assertThrows(IllegalArgumentException.class,()->GenericIceberg.encode(row,schema,"demo"));
  }
}
