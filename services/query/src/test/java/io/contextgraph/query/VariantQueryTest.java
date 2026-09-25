package io.contextgraph.query;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class VariantQueryTest {
  @Test void nativeVariantAndStructCanBeReturnedAsStructuredJson() throws Exception {
    try(var c=IcebergQueries.connect(false)) {
      var result=IcebergQueries.run(c,new IcebergQueries.BoundSql("SELECT {'name':'sample', 'extra':[1,2]} AS details, CAST(json('{\"unknown\":[1,\"two\",null]}') AS VARIANT) AS _json_remainder",List.of()));
      assertInstanceOf(Map.class,result.getFirst().get("details"));
      assertInstanceOf(Map.class,result.getFirst().get("_json_remainder"));
    }
  }
}
