package io.contextgraph.query;
import graphql.*;
import graphql.schema.idl.*;
import java.math.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class ExactScalarsTest {
 @Test void variablesLiteralsAndResponsesPreserveExactValues() {
  var schema = new SchemaParser().parse("scalar Decimal\nscalar BigInt\ntype Query { echo(x: Decimal!, n: BigInt!): Decimal! }");
  var wiring=RuntimeWiring.newRuntimeWiring().scalar(ExactScalars.DECIMAL).scalar(ExactScalars.BIG_INT)
    .type("Query",t->t.dataFetcher("echo",e->{assertEquals(new BigInteger("9223372036854775808"),e.getArgument("n"));return e.getArgument("x");})).build();
  var graph=GraphQL.newGraphQL(new SchemaGenerator().makeExecutableSchema(schema,wiring)).build();
  for(Object value:List.of("9007199254740993.123456789",new BigDecimal("9007199254740993.123456789"))) {
   var result=graph.execute(ExecutionInput.newExecutionInput().query("query($x:Decimal!,$n:BigInt!){echo(x:$x,n:$n)}").variables(Map.of("x",value,"n","9223372036854775808")).build());
   assertTrue(result.getErrors().isEmpty(),result.getErrors().toString());assertEquals("9007199254740993.123456789",((Map<?,?>)result.getData()).get("echo"));
  }
  var result=graph.execute("{echo(x:9007199254740993.123456789,n:9223372036854775808)}");
  assertTrue(result.getErrors().isEmpty());assertEquals("9007199254740993.123456789",((Map<?,?>)result.getData()).get("echo"));
 }
 @Test void typedInputsUseExactScalars() throws Exception {
  var schema=IcebergQueries.JSON.readTree("{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"value\":{\"type\":\"number\"},\"count\":{\"type\":\"integer\"}}}");
  var sdl=new StringBuilder();ApiCompiler.inputType(schema,"Input",sdl);
  assertTrue(sdl.toString().contains("value: Decimal"));assertTrue(sdl.toString().contains("count: BigInt"));
  assertThrows(Exception.class,()->ExactScalars.DECIMAL.getCoercing().parseValue(9007199254740992d));
  assertThrows(Exception.class,()->ExactScalars.BIG_INT.getCoercing().parseValue("1.5"));
 }
}
