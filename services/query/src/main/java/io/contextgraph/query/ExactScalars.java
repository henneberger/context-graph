package io.contextgraph.query;

import graphql.language.*;
import graphql.schema.*;
import java.math.*;

/** Exact numeric inputs; string outputs also preserve precision in JavaScript clients. */
final class ExactScalars {
    static final GraphQLScalarType DECIMAL = scalar("Decimal", false);
    static final GraphQLScalarType BIG_INT = scalar("BigInt", true);
    private static Number number(Object value, boolean integer) {
        if (!(value instanceof String || value instanceof BigDecimal || value instanceof BigInteger
                || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long))
            throw new IllegalArgumentException("Use an exact numeric value or decimal string");
        var decimal = new BigDecimal(value.toString());
        return integer ? decimal.toBigIntegerExact() : decimal;
    }
    private static GraphQLScalarType scalar(String name, boolean integer) {
        return GraphQLScalarType.newScalar().name(name).description("Exact numeric value. Send strings from clients with limited numeric precision; results are strings.")
            .coercing(new Coercing<Number,String>() {
                public Number parseValue(Object input) {
                    try { return number(input, integer); }
                    catch (RuntimeException e) { throw new CoercingParseValueException("Invalid " + name + "; use an exact numeric string"); }
                }
                public Number parseLiteral(Object input) {
                    Object value = input instanceof IntValue v ? v.getValue() : input instanceof FloatValue v ? v.getValue()
                        : input instanceof StringValue v ? v.getValue() : null;
                    try { return number(value, integer); }
                    catch (RuntimeException e) { throw new CoercingParseLiteralException("Invalid " + name); }
                }
                public String serialize(Object input) {
                    try { var value=number(input, integer); return value instanceof BigDecimal d ? d.toPlainString() : value.toString(); }
                    catch (RuntimeException e) { throw new CoercingSerializeException("Invalid " + name); }
                }
            }).build();
    }
}
