/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.parser;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.cdc.common.types.variant.Variant;
import org.apache.flink.cdc.common.types.variant.VariantTypeException;

import org.assertj.core.api.Assertions;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.Unparser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Unit tests for the {@link JaninoCompiler}. */
class JaninoCompilerTest {

    @Test
    void testJaninoParser() throws CompileException, IOException, InvocationTargetException {
        String expression = "1==2";
        Parser parser = new Parser(new Scanner(null, new StringReader(expression)));
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.cook(parser);
        Object evaluate = expressionEvaluator.evaluate();
        Assertions.assertThat(evaluate).isExactlyInstanceOf(Boolean.class);
        Assertions.assertThat((Boolean) evaluate).isFalse();
    }

    @Test
    void testJaninoUnParser() {
        String expression = "1 <= 2";
        String[] values = new String[1];
        values[0] = "1";
        String value2 = "2";
        Java.AmbiguousName ambiguousName1 = new Java.AmbiguousName(Location.NOWHERE, values);
        Java.AmbiguousName ambiguousName2 =
                new Java.AmbiguousName(Location.NOWHERE, new String[] {value2});
        Java.BinaryOperation binaryOperation =
                new Java.BinaryOperation(Location.NOWHERE, ambiguousName1, "<=", ambiguousName2);
        StringWriter writer = new StringWriter();
        Unparser unparser = new Unparser(writer);
        unparser.unparseAtom(binaryOperation);
        unparser.close();
        Assertions.assertThat(writer).hasToString(expression);
    }

    @Test
    void testJaninoNumericCompare() throws InvocationTargetException {
        String expression = "col1==3.14";
        List<String> columnNames = Collections.singletonList("col1");
        List<Class<?>> paramTypes = Collections.singletonList(Double.class);
        List<Object> params = Collections.singletonList(3.14);
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assertions.assertThat(evaluate).isExactlyInstanceOf(Boolean.class);
        Assertions.assertThat((Boolean) evaluate).isTrue();
    }

    @Test
    void testJaninoCharCompare() throws InvocationTargetException {
        String expression = "String.valueOf('2').equals(col1)";
        List<String> columnNames = Collections.singletonList("col1");
        List<Class<?>> paramTypes = Collections.singletonList(String.class);
        List<Object> params = Collections.singletonList("2");
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assertions.assertThat(evaluate).isExactlyInstanceOf(Boolean.class);
        Assertions.assertThat((Boolean) evaluate).isTrue();
    }

    @Test
    void testJaninoStringCompare() throws InvocationTargetException {
        String expression = "String.valueOf(\"metadata_table\").equals(__table_name__)";
        List<String> columnNames = Collections.singletonList("__table_name__");
        List<Class<?>> paramTypes = Collections.singletonList(String.class);
        List<Object> params = Collections.singletonList("metadata_table");
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assertions.assertThat(evaluate).isExactlyInstanceOf(Boolean.class);
        Assertions.assertThat((Boolean) evaluate).isTrue();
    }

    @Test
    void testBuiltInFunction() throws InvocationTargetException, IOException {
        String expression = "ceil(2.4)";
        List<String> columnNames = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction(expression),
                        columnNames,
                        paramTypes,
                        Double.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assertions.assertThat(evaluate).isEqualTo(3.0);

        // parseJson function.
        String jsonStr =
                "{\"name\":\"Bob\",\"age\":30,\"is_active\":true,\"email\":\"zhangsan@example.com\",\"hobbies\":[\"reading\",\"coding\",\"traveling\"],\"address\":{\"street\":\"MainSt\",\"city\":\"Beijing\",\"zip\":\"100000\"}}";
        expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction("parseJson(testJsonStr)"),
                        List.of("testJsonStr"),
                        List.of(String.class),
                        Variant.class);
        evaluate = expressionEvaluator.evaluate(new Object[] {jsonStr});
        Assertions.assertThat(evaluate)
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(jsonStr, false));
        final String duplicatedNameJsonStr =
                "{\"name\":\"Bob\",\"name\":\"Mark\",\"age\":30,\"is_active\":true,\"email\":\"zhangsan@example.com\",\"hobbies\":[\"reading\",\"coding\",\"traveling\"],\"address\":{\"street\":\"MainSt\",\"city\":\"Beijing\",\"zip\":\"100000\"}}";
        expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction("parseJson(testJsonStr, true)"),
                        List.of("testJsonStr"),
                        List.of(String.class),
                        Variant.class);
        evaluate = expressionEvaluator.evaluate(new Object[] {duplicatedNameJsonStr});
        Assertions.assertThat(evaluate)
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(duplicatedNameJsonStr, true));
        Assertions.assertThatThrownBy(
                        () -> {
                            JaninoCompiler.compileExpression(
                                            JaninoCompiler.loadSystemFunction(
                                                    "parseJson(testJsonStr)"),
                                            List.of("testJsonStr"),
                                            List.of(String.class),
                                            Variant.class)
                                    .evaluate(new Object[] {duplicatedNameJsonStr});
                        })
                .rootCause()
                .isExactlyInstanceOf(VariantTypeException.class)
                .hasMessageContaining("VARIANT_DUPLICATE_KEY");
        evaluate = expressionEvaluator.evaluate(new Object[] {null});
        Assertions.assertThat(evaluate).isEqualTo(null);
        evaluate = expressionEvaluator.evaluate(new Object[] {""});
        Assertions.assertThat(evaluate).isEqualTo(null);
        final String invalidJsonStr = "invalidJson";
        Assertions.assertThatThrownBy(
                        () -> {
                            JaninoCompiler.compileExpression(
                                            JaninoCompiler.loadSystemFunction(
                                                    "parseJson(testJsonStr)"),
                                            List.of("testJsonStr"),
                                            List.of(String.class),
                                            Variant.class)
                                    .evaluate(new Object[] {invalidJsonStr});
                        })
                .cause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse json string");

        // tryParseJson function.
        expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction("tryParseJson(testJsonStr)"),
                        List.of("testJsonStr"),
                        List.of(String.class),
                        Variant.class);
        evaluate = expressionEvaluator.evaluate(new Object[] {null});
        Assertions.assertThat(evaluate).isEqualTo(null);
        evaluate = expressionEvaluator.evaluate(new Object[] {""});
        Assertions.assertThat(evaluate).isEqualTo(null);
        evaluate = expressionEvaluator.evaluate(new Object[] {jsonStr});
        Assertions.assertThat(evaluate)
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(jsonStr, false));
        evaluate = expressionEvaluator.evaluate(new Object[] {duplicatedNameJsonStr});
        Assertions.assertThat(evaluate).isEqualTo(null);
        evaluate = expressionEvaluator.evaluate(new Object[] {invalidJsonStr});
        Assertions.assertThat(evaluate).isEqualTo(null);
        expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction("tryParseJson(testJsonStr, true)"),
                        List.of("testJsonStr"),
                        List.of(String.class),
                        Variant.class);
        evaluate = expressionEvaluator.evaluate(new Object[] {duplicatedNameJsonStr});
        Assertions.assertThat(evaluate)
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(duplicatedNameJsonStr, true));
    }

    @Test
    void testTranslatedLogicalExpressionCompilesWithJanino() throws InvocationTargetException {
        List<Column> columns =
                List.of(
                        Column.physicalColumn("id", DataTypes.INT()),
                        Column.physicalColumn("uid", DataTypes.INT()));
        Map<String, String> columnNameMap = Map.of("id", "$0", "uid", "$1");

        ExpressionEvaluator andEvaluator =
                compileTranslatedFilterExpression("id = 1 and 1 / uid > 0", columns, columnNameMap);
        Assertions.assertThat(andEvaluator.evaluate(new Object[] {2, 0})).isEqualTo(false);

        ExpressionEvaluator orEvaluator =
                compileTranslatedFilterExpression("id = 1 or 1 / uid > 0", columns, columnNameMap);
        Assertions.assertThat(orEvaluator.evaluate(new Object[] {1, 0})).isEqualTo(true);

        List<Column> booleanColumns =
                List.of(
                        Column.physicalColumn("left_bool", DataTypes.BOOLEAN().notNull()),
                        Column.physicalColumn("right_bool", DataTypes.BOOLEAN().notNull()),
                        Column.physicalColumn("divisor", DataTypes.INT()));
        Map<String, String> booleanColumnNameMap =
                Map.of("left_bool", "$0", "right_bool", "$1", "divisor", "$2");
        List<String> booleanColumnNames = List.of("$0", "$1", "$2");
        List<Class<?>> booleanColumnTypes = List.of(Boolean.class, Boolean.class, Integer.class);

        ExpressionEvaluator nativeAndEvaluator =
                compileTranslatedFilterExpression(
                        "left_bool and right_bool",
                        booleanColumns,
                        booleanColumnNameMap,
                        booleanColumnNames,
                        booleanColumnTypes);
        Assertions.assertThat(nativeAndEvaluator.evaluate(new Object[] {true, true, 0}))
                .isEqualTo(true);

        ExpressionEvaluator conditionalAndEvaluator =
                compileTranslatedFilterExpression(
                        "left_bool and 1 / divisor > 0",
                        booleanColumns,
                        booleanColumnNameMap,
                        booleanColumnNames,
                        booleanColumnTypes);
        Assertions.assertThat(conditionalAndEvaluator.evaluate(new Object[] {false, true, 0}))
                .isEqualTo(false);

        ExpressionEvaluator conditionalOrEvaluator =
                compileTranslatedFilterExpression(
                        "left_bool or 1 / divisor > 0",
                        booleanColumns,
                        booleanColumnNameMap,
                        booleanColumnNames,
                        booleanColumnTypes);
        Assertions.assertThat(conditionalOrEvaluator.evaluate(new Object[] {true, false, 0}))
                .isEqualTo(true);

        List<Column> nullableBooleanColumns =
                List.of(Column.physicalColumn("nullable_bool", DataTypes.BOOLEAN()));
        Map<String, String> nullableBooleanColumnNameMap = Map.of("nullable_bool", "$0");
        ExpressionEvaluator nullableOrEvaluator =
                compileTranslatedFilterExpression(
                        "nullable_bool or false",
                        nullableBooleanColumns,
                        nullableBooleanColumnNameMap,
                        List.of("$0"),
                        List.of(Boolean.class));
        Assertions.assertThat(nullableOrEvaluator.evaluate(new Object[] {null})).isNull();
    }

    @Test
    void testLargeNumericLiterals() {
        // Test parsing integer literals
        Stream.of(
                        Tuple2.of("0", 0),
                        Tuple2.of("1", 1),
                        Tuple2.of("1", 1),
                        Tuple2.of("2147483647", 2147483647),
                        Tuple2.of("-2147483648", -2147483648))
                .forEach(
                        t -> {
                            String expression = t.f0;
                            List<String> columnNames = new ArrayList<>();
                            List<Class<?>> paramTypes = new ArrayList<>();
                            ExpressionEvaluator expressionEvaluator =
                                    JaninoCompiler.compileExpression(
                                            JaninoCompiler.loadSystemFunction(expression),
                                            columnNames,
                                            paramTypes,
                                            Integer.class);
                            try {
                                Assertions.assertThat(expressionEvaluator.evaluate())
                                        .isEqualTo(t.f1);
                            } catch (InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        });

        // Test parsing double literals
        Stream.of(
                        Tuple2.of("3.1415926", 3.1415926),
                        Tuple2.of("0.0", 0.0),
                        Tuple2.of("17.0", 17.0),
                        Tuple2.of("123456789.123456789", 123456789.123456789),
                        Tuple2.of("-987654321.987654321", -987654321.987654321))
                .forEach(
                        t -> {
                            String expression = t.f0;
                            List<String> columnNames = new ArrayList<>();
                            List<Class<?>> paramTypes = new ArrayList<>();
                            ExpressionEvaluator expressionEvaluator =
                                    JaninoCompiler.compileExpression(
                                            JaninoCompiler.loadSystemFunction(expression),
                                            columnNames,
                                            paramTypes,
                                            Double.class);
                            try {
                                Assertions.assertThat(expressionEvaluator.evaluate())
                                        .isEqualTo(t.f1);
                            } catch (InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        });

        // Test parsing long literals
        Stream.of(
                        Tuple2.of("2147483648L", 2147483648L),
                        Tuple2.of("-2147483649L", -2147483649L),
                        Tuple2.of("9223372036854775807L", 9223372036854775807L),
                        Tuple2.of("-9223372036854775808L", -9223372036854775808L))
                .forEach(
                        t -> {
                            String expression = t.f0;
                            List<String> columnNames = new ArrayList<>();
                            List<Class<?>> paramTypes = new ArrayList<>();
                            ExpressionEvaluator expressionEvaluator =
                                    JaninoCompiler.compileExpression(
                                            JaninoCompiler.loadSystemFunction(expression),
                                            columnNames,
                                            paramTypes,
                                            Long.class);
                            try {
                                Assertions.assertThat(expressionEvaluator.evaluate())
                                        .isEqualTo(t.f1);
                            } catch (InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private static ExpressionEvaluator compileTranslatedFilterExpression(
            String expression, List<Column> columns, Map<String, String> columnNameMap) {
        return compileTranslatedFilterExpression(
                expression,
                columns,
                columnNameMap,
                List.of("$0", "$1"),
                List.of(Integer.class, Integer.class));
    }

    private static ExpressionEvaluator compileTranslatedFilterExpression(
            String expression,
            List<Column> columns,
            Map<String, String> columnNameMap,
            List<String> columnNames,
            List<Class<?>> columnTypes) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        expression,
                        columns,
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0],
                        columnNameMap);
        return JaninoCompiler.compileExpression(
                JaninoCompiler.loadSystemFunction(janinoExpression),
                columnNames,
                columnTypes,
                Boolean.class);
    }
}
