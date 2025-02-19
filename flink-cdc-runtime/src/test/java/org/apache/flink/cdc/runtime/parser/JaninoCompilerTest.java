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
    void testBuildInFunction() throws InvocationTargetException {
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
}
