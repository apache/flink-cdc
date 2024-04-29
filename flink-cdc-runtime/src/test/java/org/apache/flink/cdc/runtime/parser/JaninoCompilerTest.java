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

import org.apache.flink.cdc.common.data.TimestampData;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.Unparser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/** Unit tests for the {@link JaninoCompiler}. */
public class JaninoCompilerTest {

    @Test
    public void testJaninoParser() throws CompileException, IOException, InvocationTargetException {
        String expression = "1==2";
        Parser parser = new Parser(new Scanner(null, new StringReader(expression)));
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.cook(parser);
        Object evaluate = expressionEvaluator.evaluate();
        Assert.assertEquals(false, evaluate);
    }

    @Test
    public void testJaninoUnParser() {
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
        Assert.assertEquals(expression, writer.toString());
    }

    @Test
    public void testJaninoNumericCompare() throws InvocationTargetException {
        String expression = "col1==3.14";
        List<String> columnNames = Arrays.asList("col1");
        List<Class<?>> paramTypes = Arrays.asList(Double.class);
        List<Object> params = Arrays.asList(3.14);
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(true, evaluate);
    }

    @Test
    public void testJaninoCharCompare() throws InvocationTargetException {
        String expression = "String.valueOf('2').equals(col1)";
        List<String> columnNames = Arrays.asList("col1");
        List<Class<?>> paramTypes = Arrays.asList(String.class);
        List<Object> params = Arrays.asList("2");
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(true, evaluate);
    }

    @Test
    public void testJaninoStringCompare() throws InvocationTargetException {
        String expression = "String.valueOf(\"metadata_table\").equals(__table_name__)";
        List<String> columnNames = Arrays.asList("__table_name__");
        List<Class<?>> paramTypes = Arrays.asList(String.class);
        List<Object> params = Arrays.asList("metadata_table");
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(true, evaluate);
    }

    @Test
    public void testJaninoTimestampFunction() throws InvocationTargetException {
        long epochTime = System.currentTimeMillis();
        long localTime = epochTime + TimeZone.getTimeZone("GMT-8:00").getOffset(epochTime);
        String expression = "currentTimestamp(epochTime, \"GMT-8:00\")";
        List<String> columnNames = Arrays.asList("epochTime");
        List<Class<?>> paramTypes = Arrays.asList(Long.class);
        List<Object> params = Arrays.asList(epochTime);
        ExpressionEvaluator expressionEvaluator =
                JaninoCompiler.compileExpression(
                        JaninoCompiler.loadSystemFunction(expression),
                        columnNames,
                        paramTypes,
                        TimestampData.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(TimestampData.fromMillis(localTime), evaluate);
    }

    @Test
    public void testBuildInFunction() throws InvocationTargetException {
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
        Assert.assertEquals(3.0, evaluate);
    }
}
