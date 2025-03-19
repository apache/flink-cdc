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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import java.util.List;

/**
 * The processor of the transform expression. It processes the expression of projections and
 * filters.
 */
public class TransformExpressionCompiler {

    static final Cache<TransformExpressionKey, ExpressionEvaluator> COMPILED_EXPRESSION_CACHE =
            CacheBuilder.newBuilder().softValues().build();

    /** Triggers internal garbage collection of expired cache entries. */
    public static void cleanUp() {
        // com.google.common.cache.Cache from Guava isn't guaranteed to clear all cached records
        // when invoking Cache#cleanUp, which may cause classloader leakage. Use #invalidateAll
        // instead to ensure all key / value pairs to be correctly discarded.
        COMPILED_EXPRESSION_CACHE.invalidateAll();
    }

    /** Compiles an expression code to a janino {@link ExpressionEvaluator}. */
    public static ExpressionEvaluator compileExpression(
            TransformExpressionKey key, List<UserDefinedFunctionDescriptor> udfDescriptors) {
        try {
            return COMPILED_EXPRESSION_CACHE.get(
                    key,
                    () -> {
                        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();

                        List<String> argumentNames = key.getArgumentNames();
                        List<Class<?>> argumentClasses = key.getArgumentClasses();

                        for (UserDefinedFunctionDescriptor udfFunction : udfDescriptors) {
                            argumentNames.add("__instanceOf" + udfFunction.getClassName());
                            argumentClasses.add(Class.forName(udfFunction.getClasspath()));
                        }

                        // Input args
                        expressionEvaluator.setParameters(
                                argumentNames.toArray(new String[0]),
                                argumentClasses.toArray(new Class[0]));

                        // Result type
                        expressionEvaluator.setExpressionType(key.getReturnClass());
                        try {
                            // Compile
                            expressionEvaluator.cook(key.getExpression());
                        } catch (CompileException e) {
                            throw new InvalidProgramException(
                                    String.format(
                                            "Expression cannot be compiled. This is a bug. Please file an issue.\n\tExpression: %s\n\tColumn name map: {%s}",
                                            key.getExpression(),
                                            TransformException.prettyPrintColumnNameMap(
                                                    key.getColumnNameMap())),
                                    e);
                        }
                        return expressionEvaluator;
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to compile expression " + key, e);
        }
    }
}
