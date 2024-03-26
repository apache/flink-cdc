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
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * The processor of the transform expression. It processes the expression of projections and
 * filters.
 */
public class TransformExpressionCompiler {

    static final Cache<TransformExpressionKey, ExpressionEvaluator> COMPILED_EXPRESSION_CACHE =
            CacheBuilder.newBuilder().softValues().build();

    /** Triggers internal garbage collection of expired cache entries. */
    public static void cleanUp() {
        COMPILED_EXPRESSION_CACHE.cleanUp();
    }

    /** Compiles an expression code to a janino {@link ExpressionEvaluator}. */
    public static ExpressionEvaluator compileExpression(TransformExpressionKey key) {
        try {
            return COMPILED_EXPRESSION_CACHE.get(
                    key,
                    () -> {
                        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
                        // Input args
                        expressionEvaluator.setParameters(
                                key.getArgumentNames().toArray(new String[0]),
                                key.getArgumentClasses().toArray(new Class[0]));
                        // Result type
                        expressionEvaluator.setExpressionType(key.getReturnClass());
                        try {
                            // Compile
                            expressionEvaluator.cook(key.getExpression());
                        } catch (CompileException e) {
                            throw new InvalidProgramException(
                                    "Expression cannot be compiled. This is a bug. Please file an issue.\nExpression: "
                                            + key.getExpression(),
                                    e);
                        }
                        return expressionEvaluator;
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }
}
