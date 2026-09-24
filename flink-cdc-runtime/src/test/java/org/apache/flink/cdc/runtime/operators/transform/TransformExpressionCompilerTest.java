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

import org.codehaus.janino.ExpressionEvaluator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransformExpressionCompiler}. */
class TransformExpressionCompilerTest {

    private static final int CONCURRENT_WORKERS = 32;
    private static final int CONCURRENT_EVALUATION_ROUNDS = 512;

    @AfterEach
    void cleanUp() {
        TransformExpressionCompiler.cleanUp();
    }

    @Test
    void testCachedEvaluatorSupportsConcurrentFirstEvaluation() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_WORKERS);

        try {
            for (int round = 0; round < CONCURRENT_EVALUATION_ROUNDS; round++) {
                String compiledExpression = "extras.equals(\"test data\") && " + round + " >= 0";
                TransformExpressionKey expressionKey =
                        TransformExpressionKey.of(
                                "extras = 'test data' AND round = " + round,
                                compiledExpression,
                                Collections.singletonList("extras"),
                                Collections.singletonList(String.class),
                                Boolean.class,
                                Collections.singletonMap("extras", "extras"));
                CountDownLatch evaluatorsReady = new CountDownLatch(CONCURRENT_WORKERS);
                CountDownLatch startEvaluation = new CountDownLatch(1);
                List<Future<Object>> results = new ArrayList<>();
                for (int i = 0; i < CONCURRENT_WORKERS; i++) {
                    results.add(
                            executor.submit(
                                    () -> {
                                        ExpressionEvaluator evaluator;
                                        try {
                                            evaluator =
                                                    TransformExpressionCompiler.compileExpression(
                                                            expressionKey, Collections.emptyList());
                                        } finally {
                                            evaluatorsReady.countDown();
                                        }
                                        startEvaluation.await();
                                        return evaluator.evaluate(
                                                new Object[] {"test data", Collections.emptyMap()});
                                    }));
                }

                assertThat(evaluatorsReady.await(30, TimeUnit.SECONDS)).isTrue();
                startEvaluation.countDown();
                for (Future<Object> result : results) {
                    assertThat(result.get(30, TimeUnit.SECONDS)).isEqualTo(true);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCompiledEvaluatorInitializesJaninoMethodCacheBeforePublication() throws Exception {
        TransformExpressionKey expressionKey =
                TransformExpressionKey.of(
                        "extras = 'test data'",
                        "extras.equals(\"test data\")",
                        Collections.singletonList("extras"),
                        Collections.singletonList(String.class),
                        Boolean.class,
                        Collections.singletonMap("extras", "extras"));

        ExpressionEvaluator evaluator =
                TransformExpressionCompiler.compileExpression(
                        expressionKey, Collections.emptyList());

        // Inspect Janino 3.1.10's private cache without calling getMethod(), which would lazily
        // initialize the cache and hide the unsafe-publication regression this test guards against.
        assertThat(getJaninoMethodCache(evaluator))
                .as(
                        "The Janino Method cache must be complete before the evaluator is "
                                + "published to concurrent cache readers")
                .hasSize(1)
                .doesNotContainNull();
    }

    private static Method[] getJaninoMethodCache(ExpressionEvaluator evaluator) throws Exception {
        Field scriptEvaluatorField = ExpressionEvaluator.class.getDeclaredField("se");
        scriptEvaluatorField.setAccessible(true);
        Object scriptEvaluator = scriptEvaluatorField.get(evaluator);

        Field methodCacheField = scriptEvaluator.getClass().getDeclaredField("getMethodsCache");
        methodCacheField.setAccessible(true);
        return (Method[]) methodCacheField.get(scriptEvaluator);
    }
}
