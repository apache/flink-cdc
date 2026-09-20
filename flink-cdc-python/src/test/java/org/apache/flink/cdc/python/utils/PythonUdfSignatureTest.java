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

package org.apache.flink.cdc.python.utils;

import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pemja.core.PythonInterpreterConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PythonUdfSignature}. */
class PythonUdfSignatureTest {

    @BeforeAll
    static void requirePemja() {
        PemjaTestSupport.requirePemja();
    }

    @Test
    void usesSubInterpreterForSignatureParsing() {
        assertThat(
                        PythonUdfSignature.createInterpreterConfig(PemjaTestSupport.PYTHON_EXEC)
                                .getExecType())
                .isEqualTo(PythonInterpreterConfig.ExecType.SUB_INTERPRETER);
    }

    @Test
    void resolvesLargeSourcesConcurrently() throws Exception {
        int threadCount = 2;
        int rounds = 8;
        String source =
                "PADDING = '"
                        + "x".repeat(256 * 1024)
                        + "'\n"
                        + "def eval(value: bytes) -> bytes:\n"
                        + "    return value\n";
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int thread = 0; thread < threadCount; thread++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    barrier.await();
                                    for (int round = 0; round < rounds; round++) {
                                        assertThat(
                                                        PythonUdfSignature.parseReturnType(
                                                                source,
                                                                PemjaTestSupport.PYTHON_EXEC))
                                                .isEqualTo(DataTypes.BYTES());
                                    }
                                    return null;
                                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void resolvesEvenWhenOtherFunctionsArePresent() {
        String source =
                "def helper(x: int) -> int:\n"
                        + "    return x\n"
                        + "def eval(x: str) -> str:\n"
                        + "    return helper(len(x)) and x\n";
        assertThat(PythonUdfSignature.parseReturnType(source, PemjaTestSupport.PYTHON_EXEC))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void throwsWhenNoEvalFunction() {
        String source = "def other(x: int) -> int:\n    return x\n";
        assertThatThrownBy(
                        () ->
                                PythonUdfSignature.parseReturnType(
                                        source, PemjaTestSupport.PYTHON_EXEC))
                .hasMessageContaining(
                        "Python UDF source does not define a top-level 'eval' function.");
    }

    @Test
    void throwsWhenEvalHasNoReturnAnnotation() {
        String source = "def eval(x: int):\n    return x\n";
        assertThatThrownBy(
                        () ->
                                PythonUdfSignature.parseReturnType(
                                        source, PemjaTestSupport.PYTHON_EXEC))
                .hasMessageContaining("Function 'eval' has no return type annotation.");
    }

    @Test
    void throwsWhenAnnotationUnsupported() {
        String source = "def eval(x: int) -> bytearray:\n    return bytearray(x)\n";
        assertThatThrownBy(
                        () ->
                                PythonUdfSignature.parseReturnType(
                                        source, PemjaTestSupport.PYTHON_EXEC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Python UDF return type 'bytearray'.")
                .hasMessageContaining("Supported annotations: [bool, bytes, float, int, str]");
    }

    @Test
    void throwsOnInvalidPythonSource() {
        // Not "no eval" — outright syntax error so ast.parse blows up inside the parser.
        String source = "def eval(x ->\n";
        assertThatThrownBy(
                        () ->
                                PythonUdfSignature.parseReturnType(
                                        source, PemjaTestSupport.PYTHON_EXEC))
                .hasMessageContaining("invalid syntax");
    }
}
