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

package org.apache.flink.cdc.python;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.python.utils.PemjaTestSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PythonUdf}. */
class PythonUdfTest {

    @BeforeAll
    static void requirePemja() {
        PemjaTestSupport.requirePemja();
    }

    @Test
    void evalBeforeOpenThrows() {
        PythonUdf udf = new PythonUdf();
        assertThatThrownBy(() -> udf.eval(1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before open()");
    }

    @Test
    void closeIsIdempotent() {
        PythonUdf udf = new PythonUdf();
        udf.close();
        udf.close();
        try {
            udf.open(contextFor("def eval(x: int) -> int:\n    return x\n"));
            udf.close();
            udf.close();
        } finally {
            udf.close();
        }
    }

    @Test
    void openRequiresSourceOption() {
        PythonUdf udf = new PythonUdf();
        Map<String, String> opts = new HashMap<>();
        opts.put(PythonUdf.OPTION_PYTHON_EXECUTABLE.key(), PemjaTestSupport.PYTHON_EXEC);
        assertThatThrownBy(() -> udf.open(() -> Configuration.fromMap(opts)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PythonUdf.OPTION_SOURCE.key());
    }

    @Test
    void getReturnTypeReadsFromSource() {
        PythonUdf udf = new PythonUdf();
        assertThat(udf.getReturnType(contextFor("def eval(x: int) -> int:\n    return x * 2\n")))
                .isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void evalRoundTripsInt() {
        PythonUdf udf = new PythonUdf();
        udf.open(contextFor("def eval(x: int) -> int:\n    return x * 2\n"));
        try {
            assertThat(udf.eval(21L)).isEqualTo(42L);
        } finally {
            udf.close();
        }
    }

    @Test
    void evalRoundTripsString() {
        PythonUdf udf = new PythonUdf();
        udf.open(contextFor("def eval(s: str) -> str:\n    return s.upper()\n"));
        try {
            assertThat(udf.eval("abc")).isEqualTo("ABC");
        } finally {
            udf.close();
        }
    }

    @Test
    void evalForwardsNullToPython() {
        // Pemja maps Java null -> Python None; a guard-clause UDF should see it and may handle it.
        PythonUdf udf = new PythonUdf();
        udf.open(
                contextFor(
                        "def eval(s) -> str:\n" + "    return 'null' if s is None else str(s)\n"));
        try {
            assertThat(udf.eval(new Object[] {null})).isEqualTo("null");
        } finally {
            udf.close();
        }
    }

    private static UserDefinedFunctionContext contextFor(String source) {
        Map<String, String> opts = new HashMap<>();
        opts.put(PythonUdf.OPTION_SOURCE.key(), source);
        opts.put(PythonUdf.OPTION_PYTHON_EXECUTABLE.key(), PemjaTestSupport.PYTHON_EXEC);
        return () -> Configuration.fromMap(opts);
    }
}
