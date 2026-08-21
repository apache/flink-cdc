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

package org.apache.flink.cdc.connectors.oracle.source.utils;

import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.oracle.Scn;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link OracleConnectionUtils}. */
class OracleConnectionUtilsTest {

    @Test
    void testResolveTimestampScnAsExclusiveLowerBound() {
        long startupTimestampMillis = 1700000000000L;

        RedoLogOffset offset =
                OracleConnectionUtils.resolveRedoLogOffsetByTimestamp(
                        startupTimestampMillis, () -> Scn.valueOf("456"), () -> Scn.valueOf("1"));

        assertThat(offset.getScn()).isEqualTo("455");
    }

    @Test
    void testFallbackToOldestAvailableScnWhenTimestampCannotBeMapped() {
        long startupTimestampMillis = 1700000000000L;

        RedoLogOffset offset =
                OracleConnectionUtils.resolveRedoLogOffsetByTimestamp(
                        startupTimestampMillis,
                        () -> {
                            throw new SQLException(
                                    "ORA-08181: specified number is not a valid system change number");
                        },
                        () -> Scn.valueOf("123456"));

        assertThat(offset.getScn()).isEqualTo("123455");
    }

    @Test
    void testFallbackToOldestAvailableScnWhenTimestampIsInvalid() {
        long startupTimestampMillis = 1700000000000L;

        RedoLogOffset offset =
                OracleConnectionUtils.resolveRedoLogOffsetByTimestamp(
                        startupTimestampMillis,
                        () -> {
                            throw new SQLException(
                                    "ORA-08186: invalid timestamp specified", "72000", 8186);
                        },
                        () -> Scn.valueOf("223344"));

        assertThat(offset.getScn()).isEqualTo("223343");
    }

    @Test
    void testThrowWhenTimestampQueryFailedWithUnexpectedSQLException() {
        long startupTimestampMillis = 1700000000000L;

        assertThatThrownBy(
                        () ->
                                OracleConnectionUtils.resolveRedoLogOffsetByTimestamp(
                                        startupTimestampMillis,
                                        () -> {
                                            throw new SQLException(
                                                    "ORA-01017: invalid username/password");
                                        },
                                        () -> Scn.valueOf("123456")))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Cannot resolve startup timestamp");
    }

    @Test
    void testThrowWhenFallbackOldestScnIsUnavailable() {
        long startupTimestampMillis = 1700000000000L;

        assertThatThrownBy(
                        () ->
                                OracleConnectionUtils.resolveRedoLogOffsetByTimestamp(
                                        startupTimestampMillis, () -> null, () -> Scn.NULL))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Cannot resolve oldest available Oracle SCN");
    }
}
