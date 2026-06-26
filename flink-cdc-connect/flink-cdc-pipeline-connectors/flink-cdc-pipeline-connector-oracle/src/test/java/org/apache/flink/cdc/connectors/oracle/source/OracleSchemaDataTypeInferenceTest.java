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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleSchemaDataTypeInference}. */
class OracleSchemaDataTypeInferenceTest {

    private static final long ONE_DAY_IN_MILLIS = 86_400_000L;

    private final OracleSchemaDataTypeInference inference = new OracleSchemaDataTypeInference();

    private static Schema int64Schema(String name) {
        return SchemaBuilder.int64().name(name).optional().build();
    }

    @Test
    void testOracleDateMidnightIsRecognisedAsDate() {
        // Oracle DATE columns arrive from LogMiner as INT64 epoch millis with
        // midnight as the time-of-day portion. The schema name matches the
        // regular Timestamp semantic type, but the value round-trips as a
        // LocalDate.
        Schema schema = int64Schema(Timestamp.SCHEMA_NAME);
        long epochMillisForDate = LocalDate.of(2022, 10, 30).toEpochDay() * ONE_DAY_IN_MILLIS;
        DataType result = inference.infer(epochMillisForDate, schema);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    @Test
    void testOracleDateAtUnixEpochIsRecognisedAsDate() {
        Schema schema = int64Schema(Timestamp.SCHEMA_NAME);
        DataType result = inference.infer(0L, schema);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    @Test
    void testOracleDateBeforeLowerBoundIsNotRecognisedAsDate() {
        Schema schema = int64Schema(Timestamp.SCHEMA_NAME);
        // 1899-12-31T00:00:00Z is below the 1900 lower bound.
        long millis = -2208988800000L - ONE_DAY_IN_MILLIS;
        DataType result = inference.infer(millis, schema);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testOracleTimestampWithNonMidnightTimeIsNotRecognisedAsDate() {
        Schema schema = int64Schema(Timestamp.SCHEMA_NAME);
        // 2022-10-30T00:00:01Z is a valid TIMESTAMP value but not a DATE.
        long millis = LocalDate.of(2022, 10, 30).toEpochDay() * ONE_DAY_IN_MILLIS + 1000L;
        DataType result = inference.infer(millis, schema);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testMicrosecondTimestampIsStillRecognisedAsMicroTimestamp() {
        Schema schema = int64Schema(MicroTimestamp.SCHEMA_NAME);
        // 2022-10-30T12:34:56.00789Z in microseconds. Value is not a midnight
        // boundary so it must still be treated as TIMESTAMP(6).
        long micros =
                LocalDateTime.of(2022, 10, 30, 12, 34, 56, 7_890_000)
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()
                        * 1000L;
        DataType result = inference.infer(micros, schema);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(6));
    }

    @Test
    void testNonLongValueKeepsDefaultInference() {
        Schema schema = int64Schema(Timestamp.SCHEMA_NAME);
        // value is not a Long so the DATE heuristic must not fire.
        DataType result = inference.infer("not a number", schema);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(3));
    }
}
