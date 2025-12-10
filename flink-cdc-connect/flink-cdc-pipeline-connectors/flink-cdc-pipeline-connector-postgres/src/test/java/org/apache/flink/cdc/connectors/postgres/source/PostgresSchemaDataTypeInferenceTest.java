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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/** Test cases for {@link PostgresSchemaDataTypeInference}. */
class PostgresSchemaDataTypeInferenceTest {

    private final PostgresSchemaDataTypeInference inference = new PostgresSchemaDataTypeInference();

    private DataType inferString(Object value, Schema schema) {
        try {
            Method method =
                    PostgresSchemaDataTypeInference.class.getDeclaredMethod(
                            "inferString", Object.class, Schema.class);
            method.setAccessible(true);
            return (DataType) method.invoke(inference, value, schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testInferZonedTimestampWithZeroPrecision() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result = inferString("2020-07-17T18:00:22+00:00", zonedTimestampSchema);

        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_TZ(0));
    }

    @Test
    void testInferZonedTimestampWithMillisecondPrecision() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result = inferString("2020-07-17T18:00:22.123+00:00", zonedTimestampSchema);

        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_TZ(3));
    }

    @Test
    void testInferZonedTimestampWithMicrosecondPrecision() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result = inferString("2020-07-17T18:00:22.123456+00:00", zonedTimestampSchema);

        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_TZ(6));
    }

    @Test
    void testInferZonedTimestampWithNanosecondPrecision() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result = inferString("2020-07-17T18:00:22.123456789+00:00", zonedTimestampSchema);

        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_TZ(9));
    }

    @Test
    void testInferZonedTimestampWithDifferentTimezones() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result1 = inferString("2020-07-17T18:00:22+05:30", zonedTimestampSchema);
        DataType result2 = inferString("2020-07-17T18:00:22-08:00", zonedTimestampSchema);
        DataType result3 = inferString("2020-07-17T18:00:22Z", zonedTimestampSchema);

        assertThat(result1).isEqualTo(DataTypes.TIMESTAMP_TZ(0));
        assertThat(result2).isEqualTo(DataTypes.TIMESTAMP_TZ(0));
        assertThat(result3).isEqualTo(DataTypes.TIMESTAMP_TZ(0));
    }

    @Test
    void testInferZonedTimestampWithNullValue() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        DataType result = inferString(null, zonedTimestampSchema);

        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_TZ(0));
    }

    @Test
    void testInferNonZonedTimestampString() {
        Schema regularStringSchema =
                SchemaBuilder.string().name("some.other.schema").optional().build();

        DataType result = inferString("some string value", regularStringSchema);

        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testInferZonedTimestampWithVariousPrecisions() {
        Schema zonedTimestampSchema =
                SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        assertThat(inferString("2020-07-17T18:00:22+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(0));

        assertThat(inferString("2020-07-17T18:00:22.1+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(3));

        assertThat(inferString("2020-07-17T18:00:22.12+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(3));

        assertThat(inferString("2020-07-17T18:00:22.123+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(3));

        assertThat(inferString("2020-07-17T18:00:22.1234+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(6));

        assertThat(inferString("2020-07-17T18:00:22.123456+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(6));

        assertThat(inferString("2020-07-17T18:00:22.1234567+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(9));

        assertThat(inferString("2020-07-17T18:00:22.123456789+00:00", zonedTimestampSchema))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(9));
    }
}
