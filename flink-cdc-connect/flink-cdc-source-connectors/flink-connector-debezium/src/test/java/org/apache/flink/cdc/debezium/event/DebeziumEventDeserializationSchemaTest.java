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

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for {@link DebeziumEventDeserializationSchema}. */
class DebeziumEventDeserializationSchemaTest {

    private TestDebeziumEventDeserializationSchema deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new TestDebeziumEventDeserializationSchema();
    }

    @Test
    void testConvertToZonedTimestampWithZonedTimestampSchema() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22+00:00";
        Object result = deserializer.convertToZonedTimestamp(timestampStr, schema);

        assertThat(result).isInstanceOf(ZonedTimestampData.class);
        ZonedTimestampData zonedTimestampData = (ZonedTimestampData) result;
        OffsetDateTime expected = OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER);
        assertThat(zonedTimestampData.getZonedDateTime().toOffsetDateTime()).isEqualTo(expected);
    }

    @Test
    void testConvertToZonedTimestampWithDifferentTimezones() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestamp1 = "2020-07-17T18:00:22+05:30";
        String timestamp2 = "2020-07-17T18:00:22-08:00";
        String timestamp3 = "2020-07-17T18:00:22Z";

        ZonedTimestampData result1 =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestamp1, schema);
        ZonedTimestampData result2 =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestamp2, schema);
        ZonedTimestampData result3 =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestamp3, schema);

        assertThat(result1.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestamp1, ZonedTimestamp.FORMATTER));
        assertThat(result2.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestamp2, ZonedTimestamp.FORMATTER));
        assertThat(result3.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestamp3, ZonedTimestamp.FORMATTER));
    }

    @Test
    void testConvertToZonedTimestampWithMillisecondPrecision() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22.123+00:00";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        assertThat(result.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER));
    }

    @Test
    void testConvertToZonedTimestampWithMicrosecondPrecision() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22.123456+00:00";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        assertThat(result.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER));
    }

    @Test
    void testConvertToZonedTimestampWithNanosecondPrecision() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22.123456789+00:00";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        assertThat(result.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER));
    }

    @Test
    void testConvertToZonedTimestampWithStandardIso8601Format() throws Exception {
        Schema schema = SchemaBuilder.string().name("some.other.schema").optional().build();

        String timestampStr = "2020-07-17T18:00:22+00:00";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        assertThat(result.getZonedDateTime().toOffsetDateTime())
                .isEqualTo(OffsetDateTime.parse(timestampStr));
    }

    @Test
    void testConvertToZonedTimestampThrowsExceptionForNonString() {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        assertThatThrownBy(() -> deserializer.convertToZonedTimestamp(12345, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to convert to TIMESTAMP WITH TIME ZONE");
    }

    @Test
    void testConvertToZonedTimestampPreservesTimezoneOffset() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22+05:30";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        OffsetDateTime expected = OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER);
        assertThat(result.getZonedDateTime().toOffsetDateTime()).isEqualTo(expected);
        assertThat(result.getZonedDateTime().toOffsetDateTime().getOffset())
                .isEqualTo(ZoneOffset.of("+05:30"));
    }

    @Test
    void testConvertToZonedTimestampWithUtcTimezone() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String timestampStr = "2020-07-17T18:00:22Z";
        ZonedTimestampData result =
                (ZonedTimestampData) deserializer.convertToZonedTimestamp(timestampStr, schema);

        OffsetDateTime expected = OffsetDateTime.parse(timestampStr, ZonedTimestamp.FORMATTER);
        assertThat(result.getZonedDateTime().toOffsetDateTime()).isEqualTo(expected);
        assertThat(result.getZonedDateTime().toOffsetDateTime().getOffset())
                .isEqualTo(ZoneOffset.UTC);
    }

    @Test
    void testConvertToZonedTimestampRoundTrip() throws Exception {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).optional().build();

        String originalTimestamp = "2020-07-17T18:00:22.123456+05:30";
        ZonedTimestampData result =
                (ZonedTimestampData)
                        deserializer.convertToZonedTimestamp(originalTimestamp, schema);

        OffsetDateTime converted = result.getZonedDateTime().toOffsetDateTime();
        String roundTrip = ZonedTimestamp.FORMATTER.format(converted);

        assertThat(roundTrip).isEqualTo(originalTimestamp);
    }

    private static class TestDebeziumEventDeserializationSchema
            extends DebeziumEventDeserializationSchema {

        public TestDebeziumEventDeserializationSchema() {
            super(
                    new org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference(),
                    DebeziumChangelogMode.ALL);
        }

        public Object convertToZonedTimestamp(Object dbzObj, Schema schema) {
            return super.convertToZonedTimestamp(dbzObj, schema);
        }

        @Override
        protected boolean isDataChangeRecord(org.apache.kafka.connect.source.SourceRecord record) {
            return false;
        }

        @Override
        protected boolean isSchemaChangeRecord(
                org.apache.kafka.connect.source.SourceRecord record) {
            return false;
        }

        @Override
        protected org.apache.flink.cdc.common.event.TableId getTableId(
                org.apache.kafka.connect.source.SourceRecord record) {
            return org.apache.flink.cdc.common.event.TableId.tableId("test", "test");
        }

        @Override
        protected java.util.Map<String, String> getMetadata(
                org.apache.kafka.connect.source.SourceRecord record) {
            return java.util.Collections.emptyMap();
        }

        @Override
        protected java.util.List<org.apache.flink.cdc.common.event.SchemaChangeEvent>
                deserializeSchemaChangeRecord(org.apache.kafka.connect.source.SourceRecord record) {
            return java.util.Collections.emptyList();
        }
    }
}
