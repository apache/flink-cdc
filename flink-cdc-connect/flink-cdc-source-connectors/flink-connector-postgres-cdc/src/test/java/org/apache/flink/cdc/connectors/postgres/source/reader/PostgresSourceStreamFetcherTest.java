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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSourceStreamFetcher#isLogicalMessage(SourceRecord)}. */
class PostgresSourceStreamFetcherTest {

    private static final Schema ENVELOPE_WITH_OP =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    private static final Schema ENVELOPE_WITHOUT_OP =
            SchemaBuilder.struct().field("source", Schema.OPTIONAL_STRING_SCHEMA).build();

    @Test
    void logicalMessageRecordIsDetected() {
        SourceRecord record = recordWithOp("m");
        assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record)).isTrue();
    }

    @Test
    void dataChangeRecordsAreNotLogicalMessages() {
        for (String op : new String[] {"c", "u", "d", "r", "t"}) {
            SourceRecord record = recordWithOp(op);
            assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record))
                    .as("op=%s should not be detected as logical message", op)
                    .isFalse();
        }
    }

    @Test
    void recordWithNullValueIsNotLogicalMessage() {
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "topic",
                        ENVELOPE_WITH_OP,
                        null);
        assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record)).isFalse();
    }

    @Test
    void recordWithoutOperationFieldIsNotLogicalMessage() {
        Struct value = new Struct(ENVELOPE_WITHOUT_OP).put("source", "anything");
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "topic",
                        ENVELOPE_WITHOUT_OP,
                        value);
        assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record)).isFalse();
    }

    @Test
    void recordWithNullOperationValueIsNotLogicalMessage() {
        Struct value = new Struct(ENVELOPE_WITH_OP);
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "topic",
                        ENVELOPE_WITH_OP,
                        value);
        assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record)).isFalse();
    }

    @Test
    void recordWithNonStructValueIsNotLogicalMessage() {
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "topic",
                        Schema.STRING_SCHEMA,
                        "not-a-struct");
        assertThat(PostgresSourceStreamFetcher.isLogicalMessage(record)).isFalse();
    }

    private static SourceRecord recordWithOp(String op) {
        Struct value = new Struct(ENVELOPE_WITH_OP).put(Envelope.FieldName.OPERATION, op);
        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "topic", ENVELOPE_WITH_OP, value);
    }
}
