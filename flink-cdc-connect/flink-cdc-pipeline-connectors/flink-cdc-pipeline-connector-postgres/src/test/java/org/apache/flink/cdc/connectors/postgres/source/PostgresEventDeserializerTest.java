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

import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PostgresEventDeserializer}. */
class PostgresEventDeserializerTest {

    @Test
    void testNullBeforeThrowsNpeWithReplicaIdentityHint() throws Exception {
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        // A value schema whose BEFORE field is optional, and a value struct that leaves BEFORE
        // unset so that fieldStruct(value, BEFORE) returns null (as happens for UPDATE events when
        // the source table is not configured with REPLICA IDENTITY FULL).
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.namespace")
                        .field(Envelope.FieldName.BEFORE, SchemaBuilder.struct().optional().build())
                        .build();
        Struct value = new Struct(valueSchema);

        assertThatThrownBy(() -> deserializer.extractBeforeDataRecord(value, valueSchema))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Before data is null")
                .hasMessageContaining("REPLICA IDENTITY FULL");
    }
}
