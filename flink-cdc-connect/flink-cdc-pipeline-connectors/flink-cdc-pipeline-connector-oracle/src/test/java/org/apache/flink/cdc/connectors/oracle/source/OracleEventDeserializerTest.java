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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class OracleEventDeserializerTest {

    @Test
    void shouldDeserializeSourceStructToDatabaseSchemaTableId() {
        TestOracleEventDeserializer deserializer = new TestOracleEventDeserializer();
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, sourceSchema).build();
        Struct source =
                new Struct(sourceSchema)
                        .put("db", "DLZSJT")
                        .put("schema", "SOUTH")
                        .put("table", "T_IM_SALEISSUEENTRY");
        Struct value = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, source);
        SourceRecord record =
                new SourceRecord(
                        null,
                        null,
                        "oracle_logminer.SOUTH.T_IM_SALEISSUEENTRY",
                        null,
                        null,
                        null,
                        valueSchema,
                        value);

        TableId tableId = deserializer.getTableId(record);

        assertThat(tableId.getNamespace()).isEqualTo("DLZSJT");
        assertThat(tableId.getSchemaName()).isEqualTo("SOUTH");
        assertThat(tableId.getTableName()).isEqualTo("T_IM_SALEISSUEENTRY");
        assertThat(tableId.toString()).isEqualTo("DLZSJT.SOUTH.T_IM_SALEISSUEENTRY");
    }

    private static class TestOracleEventDeserializer extends OracleEventDeserializer {
        private TestOracleEventDeserializer() {
            super(DebeziumChangelogMode.ALL, true, Collections.emptyList());
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return super.getTableId(record);
        }
    }
}
