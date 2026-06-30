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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.Envelope;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MySqlEventDeserializer}. */
class MySqlEventDeserializerTest {

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    @Test
    void testGetTableIdLowercasesOnlyTableIdentityWhenCaseInsensitive() {
        TestingMySqlEventDeserializer deserializer = new TestingMySqlEventDeserializer(true);

        TableId tableId =
                deserializer.extractTableId(createRecord("InventoryDB", "MixedCaseOrders"));

        assertThat(tableId).isEqualTo(TableId.tableId("inventorydb", "mixedcaseorders"));
    }

    @Test
    void testGetTableIdPreservesCaseWhenCaseSensitive() {
        TestingMySqlEventDeserializer deserializer = new TestingMySqlEventDeserializer(false);

        TableId tableId =
                deserializer.extractTableId(createRecord("InventoryDB", "MixedCaseOrders"));

        assertThat(tableId).isEqualTo(TableId.tableId("InventoryDB", "MixedCaseOrders"));
    }

    @Test
    void testConvertToStringUsesSharedDecimalConversion() {
        TestingMySqlEventDeserializer deserializer = new TestingMySqlEventDeserializer(false);
        Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(1)
                        .parameter(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY, "65")
                        .build();

        byte[] decimalBytes =
                org.apache.kafka.connect.data.Decimal.fromLogical(
                        decimalSchema, new BigDecimal("12345678901234567890.1"));

        assertThat(deserializer.convertToStringForTest(decimalBytes, decimalSchema))
                .isEqualTo(BinaryStringData.fromString("12345678901234567890.1"));
    }

    @Test
    void testDeserializeColumnCommentOnlyAlter() throws Exception {
        TestingMySqlEventDeserializer deserializer = new TestingMySqlEventDeserializer(false);
        SourceRecord createTableRecord =
                createSchemaChangeRecord(
                        "inventory",
                        "CREATE TABLE student ("
                                + "id BIGINT NOT NULL COMMENT 'student id',"
                                + "AGE INT DEFAULT 30 COMMENT 'old age comment',"
                                + "JOB VARCHAR(64) COMMENT 'old job',"
                                + "PRIMARY KEY (id))");
        SourceRecord alterAgeTypeRecord =
                createSchemaChangeRecord(
                        "inventory",
                        "ALTER TABLE student MODIFY COLUMN AGE BIGINT DEFAULT 30 COMMENT 'old age comment'");
        SourceRecord alterJobRecord =
                createSchemaChangeRecord(
                        "inventory",
                        "ALTER TABLE student MODIFY COLUMN JOB VARCHAR(255) COMMENT 'new job'");
        SourceRecord alterAgeCommentRecord =
                createSchemaChangeRecord(
                        "inventory",
                        "ALTER TABLE student MODIFY COLUMN AGE BIGINT DEFAULT 30 COMMENT 'updated age comment'");

        deserializer.deserialize(createTableRecord);
        deserializer.deserialize(alterAgeTypeRecord);
        deserializer.deserialize(alterJobRecord);
        List<? extends Event> events = deserializer.deserialize(alterAgeCommentRecord);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent alterEvent = (AlterColumnTypeEvent) events.get(0);
        assertThat(alterEvent.tableId()).isEqualTo(TableId.tableId("inventory", "student"));
        assertThat(alterEvent.getTypeMapping())
                .containsExactlyEntriesOf(Collections.singletonMap("AGE", DataTypes.BIGINT()));
        assertThat(alterEvent.getComments())
                .containsExactlyEntriesOf(Collections.singletonMap("AGE", "updated age comment"));
    }

    private static SourceRecord createRecord(String databaseName, String tableName) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, sourceSchema).build();
        Struct source =
                new Struct(sourceSchema)
                        .put(DATABASE_NAME_KEY, databaseName)
                        .put(TABLE_NAME_KEY, tableName);
        Struct value = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, source);

        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "mysql-binlog",
                null,
                null,
                null,
                valueSchema,
                value);
    }

    private static SourceRecord createSchemaChangeRecord(String databaseName, String ddl)
            throws Exception {
        HistoryRecord historyRecord =
                new HistoryRecord(
                        Collections.singletonMap("file", "mysql-bin.000001"),
                        Collections.singletonMap("file", "mysql-bin.000001"),
                        databaseName,
                        null,
                        ddl,
                        new TableChanges());
        Schema keySchema =
                SchemaBuilder.struct()
                        .name(MySqlEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                        .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                        .build();
        Struct key = new Struct(keySchema).put(HistoryRecord.Fields.DATABASE_NAME, databaseName);
        Schema valueSchema =
                SchemaBuilder.struct().field(HISTORY_RECORD_FIELD, Schema.STRING_SCHEMA).build();
        Struct value =
                new Struct(valueSchema)
                        .put(HISTORY_RECORD_FIELD, DOCUMENT_WRITER.write(historyRecord.document()));
        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", 1L);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset,
                "mysql.schema_change",
                keySchema,
                key,
                valueSchema,
                value);
    }

    private static final class TestingMySqlEventDeserializer extends MySqlEventDeserializer {

        private TestingMySqlEventDeserializer(boolean isTableIdCaseInsensitive) {
            super(
                    DebeziumChangelogMode.ALL,
                    false,
                    new ArrayList<>(),
                    true,
                    true,
                    isTableIdCaseInsensitive);
        }

        private TableId extractTableId(SourceRecord record) {
            return getTableId(record);
        }

        private BinaryStringData convertToStringForTest(Object dbzObj, Schema schema) {
            return (BinaryStringData) convertToString(dbzObj, schema);
        }
    }
}
