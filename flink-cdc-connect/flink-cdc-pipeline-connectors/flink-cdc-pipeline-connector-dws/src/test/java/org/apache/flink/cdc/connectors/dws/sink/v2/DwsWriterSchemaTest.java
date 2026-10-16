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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Schema cache lifecycle tests for {@link DwsWriter}. */
class DwsWriterSchemaTest {

    private static final TableId MIXED_CASE_TABLE = TableId.tableId("Sales", "Customers");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("Id", DataTypes.INT())
                    .physicalColumn("Name", DataTypes.STRING())
                    .primaryKey("Id")
                    .build();

    @Test
    void reloadsNativeSchemaAfterCreateAndEvolutionAndRefreshesPrimaryKeyGetter() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(true, client);

        writer.write(new CreateTableEvent(MIXED_CASE_TABLE, INITIAL_SCHEMA), null);
        writer.write(
                new AddColumnEvent(
                        MIXED_CASE_TABLE,
                        Collections.singletonList(
                                AddColumnEvent.first(
                                        Column.physicalColumn("Prefix", DataTypes.STRING())))),
                null);

        Schema evolvedSchema =
                Schema.newBuilder()
                        .physicalColumn("Prefix", DataTypes.STRING())
                        .physicalColumn("Id", DataTypes.INT())
                        .physicalColumn("Name", DataTypes.STRING())
                        .primaryKey("Id")
                        .build();
        RecordData evolvedRow =
                new BinaryRecordDataGenerator((RowType) evolvedSchema.toRowDataType())
                        .generate(
                                new Object[] {
                                    BinaryStringData.fromString("VIP"),
                                    7,
                                    BinaryStringData.fromString("Alice")
                                });
        writer.write(DataChangeEvent.deleteEvent(MIXED_CASE_TABLE, evolvedRow), null);

        assertThat(client.refreshedTables).containsExactly("Sales.Customers", "Sales.Customers");
        assertThat(client.deletes).hasSize(1);
        assertThat(client.deletes.get(0)).containsEntry("Id", 7).doesNotContainKey("Prefix");
    }

    @Test
    void normalizesNativeSchemaLookupWhenIdentifiersAreCaseInsensitive() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(false, client);

        writer.write(new CreateTableEvent(MIXED_CASE_TABLE, INITIAL_SCHEMA), null);
        writer.write(new CreateTableEvent(MIXED_CASE_TABLE, INITIAL_SCHEMA), null);

        assertThat(client.refreshedTables).containsExactly("sales.customers", "sales.customers");
    }

    @Test
    void dropOnlyInvalidatesAndTruncateDoesNotReloadSchema() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(true, client);
        writer.write(new CreateTableEvent(MIXED_CASE_TABLE, INITIAL_SCHEMA), null);
        client.refreshedTables.clear();

        writer.write(new TruncateTableEvent(MIXED_CASE_TABLE), null);
        writer.write(new DropTableEvent(MIXED_CASE_TABLE), null);

        assertThat(client.refreshedTables).isEmpty();
        assertThat(client.removedTables).containsExactly("Sales.Customers");
    }

    @Test
    void failedNativeReloadDoesNotPublishNewConnectorSchema() {
        RecordingClient client = new RecordingClient();
        client.refreshFailure = new IOException("metadata reload failed");
        DwsWriter writer = createWriter(true, client);

        assertThatThrownBy(
                        () ->
                                writer.write(
                                        new CreateTableEvent(MIXED_CASE_TABLE, INITIAL_SCHEMA),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("metadata reload failed");

        RecordData row =
                new BinaryRecordDataGenerator((RowType) INITIAL_SCHEMA.toRowDataType())
                        .generate(new Object[] {1, BinaryStringData.fromString("Alice")});
        assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.insertEvent(MIXED_CASE_TABLE, row), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schema cache is missing");
    }

    private static DwsWriter createWriter(boolean caseSensitive, RecordingClient client) {
        return new DwsWriter(
                DwsDataSinkConfig.builder()
                        .withUrl("jdbc:gaussdb://localhost:8000/test")
                        .withUsername("user")
                        .withPassword("password")
                        .withZoneId(ZoneId.of("UTC"))
                        .withCaseSensitive(caseSensitive)
                        .build(),
                client);
    }

    private static final class RecordingClient implements DwsClientFacade {
        private final List<String> refreshedTables = new ArrayList<>();
        private final List<String> removedTables = new ArrayList<>();
        private final List<Map<String, Object>> deletes = new ArrayList<>();
        private IOException refreshFailure;

        public void refreshTableSchema(String tableName) throws IOException {
            refreshedTables.add(tableName);
            if (refreshFailure != null) {
                throw refreshFailure;
            }
        }

        public void removeTableSchema(String tableName) {
            removedTables.add(tableName);
        }

        @Override
        public void write(String tableName, Map<String, Object> values) {}

        @Override
        public void delete(String tableName, Map<String, Object> values) {
            deletes.add(new LinkedHashMap<>(values));
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
