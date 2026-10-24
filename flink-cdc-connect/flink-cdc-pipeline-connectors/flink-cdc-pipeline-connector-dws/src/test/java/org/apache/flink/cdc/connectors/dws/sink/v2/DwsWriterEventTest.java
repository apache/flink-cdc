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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Event mapping contract for the official native DWS client writer. */
class DwsWriterEventTest {

    private static final TableId TABLE = TableId.tableId("analytics", "customers");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("score", DataTypes.DECIMAL(10, 2))
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator((RowType) SCHEMA.toRowDataType());

    @Test
    void writesCompleteRowsAndDeletesOnlyPrimaryKeys() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(true, client);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);

        writer.write(DataChangeEvent.insertEvent(TABLE, row(1, "Alice", "12.30")), null);
        writer.write(DataChangeEvent.deleteEvent(TABLE, row(1, "ignored", "99.00")), null);

        assertThat(client.calls)
                .containsExactly(
                        new Call(
                                "WRITE",
                                "analytics.customers",
                                mapOf(
                                        "id",
                                        1,
                                        "name",
                                        "Alice",
                                        "score",
                                        new java.math.BigDecimal("12.30"))),
                        new Call("DELETE", "analytics.customers", mapOf("id", 1)));
    }

    @Test
    void alwaysExecutesUpdateBeforeRetractionWhenIndependentDeletesAreDisabled() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(false, client);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);

        writer.write(DataChangeEvent.deleteEvent(TABLE, row(1, "old", "1.00")), null);
        writer.write(DataChangeEvent.updateBeforeEvent(TABLE, row(2, "old", "2.00")), null);

        assertThat(client.calls)
                .containsExactly(new Call("DELETE", "analytics.customers", mapOf("id", 2)));
    }

    @Test
    void rejectsUnsplitPrimaryKeyChangingUpdateButAllowsSameKeyUpdate() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(true, client);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);

        writer.write(
                DataChangeEvent.updateEvent(TABLE, row(1, "old", "1.00"), row(1, "new", "2.00")),
                null);
        assertThat(client.calls).hasSize(1);
        assertThat(client.calls.get(0).operation).isEqualTo("WRITE");

        assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.updateEvent(
                                                TABLE,
                                                row(1, "old", "1.00"),
                                                row(2, "new", "2.00")),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("split");
        assertThat(client.calls).hasSize(1);
    }

    @Test
    void rejectsTablesWithoutPrimaryKeysBeforeCallingNativeClient() {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(true, client);
        Schema noPrimaryKey = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();

        assertThatThrownBy(() -> writer.write(new CreateTableEvent(TABLE, noPrimaryKey), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");
        assertThat(client.calls).isEmpty();
    }

    private static DwsWriter createWriter(boolean enableDelete, RecordingClient client) {
        DwsDataSinkConfig config =
                DwsDataSinkConfig.builder()
                        .withUrl("jdbc:gaussdb://localhost:8000/test")
                        .withUsername("user")
                        .withPassword("password")
                        .withZoneId(ZoneId.of("UTC"))
                        .withEnableDelete(enableDelete)
                        .build();
        return new DwsWriter(config, client);
    }

    private static RecordData row(int id, String name, String score) {
        return GENERATOR.generate(
                new Object[] {
                    id,
                    BinaryStringData.fromString(name),
                    org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                            new java.math.BigDecimal(score), 10, 2)
                });
    }

    private static Map<String, Object> mapOf(Object... entries) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            values.put((String) entries[i], entries[i + 1]);
        }
        return values;
    }

    private static final class RecordingClient implements DwsClientFacade {
        private final List<Call> calls = new ArrayList<>();

        @Override
        public void write(String tableName, Map<String, Object> values) {
            calls.add(new Call("WRITE", tableName, values));
        }

        @Override
        public void delete(String tableName, Map<String, Object> values) {
            calls.add(new Call("DELETE", tableName, values));
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static final class Call {
        private final String operation;
        private final String tableName;
        private final Map<String, Object> values;

        private Call(String operation, String tableName, Map<String, Object> values) {
            this.operation = operation;
            this.tableName = tableName;
            this.values = values;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Call)) {
                return false;
            }
            Call that = (Call) object;
            return operation.equals(that.operation)
                    && tableName.equals(that.tableName)
                    && values.equals(that.values);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(operation, tableName, values);
        }

        @Override
        public String toString() {
            return operation + " " + tableName + " " + values;
        }
    }
}
