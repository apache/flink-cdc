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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Conservative writer-side byte budget tests. */
class DwsWriterBufferTest {

    private static final TableId TABLE_A = TableId.tableId("ods", "a");
    private static final TableId TABLE_B = TableId.tableId("ods", "b");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("payload", DataTypes.BYTES())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator((RowType) SCHEMA.toRowDataType());

    @Test
    void rejectsSingleBinaryRecordBeforeNativeCommit() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(client, 220, 220, 220);
        writer.write(new CreateTableEvent(TABLE_A, SCHEMA), null);

        assertThatThrownBy(
                        () -> writer.write(DataChangeEvent.insertEvent(TABLE_A, row(1, 128)), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("single DWS record")
                .hasMessageContaining("budget");
        assertThat(client.writtenTables).isEmpty();
    }

    @Test
    void flushesBeforeNextRecordWouldExceedTableBudget() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(client, 600, 300, 300);
        writer.write(new CreateTableEvent(TABLE_A, SCHEMA), null);

        writer.write(DataChangeEvent.insertEvent(TABLE_A, row(1, 16)), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_A, row(2, 16)), null);

        assertThat(client.operations).containsExactly("WRITE:ods.a", "FLUSH", "WRITE:ods.a");
    }

    @Test
    void enforcesAggregateBudgetAcrossTables() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(client, 350, 300, 300);
        writer.write(new CreateTableEvent(TABLE_A, SCHEMA), null);
        writer.write(new CreateTableEvent(TABLE_B, SCHEMA), null);

        writer.write(DataChangeEvent.insertEvent(TABLE_A, row(1, 16)), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_B, row(2, 16)), null);

        assertThat(client.operations).containsExactly("WRITE:ods.a", "FLUSH", "WRITE:ods.b");
    }

    @Test
    void clearsCountersOnlyAfterSuccessfulFlush() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = createWriter(client, 600, 300, 300);
        writer.write(new CreateTableEvent(TABLE_A, SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_A, row(1, 16)), null);
        client.flushFailure = new IOException("flush failed");

        assertThatThrownBy(
                        () -> writer.write(DataChangeEvent.insertEvent(TABLE_A, row(2, 16)), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("flush failed");

        client.flushFailure = null;
        writer.write(DataChangeEvent.insertEvent(TABLE_A, row(2, 16)), null);
        assertThat(client.operations)
                .containsExactly("WRITE:ods.a", "FLUSH", "FLUSH", "WRITE:ods.a");
    }

    private static DwsWriter createWriter(
            RecordingClient client, long allBytes, long tableBytes, long partitionBytes) {
        return new DwsWriter(
                DwsDataSinkConfig.builder()
                        .withUrl("jdbc:gaussdb://localhost:8000/test")
                        .withUsername("user")
                        .withPassword("password")
                        .withZoneId(ZoneId.of("UTC"))
                        .withBufferAllMaxBytes(allBytes)
                        .withBufferTableMaxBytes(tableBytes)
                        .withBufferPartitionMaxBytes(partitionBytes)
                        .build(),
                client);
    }

    private static RecordData row(int id, int payloadBytes) {
        return GENERATOR.generate(new Object[] {id, new byte[payloadBytes]});
    }

    private static final class RecordingClient implements DwsClientFacade {
        private final List<String> operations = new ArrayList<>();
        private final List<String> writtenTables = new ArrayList<>();
        private IOException flushFailure;

        @Override
        public void write(String tableName, Map<String, Object> values) {
            operations.add("WRITE:" + tableName);
            writtenTables.add(tableName);
        }

        @Override
        public void delete(String tableName, Map<String, Object> values) {}

        @Override
        public void flush() throws IOException {
            operations.add("FLUSH");
            if (flushFailure != null) {
                throw flushFailure;
            }
        }

        @Override
        public void close() {}
    }
}
