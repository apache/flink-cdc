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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link LanceDbEventWriter}. */
class LanceDbEventWriterTest {

    @Test
    void testAutoFlushesWhenMaxRowsReached() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config("append-only", 2, 0, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        client);

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
        writer.write(insert(1), null);
        writer.write(insert(2), null);

        Assertions.assertThat(client.appendCalls).hasSize(1);
        Assertions.assertThat(client.appendCalls.get(0).rows).hasSize(2);
    }

    @Test
    void testRejectsUpdateInAppendOnlyMode() throws Exception {
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.defaultConfig(),
                        LanceDbSinkMetrics.NOOP,
                        new LanceDbTestUtils.RecordingClient());

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.updateEvent(
                                                LanceDbTestUtils.TABLE_ID,
                                                LanceDbTestUtils.record(1, "old", 1.0D),
                                                LanceDbTestUtils.record(1, "new", 2.0D)),
                                        null))
                .isInstanceOf(java.io.IOException.class)
                .hasRootCauseMessage(
                        "LanceDB append-only mode rejects UPDATE events. Use sink.changelog-mode=append-with-metadata to persist CDC logs.");
    }

    @Test
    void testAppendWithMetadataAcceptsDelete() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config(
                                "append-with-metadata", 100, 0, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        client);

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
        writer.write(
                DataChangeEvent.deleteEvent(
                        LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.record(1, "old", 1.0D)),
                null);
        writer.flush(false);

        Assertions.assertThat(client.appendCalls).hasSize(1);
        Assertions.assertThat(client.appendCalls.get(0).schema.findField("_cdc_op")).isNotNull();
        Assertions.assertThat(client.appendCalls.get(0).rows.get(0)).contains("DELETE", true);
    }

    @Test
    void testRejectsReservedCdcMetadataColumnInWriter() {
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config(
                                "append-with-metadata", 100, 0, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        new LanceDbTestUtils.RecordingClient());

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        new CreateTableEvent(
                                                LanceDbTestUtils.TABLE_ID, schemaWithCdcColumn()),
                                        null))
                .isInstanceOf(java.io.IOException.class)
                .hasRootCauseMessage(
                        "Source schema contains reserved LanceDB CDC metadata column _cdc_op. Rename the source column or use sink.changelog-mode=append-only.");
    }

    @Test
    void testRetriesRetryableFailureAndReopensDataset() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        client.remainingFailures = 1;
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config("append-only", 100, 1, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        client);

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
        writer.write(insert(1), null);
        writer.flush(false);

        Assertions.assertThat(client.appendCalls).hasSize(1);
        Assertions.assertThat(client.reopened).contains("/tmp/lancedb/inventory_products.lance");
    }

    @Test
    void testSplitsLargeFlushByMaxRowsPerCommit() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config("append-only", 100, 2, 0, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        client);

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
        for (int id = 1; id <= 5; id++) {
            writer.write(insert(id), null);
        }
        writer.flush(false);

        Assertions.assertThat(client.appendCalls).hasSize(3);
        Assertions.assertThat(client.appendCalls)
                .extracting(call -> call.rows.size())
                .containsExactly(2, 2, 1);
    }

    @Test
    void testDoesNotReplaySuccessfulBatchAfterLaterBatchFailure() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        client.failOnAppendCall = 2;
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        LanceDbTestUtils.config("append-only", 100, 2, 1, Duration.ZERO, false),
                        LanceDbSinkMetrics.NOOP,
                        client);

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
        for (int id = 1; id <= 4; id++) {
            writer.write(insert(id), null);
        }
        writer.flush(false);

        Assertions.assertThat(client.appendCalls).hasSize(2);
        Assertions.assertThat(client.appendCalls.get(0).rows)
                .extracting(row -> row.get(0))
                .containsExactly(1, 2);
        Assertions.assertThat(client.appendCalls.get(1).rows)
                .extracting(row -> row.get(0))
                .containsExactly(3, 4);
    }

    @Test
    void testRejectsDatasetPathOwnershipConflictAtRuntime() throws Exception {
        LanceDbEventWriter writer =
                new LanceDbEventWriter(
                        configWithPathConflict(),
                        LanceDbSinkMetrics.NOOP,
                        new LanceDbTestUtils.RecordingClient());

        writer.write(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        new CreateTableEvent(
                                                TableId.parse("inventory.products_b"),
                                                LanceDbTestUtils.SCHEMA),
                                        null))
                .isInstanceOf(java.io.IOException.class)
                .hasRootCauseMessage(
                        "Lance dataset path /tmp/lancedb/products.lance is already owned by source table inventory.products and cannot also be used by inventory.products_b. Configure table.path.mapping to unique dataset paths.");
    }

    private DataChangeEvent insert(int id) {
        return DataChangeEvent.insertEvent(
                LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.record(id, "name-" + id, id));
    }

    private Schema schemaWithCdcColumn() {
        return Schema.newBuilder()
                .physicalColumn("_cdc_op", DataTypes.STRING())
                .physicalColumn("payload", DataTypes.STRING())
                .build();
    }

    private LanceDbDataSinkConfig configWithPathConflict() {
        Map<TableId, String> mappings = new HashMap<>();
        mappings.put(LanceDbTestUtils.TABLE_ID, "/tmp/lancedb/products.lance");
        mappings.put(TableId.parse("inventory.products_b"), "/tmp/lancedb/products.lance");
        return new LanceDbDataSinkConfig(
                "/tmp/lancedb",
                mappings,
                true,
                true,
                true,
                false,
                "append-only",
                100,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                1024,
                128,
                1024 * 1024L,
                true,
                "CREATE",
                ZoneId.of("UTC"),
                Collections.emptyMap());
    }
}
