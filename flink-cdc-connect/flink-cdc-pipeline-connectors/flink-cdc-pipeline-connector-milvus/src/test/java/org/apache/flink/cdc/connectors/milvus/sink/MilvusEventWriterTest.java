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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.metrics.Counter;

import com.google.gson.JsonObject;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Tests for {@link MilvusEventWriter}. */
class MilvusEventWriterTest {

    private static final TableId TABLE_ID = TableId.parse("inventory.docs");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .primaryKey("id")
                    .build();
    private static final Schema PARTITIONED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("category", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .primaryKey("id")
                    .build();

    @Test
    void testPartialAdaptiveSplitRetainsCompletedRowsOnRetry() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.failUpsertIdOnce = 3L;
        MilvusDataSinkConfig config = partialAdaptiveSplitConfig(1, 1);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());
        writer.write(insert(3L, "doc-3", 3.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs [1]",
                        "upsert default inventory_docs [2]",
                        "upsert default inventory_docs [3]");
    }

    @Test
    void testReplayUpsertIsIdempotentWithinWriter() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusEventWriter writer =
                new MilvusEventWriter(
                        MilvusTestUtils.defaultConfig(), MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.flush(false);
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs [1]", "upsert default inventory_docs [1]");
    }

    @Test
    void testPartialFlushRetriesOnlyRemainingBatches() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.remainingDeleteFailures = 1;
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());
        writer.write(
                DataChangeEvent.deleteEvent(TABLE_ID, record(2L, "doc-2", 2.0f)),
                new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs [1, 2]",
                        "delete default inventory_docs [2]");
        Assertions.assertThat(client.upsertInvocations).isEqualTo(1);
    }

    @Test
    void testNonRetryableFlushFailureDoesNotRetry() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.nonRetryableUpsertFailure = true;
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        3,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());

        Assertions.assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush Milvus sink");
        Assertions.assertThat(client.upsertInvocations).isEqualTo(1);
    }

    @Test
    void testReconnectsAfterConnectionFailure() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.remainingConnectionFailures = 1;
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.reconnectCount).isEqualTo(1);
        Assertions.assertThat(client.calls).containsExactly("upsert default inventory_docs [1]");
    }

    @Test
    void testConnectionFailureRetriesWholeBatchWithoutAdaptiveSplit() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.remainingConnectionFailures = 1;
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());
        writer.write(insert(3L, "doc-3", 3.0f), new MockContext());
        writer.write(insert(4L, "doc-4", 4.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.upsertInvocations).isEqualTo(2);
        Assertions.assertThat(client.reconnectCount).isEqualTo(1);
        Assertions.assertThat(client.calls)
                .containsExactly("upsert default inventory_docs [1, 2, 3, 4]");
    }

    @Test
    void testFlushesOrderedBatchesByOperationTypeAndCollection() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        "http://localhost:19530",
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        0,
                        Duration.ZERO,
                        true,
                        true,
                        "",
                        "allow");
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());
        writer.write(
                DataChangeEvent.updateEvent(
                        TABLE_ID, record(2L, "doc-2", 2.0f), record(3L, "doc-3", 3.0f)),
                new MockContext());
        writer.write(
                DataChangeEvent.deleteEvent(TABLE_ID, record(3L, "doc-3", 3.0f)),
                new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs [1, 2]",
                        "delete default inventory_docs [2]",
                        "upsert default inventory_docs [3]",
                        "delete default inventory_docs [3]");
    }

    @Test
    void testAutoFlushesWhenMaxRowsReached() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        2,
                        Duration.ofSeconds(10),
                        0,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());

        Assertions.assertThat(client.calls).containsExactly("upsert default inventory_docs [1, 2]");
    }

    @Test
    void testFlushRetriesAndRecordsMetrics() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.remainingUpsertFailures = 1;
        TestingMilvusSinkMetrics metrics = new TestingMilvusSinkMetrics();
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(25),
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, metrics, client);

        long startMillis = System.currentTimeMillis();
        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(System.currentTimeMillis() - startMillis).isGreaterThanOrEqualTo(25);
        Assertions.assertThat(client.calls).containsExactly("upsert default inventory_docs [1]");
        Assertions.assertThat(metrics.pendingRows).isZero();
        Assertions.assertThat(metrics.successfulFlushRows.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.flushFailures.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.retries.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.recordsOutErrors.getCount()).isZero();
    }

    @Test
    void testFinalFlushFailureRecordsOutputErrorsAndKeepsPendingRows() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.remainingUpsertFailures = 2;
        TestingMilvusSinkMetrics metrics = new TestingMilvusSinkMetrics();
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ZERO,
                        true);
        MilvusEventWriter writer = new MilvusEventWriter(config, metrics, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());

        Assertions.assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush Milvus sink");
        Assertions.assertThat(client.calls).isEmpty();
        Assertions.assertThat(metrics.flushFailures.getCount()).isEqualTo(2);
        Assertions.assertThat(metrics.retries.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.recordsOutErrors.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.pendingRows).isEqualTo(1);
    }

    @Test
    void testVectorDimensionMismatchFailsBeforeClientCall() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusEventWriter writer =
                new MilvusEventWriter(
                        MilvusTestUtils.defaultConfig(), MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.insertEvent(
                                                TABLE_ID,
                                                GenericRecordData.of(
                                                        1L,
                                                        BinaryStringData.fromString("doc-1"),
                                                        new GenericArrayData(
                                                                new float[] {1.0f, 2.0f}))),
                                        new MockContext()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to write event to Milvus sink")
                .hasRootCauseMessage(
                        "Vector field embedding dimension mismatch. Expected 3 but was 2.");
        Assertions.assertThat(client.calls).isEmpty();
    }

    @Test
    void testWritesToConfiguredPartitionAndAutoCreatesMissingPartition() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(true, 100, 0, Duration.ZERO, true, 10);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(partitionedInsert(1L, "doc-1", "manual", 1.0f), new MockContext());
        writer.write(partitionedInsert(2L, "doc-2", "manual", 2.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "createPartition default inventory_docs manual",
                        "upsert default inventory_docs partition=manual [1, 2]");
    }

    @Test
    void testAutoCreatedPartitionIsCachedAcrossFlushes() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(true, 100, 0, Duration.ZERO, true, 10);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(partitionedInsert(1L, "doc-1", "manual", 1.0f), new MockContext());
        writer.flush(false);
        writer.write(partitionedInsert(2L, "doc-2", "manual", 2.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "createPartition default inventory_docs manual",
                        "upsert default inventory_docs partition=manual [1]",
                        "upsert default inventory_docs partition=manual [2]");
        Assertions.assertThat(client.hasPartitionInvocations).isEqualTo(1);
    }

    @Test
    void testPartitionAutoCreateLimitFailsFast() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(true, 100, 0, Duration.ZERO, true, 10, 1);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(partitionedInsert(1L, "doc-1", "manual", 1.0f), new MockContext());
        writer.flush(false);
        writer.write(partitionedInsert(2L, "doc-2", "auto", 2.0f), new MockContext());

        Assertions.assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage(
                        "Milvus partition auto-create limit exceeded. collection=inventory_docs, partition=auto, limit=1. Configure partition.names or increase partition.auto-create.max-count only after validating partition cardinality.");
        Assertions.assertThat(client.calls)
                .containsExactly(
                        "createPartition default inventory_docs manual",
                        "upsert default inventory_docs partition=manual [1]");
    }

    @Test
    void testKeepsPartitionOperationOrder() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(false, 100, 0, Duration.ZERO, true, 10);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(partitionedInsert(1L, "doc-1", "manual", 1.0f), new MockContext());
        writer.write(partitionedInsert(2L, "doc-2", "auto", 2.0f), new MockContext());
        writer.write(
                DataChangeEvent.deleteEvent(
                        TABLE_ID, partitionedRecord(1L, "doc-1", "manual", 1.0f)),
                new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs partition=manual [1]",
                        "upsert default inventory_docs partition=auto [2]",
                        "delete default inventory_docs partition=manual [1]");
    }

    @Test
    void testKeepsOrderWhenPartitionMovesToDefault() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(false, 100, 0, Duration.ZERO, true, 10);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(
                DataChangeEvent.updateEvent(
                        TABLE_ID,
                        partitionedRecord(1L, "old", "manual", 1.0f),
                        partitionedRecord(1L, "new", null, 4.0f)),
                new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "delete default inventory_docs partition=manual [1]",
                        "upsert default inventory_docs [1]");
    }

    @Test
    void testKeepsOrderWhenPartitionMovesFromDefault() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        MilvusDataSinkConfig config = partitionConfig(false, 100, 0, Duration.ZERO, true, 10);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, PARTITIONED_SCHEMA), new MockContext());
        writer.write(
                DataChangeEvent.updateEvent(
                        TABLE_ID,
                        partitionedRecord(1L, "old", null, 1.0f),
                        partitionedRecord(1L, "new", "auto", 4.0f)),
                new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "delete default inventory_docs [1]",
                        "upsert default inventory_docs partition=auto [1]");
    }

    @Test
    void testAdaptiveSplitRetriesLargeUpsertBatch() throws Exception {
        RecordingMilvusClient client = new RecordingMilvusClient();
        client.failLargeUpsertBatches = true;
        MilvusDataSinkConfig config = adaptiveSplitConfig(1);
        MilvusEventWriter writer = new MilvusEventWriter(config, MilvusSinkMetrics.NOOP, client);

        writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), new MockContext());
        writer.write(insert(1L, "doc-1", 1.0f), new MockContext());
        writer.write(insert(2L, "doc-2", 2.0f), new MockContext());
        writer.write(insert(3L, "doc-3", 3.0f), new MockContext());
        writer.write(insert(4L, "doc-4", 4.0f), new MockContext());
        writer.flush(false);

        Assertions.assertThat(client.calls)
                .containsExactly(
                        "upsert default inventory_docs [1, 2]",
                        "upsert default inventory_docs [3, 4]");
    }

    private static DataChangeEvent insert(long id, String title, float firstVectorValue) {
        return DataChangeEvent.insertEvent(TABLE_ID, record(id, title, firstVectorValue));
    }

    private static GenericRecordData record(long id, String title, float firstVectorValue) {
        return GenericRecordData.of(
                id,
                BinaryStringData.fromString(title),
                new GenericArrayData(
                        new float[] {
                            firstVectorValue, firstVectorValue + 1, firstVectorValue + 2
                        }));
    }

    private static DataChangeEvent partitionedInsert(
            long id, String title, String category, float firstVectorValue) {
        return DataChangeEvent.insertEvent(
                TABLE_ID, partitionedRecord(id, title, category, firstVectorValue));
    }

    private static GenericRecordData partitionedRecord(
            long id, String title, String category, float firstVectorValue) {
        return GenericRecordData.of(
                id,
                BinaryStringData.fromString(title),
                category == null ? null : BinaryStringData.fromString(category),
                new GenericArrayData(
                        new float[] {
                            firstVectorValue, firstVectorValue + 1, firstVectorValue + 2
                        }));
    }

    private static MilvusDataSinkConfig partitionConfig(
            boolean autoCreatePartition,
            int flushMaxRows,
            int maxRetries,
            Duration retryBackoff,
            boolean adaptiveSplitEnabled,
            int adaptiveSplitMinRows) {
        return partitionConfig(
                autoCreatePartition,
                flushMaxRows,
                maxRetries,
                retryBackoff,
                adaptiveSplitEnabled,
                adaptiveSplitMinRows,
                1024);
    }

    private static MilvusDataSinkConfig partitionConfig(
            boolean autoCreatePartition,
            int flushMaxRows,
            int maxRetries,
            Duration retryBackoff,
            boolean adaptiveSplitEnabled,
            int adaptiveSplitMinRows,
            int partitionAutoCreateMaxCount) {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "category",
                autoCreatePartition,
                partitionAutoCreateMaxCount,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                flushMaxRows,
                Duration.ofSeconds(10),
                maxRetries,
                retryBackoff,
                adaptiveSplitEnabled,
                adaptiveSplitMinRows,
                true,
                "reject",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }

    private static MilvusDataSinkConfig partialAdaptiveSplitConfig(
            int adaptiveSplitMinRows, int maxRetries) {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "",
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                Duration.ofSeconds(10),
                maxRetries,
                Duration.ZERO,
                true,
                adaptiveSplitMinRows,
                true,
                "reject",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }

    private static MilvusDataSinkConfig adaptiveSplitConfig(int adaptiveSplitMinRows) {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "",
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                true,
                adaptiveSplitMinRows,
                true,
                "reject",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }

    private static class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private static class RecordingMilvusClient implements MilvusClientWrapper {

        private final List<String> calls = new ArrayList<>();
        private final Set<String> partitions = new HashSet<>();
        private int remainingUpsertFailures;
        private int remainingDeleteFailures;
        private int remainingConnectionFailures;
        private boolean failLargeUpsertBatches;
        private boolean failCreatePartitionAlreadyExistsOnce;
        private boolean nonRetryableUpsertFailure;
        private Long failUpsertIdOnce;
        private int upsertInvocations;
        private int reconnectCount;
        private int hasPartitionInvocations;
        private boolean closed;

        @Override
        public void upsert(String databaseName, String collectionName, List<JsonObject> rows) {
            upsert(databaseName, collectionName, null, rows);
        }

        @Override
        public void upsert(
                String databaseName,
                String collectionName,
                String partitionName,
                List<JsonObject> rows) {
            upsertInvocations++;
            if (nonRetryableUpsertFailure) {
                throw new IllegalArgumentException("Vector field embedding dimension mismatch.");
            }
            if (remainingConnectionFailures > 0) {
                remainingConnectionFailures--;
                throw new RuntimeException("connection reset by peer");
            }
            if (remainingUpsertFailures > 0) {
                remainingUpsertFailures--;
                throw new RuntimeException("connection reset by peer");
            }
            if (failLargeUpsertBatches && rows.size() > 2) {
                throw new RuntimeException("received message larger than max");
            }
            if (failUpsertIdOnce != null
                    && rows.size() > 1
                    && rows.stream()
                            .anyMatch(row -> row.get("id").getAsLong() == failUpsertIdOnce)) {
                throw new RuntimeException("received message larger than max");
            }
            calls.add(
                    "upsert "
                            + databaseName
                            + " "
                            + collectionName
                            + formatPartition(partitionName)
                            + " "
                            + rows.stream()
                                    .map(row -> row.get("id").getAsLong())
                                    .collect(Collectors.toList()));
        }

        @Override
        public void delete(String databaseName, String collectionName, List<Object> ids) {
            delete(databaseName, collectionName, null, ids);
        }

        @Override
        public void delete(
                String databaseName,
                String collectionName,
                String partitionName,
                List<Object> ids) {
            if (remainingDeleteFailures > 0) {
                remainingDeleteFailures--;
                throw new RuntimeException("connection reset by peer");
            }
            calls.add(
                    "delete "
                            + databaseName
                            + " "
                            + collectionName
                            + formatPartition(partitionName)
                            + " "
                            + ids);
        }

        @Override
        public boolean hasPartition(
                String databaseName, String collectionName, String partitionName) {
            hasPartitionInvocations++;
            return partitions.contains(collectionName + "/" + partitionName);
        }

        @Override
        public void createPartition(
                String databaseName, String collectionName, String partitionName) {
            if (failCreatePartitionAlreadyExistsOnce) {
                failCreatePartitionAlreadyExistsOnce = false;
                partitions.add(collectionName + "/" + partitionName);
                throw new RuntimeException("partition already exists");
            }
            partitions.add(collectionName + "/" + partitionName);
            calls.add(
                    "createPartition " + databaseName + " " + collectionName + " " + partitionName);
        }

        private String formatPartition(String partitionName) {
            return partitionName == null ? "" : " partition=" + partitionName;
        }

        @Override
        public void reconnect() {
            reconnectCount++;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class TestingMilvusSinkMetrics extends MilvusSinkMetrics {

        private final TestingCounter successfulFlushRows = new TestingCounter();
        private final TestingCounter flushFailures = new TestingCounter();
        private final TestingCounter retries = new TestingCounter();
        private final TestingCounter recordsOutErrors = new TestingCounter();
        private long pendingRows;

        private TestingMilvusSinkMetrics() {
            super(null);
        }

        @Override
        void setPendingRows(long pendingRows) {
            this.pendingRows = pendingRows;
        }

        @Override
        void recordSuccessfulFlush(long rows, long durationMillis) {
            successfulFlushRows.inc(rows);
            setPendingRows(0);
        }

        @Override
        void recordFlushFailure() {
            flushFailures.inc();
        }

        @Override
        void recordRecordsOutErrors(long rows) {
            recordsOutErrors.inc(rows);
        }

        @Override
        void recordRetry() {
            retries.inc();
        }
    }

    private static class TestingCounter implements Counter {

        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
