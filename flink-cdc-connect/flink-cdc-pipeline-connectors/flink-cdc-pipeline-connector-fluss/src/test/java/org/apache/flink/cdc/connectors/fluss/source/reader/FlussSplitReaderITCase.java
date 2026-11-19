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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.event.TableSubscriptionEvent;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for table and partition cleanup with real Fluss SDK scanners. */
class FlussSplitReaderITCase {

    private static final String DATABASE = "split_reader_it_db";
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(clusterConfig())
                    .setNumOfTabletServers(1)
                    .build();

    private Connection connection;
    private WrapperFlussMetricRegistry metricRegistry;
    private SourceReaderMetricGroup metricGroup;

    @BeforeEach
    void before() throws Exception {
        metricGroup = InternalSourceReaderMetricGroup.mock(new MetricListener().getMetricGroup());
        metricRegistry = new WrapperFlussMetricRegistry(metricGroup, Collections.emptySet());
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        connection.getAdmin().createDatabase(DATABASE, DatabaseDescriptor.EMPTY, false).get();
    }

    @AfterEach
    void after() throws Exception {
        try {
            if (connection != null) {
                try {
                    connection.getAdmin().dropDatabase(DATABASE, true, true).get();
                } finally {
                    connection.close();
                }
            }
        } finally {
            if (metricRegistry != null) {
                metricRegistry.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    @Timeout(60)
    void testRemovingLogPartitionUnsubscribesSdkAndKeepsOtherPartitionReadable() throws Exception {
        TablePath tablePath = createPartitionedTable("log_partition_removal", false);
        FlussLogSplit removedSplit =
                new FlussLogSplit(
                        PhysicalTablePath.of(tablePath, "removed"),
                        tableBucket(tablePath, "removed"),
                        0L);
        FlussLogSplit retainedSplit =
                new FlussLogSplit(
                        PhysicalTablePath.of(tablePath, "retained"),
                        tableBucket(tablePath, "retained"),
                        0L);

        try (Table table = connection.getTable(tablePath);
                FlussSplitReader reader = newReader()) {
            AppendWriter writer = table.newAppend().createWriter();
            append(writer, 1, "removed", "removed-before");
            append(writer, 2, "retained", "retained-before");
            reader.handleSplitsChanges(
                    new SplitsAddition<>(Arrays.asList(removedSplit, retainedSplit)));
            List<List<Object>> unsubscribed = recordUnsubscriptions(reader);
            Object sharedTable = readerTable(reader, tablePath);
            assertThat(awaitPayloads(reader, "removed-before", "retained-before"))
                    .containsExactlyInAnyOrder("removed-before", "retained-before");

            reader.removePartitions(Collections.singleton(removedSplit.getPhysicalTablePath()));

            assertFinishedSplit(reader, removedSplit);
            assertThat(unsubscribed)
                    .containsExactly(
                            Arrays.asList(
                                    tablePath, removedSplit.getTableBucket().getPartitionId(), 0));
            assertThat(readerTable(reader, tablePath)).isSameAs(sharedTable);

            // Verify the retained partition remains readable after SDK unsubscription.
            append(writer, 3, "removed", "removed-after");
            append(writer, 4, "retained", "retained-after");
            assertThat(awaitPayloads(reader, "retained-after")).containsExactly("retained-after");
        }
    }

    @Test
    @Timeout(60)
    void testRemovingActiveSnapshotPartitionKeepsQueuedSnapshotAndLogTailReadable()
            throws Exception {
        TablePath tablePath = createPartitionedTable("snapshot_partition_removal", true);
        try (Table table = connection.getTable(tablePath);
                FlussSplitReader reader = newReader()) {
            UpsertWriter writer = table.newUpsert().createWriter();
            upsert(writer, 1, "removed", "removed-snapshot");
            upsert(writer, 2, "retained", "retained-snapshot-1");
            upsert(writer, 3, "retained", "retained-snapshot-2");
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
            FlussHybridSnapshotLogSplit removedSplit = snapshotSplit(tablePath, "removed");
            FlussHybridSnapshotLogSplit retainedSplit = snapshotSplit(tablePath, "retained");
            reader.handleSplitsChanges(
                    new SplitsAddition<>(Arrays.asList(removedSplit, retainedSplit)));
            List<List<Object>> unsubscribed = recordUnsubscriptions(reader);
            Object sharedTable = readerTable(reader, tablePath);

            assertThat(awaitPayloads(reader, "removed-snapshot"))
                    .containsExactly("removed-snapshot");
            assertThat(getField(reader, "currentBoundedSplit")).isSameAs(removedSplit);
            AtomicBoolean snapshotClosed = recordBatchScannerClose(reader);

            reader.removePartitions(Collections.singleton(removedSplit.getPhysicalTablePath()));

            assertThat(snapshotClosed).isTrue();
            assertFinishedSplit(reader, removedSplit);
            assertThat(unsubscribed)
                    .containsExactly(
                            Arrays.asList(
                                    tablePath, removedSplit.getTableBucket().getPartitionId(), 0));
            assertThat(readerTable(reader, tablePath)).isSameAs(sharedTable);
            assertThat(awaitPayloads(reader, "retained-snapshot-1", "retained-snapshot-2"))
                    .containsExactlyInAnyOrder("retained-snapshot-1", "retained-snapshot-2");

            upsert(writer, 4, "removed", "removed-tail");
            upsert(writer, 5, "retained", "retained-tail");
            assertThat(awaitPayloads(reader, "retained-tail")).containsExactly("retained-tail");
        }
    }

    @Test
    @Timeout(60)
    void testRemovingTableWithoutLocalSplitsAllowsSameNameRecreation() throws Exception {
        TablePath removedTablePath = createPartitionedTable("recreated_table", false);
        TablePath retainedTablePath = createPartitionedTable("retained_table", false);
        FlussLogSplit removedSplit =
                new FlussLogSplit(
                        PhysicalTablePath.of(removedTablePath, "removed"),
                        tableBucket(removedTablePath, "removed"),
                        0L);
        FlussLogSplit retainedSplit =
                new FlussLogSplit(
                        PhysicalTablePath.of(retainedTablePath, "retained"),
                        tableBucket(retainedTablePath, "retained"),
                        0L);

        try (Table removedTable = connection.getTable(removedTablePath);
                Table retainedTable = connection.getTable(retainedTablePath);
                FlussSplitReader reader = newReader()) {
            AppendWriter retainedWriter = retainedTable.newAppend().createWriter();
            append(removedTable.newAppend().createWriter(), 1, "removed", "removed-before");
            append(retainedWriter, 2, "retained", "retained-before");
            reader.handleSplitsChanges(
                    new SplitsAddition<>(Arrays.asList(removedSplit, retainedSplit)));
            assertThat(awaitPayloads(reader, "removed-before", "retained-before"))
                    .containsExactlyInAnyOrder("removed-before", "retained-before");
            Object cachedRemovedTable = readerTable(reader, removedTablePath);
            Object cachedRetainedTable = readerTable(reader, retainedTablePath);

            reader.removePartitions(Collections.singleton(removedSplit.getPhysicalTablePath()));
            assertFinishedSplit(reader, removedSplit);
            assertThat(readerTable(reader, removedTablePath)).isSameAs(cachedRemovedTable);
            connection.getAdmin().dropTable(removedTablePath, false).get();

            reader.removeTables(Collections.singleton(removedTablePath));

            assertThat(readerTable(reader, removedTablePath)).isNull();
            assertThat(readerTable(reader, retainedTablePath)).isSameAs(cachedRetainedTable);
            createPartitionedTable(removedTablePath.getTableName(), false);
            FlussLogSplit recreatedSplit =
                    new FlussLogSplit(
                            removedSplit.getPhysicalTablePath(),
                            tableBucket(removedTablePath, "removed"),
                            0L);
            assertThat(recreatedSplit.getTableBucket().getTableId())
                    .isNotEqualTo(removedSplit.getTableBucket().getTableId());
            reader.handleSplitsChanges(
                    new SplitsAddition<>(Collections.singletonList(recreatedSplit)));

            try (Table recreatedTable = connection.getTable(removedTablePath)) {
                append(recreatedTable.newAppend().createWriter(), 3, "removed", "recreated-after");
                append(retainedWriter, 4, "retained", "retained-after");
                assertThat(awaitPayloads(reader, "recreated-after", "retained-after"))
                        .containsExactlyInAnyOrder("recreated-after", "retained-after");
            }
        }
    }

    @ParameterizedTest(name = "allRemoved={0}, restoreBeforeFinished={1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    @Timeout(60)
    void testPartitionDroppedAfterReaderCheckBeforeSubscribe(
            boolean allRemoved, boolean restoreBeforeFinished) throws Exception {
        TablePath tablePath = createPartitionedTable("subscribe_race", false);
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            append(writer, 1, "removed", "expired-payload");
            append(writer, 2, "retained", "live-payload");
        }
        FlussSplitBase expired =
                new FlussLogSplit(
                        PhysicalTablePath.of(tablePath, "removed"),
                        tableBucket(tablePath, "removed"),
                        0L);
        FlussSplitBase retained =
                new FlussLogSplit(
                        PhysicalTablePath.of(tablePath, "retained"),
                        tableBucket(tablePath, "retained"),
                        0L);
        TableSubscriptionEvent subscription =
                new TableSubscriptionEvent(
                        Collections.singleton(tablePath),
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        new HashSet<>(
                                Arrays.asList(
                                        expired.getPhysicalTablePath(),
                                        retained.getPhysicalTablePath())));
        CountDownLatch assignmentReachedFetcher = new CountDownLatch(1);
        CountDownLatch allowSubscribe = new CountDownLatch(1);
        FlussSplitReader splitReader =
                new FlussSplitReader(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        metricRegistry,
                        new FlussSourceReaderMetrics(metricGroup)) {
                    @Override
                    public void handleSplitsChanges(SplitsChange<FlussSplitBase> changes) {
                        assignmentReachedFetcher.countDown();
                        try {
                            if (!allowSubscribe.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to subscribe");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        super.handleSplitsChanges(changes);
                    }
                };
        List<FlussSplitBase> beforeFinished;
        List<FlussSplitBase> afterFinished;
        List<String> expectedPayloads =
                allRemoved ? Collections.emptyList() : Collections.singletonList("live-payload");
        int remainingSplits = allRemoved ? 0 : 1;
        try (FlussSourceReader<String> reader = sourceReader(splitReader)) {
            reader.handleSourceEvents(subscription);
            reader.addSplits(Arrays.asList(expired, retained));
            assertThat(assignmentReachedFetcher.await(10, TimeUnit.SECONDS)).isTrue();
            beforeFinished = reader.snapshotState(1L);
            assertThat(beforeFinished).containsExactlyInAnyOrder(expired, retained);

            for (String partition :
                    allRemoved
                            ? Arrays.asList("removed", "retained")
                            : Collections.singletonList("removed")) {
                connection
                        .getAdmin()
                        .dropPartition(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("part", partition)),
                                false)
                        .get();
            }
            allowSubscribe.countDown();

            CollectingReaderOutput output = new CollectingReaderOutput();
            afterFinished = awaitCheckpoint(reader, output, remainingSplits, expectedPayloads);
            assertThat(output.payloads).containsExactlyElementsOf(expectedPayloads);
            assertThat(afterFinished)
                    .extracting(FlussSplitBase::getPhysicalTablePath)
                    .doesNotContain(expired.getPhysicalTablePath());
        } finally {
            allowSubscribe.countDown();
        }

        List<FlussSplitBase> checkpoint = restoreBeforeFinished ? beforeFinished : afterFinished;
        try (FlussSourceReader<String> restored = sourceReader(newReader())) {
            restored.handleSourceEvents(subscription);
            restored.addSplits(checkpoint);
            if (!allRemoved) {
                try (Table table = connection.getTable(tablePath)) {
                    append(table.newAppend().createWriter(), 3, "retained", "after-restore");
                }
                expectedPayloads =
                        restoreBeforeFinished
                                ? Arrays.asList("live-payload", "after-restore")
                                : Collections.singletonList("after-restore");
            }
            CollectingReaderOutput output = new CollectingReaderOutput();
            List<FlussSplitBase> restoredCheckpoint =
                    awaitCheckpoint(restored, output, remainingSplits, expectedPayloads);
            assertThat(output.payloads).containsExactlyElementsOf(expectedPayloads);
            assertThat(restoredCheckpoint)
                    .extracting(FlussSplitBase::getPhysicalTablePath)
                    .doesNotContain(expired.getPhysicalTablePath());
        }
    }

    private FlussSourceReader<String> sourceReader(FlussSplitReader splitReader) {
        SourceReaderContext context =
                (SourceReaderContext)
                        Proxy.newProxyInstance(
                                SourceReaderContext.class.getClassLoader(),
                                new Class<?>[] {SourceReaderContext.class},
                                (proxy, method, arguments) -> {
                                    switch (method.getName()) {
                                        case "metricGroup":
                                            return metricGroup;
                                        case "getConfiguration":
                                            return new org.apache.flink.configuration
                                                    .Configuration();
                                        case "getIndexOfSubtask":
                                            return 0;
                                        case "currentParallelism":
                                            return 1;
                                        case "getLocalHostName":
                                            return "localhost";
                                        case "sendSourceEventToCoordinator":
                                        case "sendSplitRequest":
                                            return null;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        return new FlussSourceReader<>(
                context,
                new WrapperFlussMetricRegistry(metricGroup, Collections.emptySet()),
                new FlussRecordEmitter<>(
                        (record, tablePath) ->
                                Collections.singletonList(
                                        record.getScanRecord().getRow().getString(2).toString())),
                new FlussSourceFetcherManager(
                        new FutureCompletingBlockingQueue<>(), () -> splitReader),
                null);
    }

    private static List<FlussSplitBase> awaitCheckpoint(
            FlussSourceReader<String> reader,
            CollectingReaderOutput output,
            int remainingSplits,
            List<String> expectedPayloads)
            throws Exception {
        long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        List<FlussSplitBase> checkpoint = Collections.emptyList();
        while (System.nanoTime() < deadline) {
            reader.pollNext(output);
            checkpoint = reader.snapshotState(2L);
            if (checkpoint.size() == remainingSplits
                    && output.payloads.containsAll(expectedPayloads)) {
                return checkpoint;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError(
                "Timed out waiting for split cleanup and payloads "
                        + expectedPayloads
                        + "; received "
                        + output.payloads
                        + "; checkpoint "
                        + checkpoint);
    }

    private static class CollectingReaderOutput implements ReaderOutput<String> {

        private final List<String> payloads = new ArrayList<>();

        @Override
        public void collect(String record) {
            payloads.add(record);
        }

        @Override
        public void collect(String record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<String> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }

    private FlussSplitReader newReader() {
        return new FlussSplitReader(
                FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                metricRegistry,
                new FlussSourceReaderMetrics(metricGroup));
    }

    private TablePath createPartitionedTable(String name, boolean primaryKey) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("part", DataTypes.STRING())
                        .column("payload", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "part");
        }
        TablePath tablePath = TablePath.of(DATABASE, name);
        connection
                .getAdmin()
                .createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .partitionedBy("part")
                                .distributedBy(1, "id")
                                .build(),
                        false)
                .get();
        for (String partition : Arrays.asList("removed", "retained")) {
            connection
                    .getAdmin()
                    .createPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("part", partition)),
                            false)
                    .get();
        }
        return tablePath;
    }

    private TableBucket tableBucket(TablePath tablePath, String partition) throws Exception {
        long tableId = connection.getAdmin().getTableInfo(tablePath).get().getTableId();
        long partitionId =
                connection.getAdmin().listPartitionInfos(tablePath).get().stream()
                        .filter(info -> info.getPartitionName().equals(partition))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing partition " + partition))
                        .getPartitionId();
        return new TableBucket(tableId, partitionId, 0);
    }

    private FlussHybridSnapshotLogSplit snapshotSplit(TablePath tablePath, String partition)
            throws Exception {
        KvSnapshots snapshots =
                connection.getAdmin().getLatestKvSnapshots(tablePath, partition).get();
        return new FlussHybridSnapshotLogSplit(
                PhysicalTablePath.of(tablePath, partition),
                tableBucket(tablePath, partition),
                snapshots.getSnapshotId(0).orElseThrow(AssertionError::new),
                snapshots.getLogOffset(0).orElseThrow(AssertionError::new));
    }

    private static void append(AppendWriter writer, int id, String partition, String payload)
            throws Exception {
        CompletableFuture<?> result = writer.append(row(id, partition, payload));
        writer.flush();
        result.get(10, TimeUnit.SECONDS);
    }

    private static void upsert(UpsertWriter writer, int id, String partition, String payload)
            throws Exception {
        CompletableFuture<?> result = writer.upsert(row(id, partition, payload));
        writer.flush();
        result.get(10, TimeUnit.SECONDS);
    }

    private static GenericRow row(int id, String partition, String payload) {
        return GenericRow.of(
                id, BinaryString.fromString(partition), BinaryString.fromString(payload));
    }

    private static List<String> awaitPayloads(FlussSplitReader reader, String... expected)
            throws Exception {
        List<String> payloads = new ArrayList<>();
        long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        while (!payloads.containsAll(Arrays.asList(expected)) && System.nanoTime() < deadline) {
            payloads.addAll(readPayloads(reader.fetch()));
        }
        assertThat(payloads).contains(expected);
        return payloads;
    }

    private static List<String> readPayloads(RecordsWithSplitIds<FlussSourceRecord> records) {
        List<String> payloads = new ArrayList<>();
        try {
            while (records.nextSplit() != null) {
                FlussSourceRecord record;
                while ((record = records.nextRecordFromSplit()) != null) {
                    payloads.add(record.getScanRecord().getRow().getString(2).toString());
                }
            }
        } finally {
            records.recycle();
        }
        return payloads;
    }

    private static void assertFinishedSplit(FlussSplitReader reader, FlussSplitBase split)
            throws Exception {
        RecordsWithSplitIds<FlussSourceRecord> records = reader.fetch();
        try {
            assertThat(records.finishedSplits()).containsExactly(split.splitId());
            assertThat(records.nextSplit()).isNull();
        } finally {
            records.recycle();
        }
    }

    private static List<List<Object>> recordUnsubscriptions(FlussSplitReader reader)
            throws Exception {
        MultiTableLogScanner scanner = (MultiTableLogScanner) getField(reader, "currentLogScanner");
        List<List<Object>> unsubscribed = new ArrayList<>();
        setField(
                reader,
                "currentLogScanner",
                Proxy.newProxyInstance(
                        MultiTableLogScanner.class.getClassLoader(),
                        new Class<?>[] {MultiTableLogScanner.class},
                        (proxy, method, arguments) -> {
                            Object result = invoke(scanner, method, arguments);
                            if (method.getName().equals("unsubscribe")) {
                                unsubscribed.add(Arrays.asList(arguments.clone()));
                            }
                            return result;
                        }));
        return unsubscribed;
    }

    private static AtomicBoolean recordBatchScannerClose(FlussSplitReader reader) throws Exception {
        BatchScanner scanner = (BatchScanner) getField(reader, "currentBatchScanner");
        assertThat(scanner).isNotNull();
        AtomicBoolean closed = new AtomicBoolean();
        setField(
                reader,
                "currentBatchScanner",
                Proxy.newProxyInstance(
                        BatchScanner.class.getClassLoader(),
                        new Class<?>[] {BatchScanner.class},
                        (proxy, method, arguments) -> {
                            Object result = invoke(scanner, method, arguments);
                            if (method.getName().equals("close")) {
                                closed.set(true);
                            }
                            return result;
                        }));
        return closed;
    }

    private static Object invoke(Object delegate, Method method, Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static Object readerTable(FlussSplitReader reader, TablePath tablePath)
            throws Exception {
        return ((Map<?, ?>) getField(reader, "tables")).get(tablePath);
    }

    private static Object getField(FlussSplitReader reader, String name) throws Exception {
        Field field = FlussSplitReader.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(reader);
    }

    private static void setField(FlussSplitReader reader, String name, Object value)
            throws Exception {
        Field field = FlussSplitReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(reader, value);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        return config;
    }
}
