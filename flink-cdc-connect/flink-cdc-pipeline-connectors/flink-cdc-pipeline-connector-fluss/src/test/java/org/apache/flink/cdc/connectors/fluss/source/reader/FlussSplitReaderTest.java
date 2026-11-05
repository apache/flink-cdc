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

import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSplitReader}. */
class FlussSplitReaderTest {

    private static final long TABLE_ID = 1001L;
    private static final long OTHER_TABLE_ID = 2002L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH = PhysicalTablePath.of(TABLE_PATH);
    private static final TableBucket TABLE_BUCKET = new TableBucket(TABLE_ID, 0);

    @Test
    void testValidateLogSplitTableId() {
        assertTableIdValidation(new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 100L));
    }

    @Test
    void testValidateHybridSnapshotLogSplitTableId() {
        assertTableIdValidation(
                new FlussHybridSnapshotLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 10L, 100L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveTablesFinishesLogAndSnapshotSplitsAndClearsTableResources(boolean closeFails)
            throws Exception {
        TableBucket logBucket = new TableBucket(TABLE_ID, 0);
        TableBucket currentSnapshotBucket = new TableBucket(TABLE_ID, 1);
        TableBucket queuedSnapshotBucket = new TableBucket(TABLE_ID, 2);
        FlussLogSplit logSplit = new FlussLogSplit(PHYSICAL_TABLE_PATH, logBucket, 10L);
        FlussHybridSnapshotLogSplit currentSnapshot =
                new FlussHybridSnapshotLogSplit(
                        PHYSICAL_TABLE_PATH, currentSnapshotBucket, 1L, 10L);
        FlussHybridSnapshotLogSplit queuedSnapshot =
                new FlussHybridSnapshotLogSplit(PHYSICAL_TABLE_PATH, queuedSnapshotBucket, 2L, 10L);
        FlussSplitReader reader = new FlussSplitReader(new Configuration(), null, null);
        AtomicBoolean tableClosed = new AtomicBoolean();
        AtomicBoolean batchScannerClosed = new AtomicBoolean();
        AtomicBoolean logScannerWokenUp = new AtomicBoolean();
        Set<Integer> unsubscribedBuckets = new HashSet<>();
        Table table =
                proxy(
                        Table.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("close")) {
                                tableClosed.set(true);
                                if (closeFails) {
                                    throw new IOException("Test table close failure");
                                }
                            }
                            return null;
                        });
        MultiTableLogScanner logScanner =
                proxy(
                        MultiTableLogScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("unsubscribe")) {
                                unsubscribedBuckets.add((Integer) arguments[1]);
                            } else if (method.getName().equals("wakeup")) {
                                logScannerWokenUp.set(true);
                            }
                            return null;
                        });
        BatchScanner batchScanner =
                proxy(
                        BatchScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("close")) {
                                batchScannerClosed.set(true);
                            }
                            return null;
                        });

        tableResources(reader).put(TABLE_PATH, table);
        TablePath otherTablePath = TablePath.of("test_db", "other_table");
        AtomicBoolean otherTableClosed = new AtomicBoolean();
        tableResources(reader)
                .put(
                        otherTablePath,
                        proxy(
                                Table.class,
                                (proxy, method, arguments) -> {
                                    if (method.getName().equals("close")) {
                                        otherTableClosed.set(true);
                                    }
                                    return null;
                                }));
        tableRowTypes(reader).put(TABLE_PATH, new RowType(Collections.emptyList()));
        tablePrimaryKeyNames(reader).put(TABLE_PATH, Collections.singletonList("id"));
        tablePartitionKeyNames(reader).put(TABLE_PATH, Collections.singletonList("part"));
        bucketToSplit(reader).put(logBucket, logSplit);
        bucketToSplit(reader).put(currentSnapshotBucket, currentSnapshot);
        bucketToSplit(reader).put(queuedSnapshotBucket, queuedSnapshot);
        boundedSplits(reader).add(queuedSnapshot);
        setField(reader, "currentBoundedSplit", currentSnapshot);
        setField(reader, "currentBatchScanner", batchScanner);
        setField(reader, "currentLogScanner", logScanner);

        Set<TablePath> removedTables = new LinkedHashSet<>();
        removedTables.add(TABLE_PATH);
        removedTables.add(otherTablePath);
        reader.removeTables(removedTables);

        RecordsWithSplitIds<FlussSourceRecord> records = reader.fetch();
        assertThat(records.finishedSplits())
                .containsExactlyInAnyOrder(
                        logSplit.splitId(), currentSnapshot.splitId(), queuedSnapshot.splitId());
        assertThat(bucketToSplit(reader)).isEmpty();
        assertThat(boundedSplits(reader)).isEmpty();
        assertThat(tableResources(reader)).isEmpty();
        assertThat(tableRowTypes(reader)).isEmpty();
        assertThat(tablePrimaryKeyNames(reader)).isEmpty();
        assertThat(tablePartitionKeyNames(reader)).isEmpty();
        assertThat(unsubscribedBuckets)
                .containsExactlyInAnyOrder(
                        logBucket.getBucket(),
                        currentSnapshotBucket.getBucket(),
                        queuedSnapshotBucket.getBucket());
        assertThat(logScannerWokenUp).isFalse();
        assertThatCode(reader::fetch).doesNotThrowAnyException();
        assertThat(batchScannerClosed).isTrue();
        assertThat(tableClosed).isTrue();
        assertThat(otherTableClosed).isTrue();
    }

    @Test
    void testUnsubscriptionFailurePropagates() throws Exception {
        FlussSplitReader reader = new FlussSplitReader(new Configuration(), null, null);
        bucketToSplit(reader)
                .put(TABLE_BUCKET, new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 10L));
        IllegalStateException failure = new IllegalStateException("Test unsubscribe failure");
        setField(
                reader,
                "currentLogScanner",
                proxy(
                        MultiTableLogScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("unsubscribe")) {
                                throw failure;
                            }
                            return null;
                        }));
        assertThatThrownBy(() -> reader.removeTables(Collections.singleton(TABLE_PATH)))
                .isSameAs(failure);
    }

    @Test
    void testRemovePartitionsFinishesExpiredSplitsAndPreservesOtherPartitions() throws Exception {
        PhysicalTablePath expiredPartition = PhysicalTablePath.of(TABLE_PATH, "expired");
        PhysicalTablePath retainedPartition = PhysicalTablePath.of(TABLE_PATH, "retained");
        TableBucket logBucket = new TableBucket(TABLE_ID, 11L, 0);
        TableBucket currentSnapshotBucket = new TableBucket(TABLE_ID, 11L, 1);
        TableBucket queuedSnapshotBucket = new TableBucket(TABLE_ID, 11L, 2);
        TableBucket retainedLogBucket = new TableBucket(TABLE_ID, 22L, 0);
        TableBucket retainedSnapshotBucket = new TableBucket(TABLE_ID, 22L, 1);
        FlussLogSplit logSplit = new FlussLogSplit(expiredPartition, logBucket, 10L);
        FlussHybridSnapshotLogSplit currentSnapshot =
                new FlussHybridSnapshotLogSplit(expiredPartition, currentSnapshotBucket, 1L, 10L);
        FlussHybridSnapshotLogSplit queuedSnapshot =
                new FlussHybridSnapshotLogSplit(expiredPartition, queuedSnapshotBucket, 2L, 10L);
        FlussLogSplit retainedLogSplit =
                new FlussLogSplit(retainedPartition, retainedLogBucket, 20L);
        FlussHybridSnapshotLogSplit retainedSnapshot =
                new FlussHybridSnapshotLogSplit(retainedPartition, retainedSnapshotBucket, 3L, 20L);
        FlussSplitReader reader = new FlussSplitReader(new Configuration(), null, null);
        AtomicBoolean tableClosed = new AtomicBoolean();
        AtomicBoolean batchScannerClosed = new AtomicBoolean();
        AtomicBoolean logScannerClosed = new AtomicBoolean();
        Set<TableBucket> unsubscribedBuckets = new HashSet<>();
        Table table =
                proxy(
                        Table.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("close")) {
                                tableClosed.set(true);
                            }
                            return null;
                        });
        MultiTableLogScanner logScanner =
                proxy(
                        MultiTableLogScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("unsubscribe")) {
                                assertThat(arguments).hasSize(3);
                                assertThat(arguments[0]).isEqualTo(TABLE_PATH);
                                unsubscribedBuckets.add(
                                        new TableBucket(
                                                TABLE_ID,
                                                (Long) arguments[1],
                                                (Integer) arguments[2]));
                            } else if (method.getName().equals("close")) {
                                logScannerClosed.set(true);
                            }
                            return null;
                        });
        BatchScanner batchScanner =
                proxy(
                        BatchScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("close")) {
                                batchScannerClosed.set(true);
                            }
                            return null;
                        });
        RowType rowType = new RowType(Collections.emptyList());
        tableResources(reader).put(TABLE_PATH, table);
        tableRowTypes(reader).put(TABLE_PATH, rowType);
        tablePrimaryKeyNames(reader).put(TABLE_PATH, Collections.singletonList("id"));
        tablePartitionKeyNames(reader).put(TABLE_PATH, Collections.singletonList("part"));
        bucketToSplit(reader).put(logBucket, logSplit);
        bucketToSplit(reader).put(currentSnapshotBucket, currentSnapshot);
        bucketToSplit(reader).put(queuedSnapshotBucket, queuedSnapshot);
        bucketToSplit(reader).put(retainedLogBucket, retainedLogSplit);
        bucketToSplit(reader).put(retainedSnapshotBucket, retainedSnapshot);
        boundedSplits(reader).add(queuedSnapshot);
        boundedSplits(reader).add(retainedSnapshot);
        setField(reader, "currentBoundedSplit", currentSnapshot);
        setField(reader, "currentBatchScanner", batchScanner);
        setField(reader, "currentBatchSchemaId", 1);
        setField(reader, "currentLogScanner", logScanner);

        reader.removePartitions(Collections.singleton(expiredPartition));

        assertThat(reader.fetch().finishedSplits())
                .containsExactlyInAnyOrder(
                        logSplit.splitId(), currentSnapshot.splitId(), queuedSnapshot.splitId());
        assertThat(unsubscribedBuckets)
                .containsExactlyInAnyOrder(logBucket, currentSnapshotBucket, queuedSnapshotBucket);
        assertThat(bucketToSplit(reader))
                .containsOnlyKeys(retainedLogBucket, retainedSnapshotBucket);
        assertThat(boundedSplits(reader)).containsExactly(retainedSnapshot);
        assertThat(getField(reader, "currentBoundedSplit")).isNull();
        assertThat(getField(reader, "currentBatchScanner")).isNull();
        assertThat(getField(reader, "currentBatchSchemaId")).isNull();
        assertThat(batchScannerClosed).isTrue();
        assertThat(tableResources(reader)).containsEntry(TABLE_PATH, table);
        assertThat(tableRowTypes(reader)).containsEntry(TABLE_PATH, rowType);
        assertThat(tablePrimaryKeyNames(reader))
                .containsEntry(TABLE_PATH, Collections.singletonList("id"));
        assertThat(tablePartitionKeyNames(reader))
                .containsEntry(TABLE_PATH, Collections.singletonList("part"));
        assertThat(getField(reader, "currentLogScanner")).isSameAs(logScanner);
        assertThat(tableClosed).isFalse();
        assertThat(logScannerClosed).isFalse();

        reader.removePartitions(Collections.singleton(expiredPartition));
        assertThat(bucketToSplit(reader))
                .containsOnlyKeys(retainedLogBucket, retainedSnapshotBucket);
        assertThat(boundedSplits(reader)).containsExactly(retainedSnapshot);
        assertThat(unsubscribedBuckets).hasSize(3);
    }

    @Test
    void testPartitionUnsubscriptionFailurePropagates() throws Exception {
        PhysicalTablePath partition = PhysicalTablePath.of(TABLE_PATH, "expired");
        TableBucket tableBucket = new TableBucket(TABLE_ID, 11L, 0);
        FlussSplitReader reader = new FlussSplitReader(new Configuration(), null, null);
        bucketToSplit(reader).put(tableBucket, new FlussLogSplit(partition, tableBucket, 10L));
        IllegalStateException failure = new IllegalStateException("Test unsubscribe failure");
        setField(
                reader,
                "currentLogScanner",
                proxy(
                        MultiTableLogScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("unsubscribe")) {
                                throw failure;
                            }
                            return null;
                        }));

        assertThatThrownBy(() -> reader.removePartitions(Collections.singleton(partition)))
                .isSameAs(failure);
    }

    @Test
    void testRemovingPartitionPreservesCurrentSnapshotOfAnotherPartition() throws Exception {
        PhysicalTablePath expiredPartition = PhysicalTablePath.of(TABLE_PATH, "expired");
        PhysicalTablePath retainedPartition = PhysicalTablePath.of(TABLE_PATH, "retained");
        TableBucket expiredBucket = new TableBucket(TABLE_ID, 11L, 0);
        TableBucket retainedBucket = new TableBucket(TABLE_ID, 22L, 0);
        FlussHybridSnapshotLogSplit expiredSnapshot =
                new FlussHybridSnapshotLogSplit(expiredPartition, expiredBucket, 1L, 10L);
        FlussHybridSnapshotLogSplit retainedSnapshot =
                new FlussHybridSnapshotLogSplit(retainedPartition, retainedBucket, 2L, 20L);
        FlussSplitReader reader = new FlussSplitReader(new Configuration(), null, null);
        AtomicBoolean batchScannerClosed = new AtomicBoolean();
        BatchScanner batchScanner =
                proxy(
                        BatchScanner.class,
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("close")) {
                                batchScannerClosed.set(true);
                            }
                            return null;
                        });
        bucketToSplit(reader).put(expiredBucket, expiredSnapshot);
        bucketToSplit(reader).put(retainedBucket, retainedSnapshot);
        boundedSplits(reader).add(expiredSnapshot);
        setField(reader, "currentBoundedSplit", retainedSnapshot);
        setField(reader, "currentBatchScanner", batchScanner);

        reader.removePartitions(Collections.singleton(expiredPartition));

        assertThat(reader.fetch().finishedSplits()).containsExactly(expiredSnapshot.splitId());
        assertThat(bucketToSplit(reader)).containsOnlyKeys(retainedBucket);
        assertThat(boundedSplits(reader)).isEmpty();
        assertThat(getField(reader, "currentBoundedSplit")).isSameAs(retainedSnapshot);
        assertThat(getField(reader, "currentBatchScanner")).isSameAs(batchScanner);
        assertThat(batchScannerClosed).isFalse();
    }

    @Test
    void testFetcherManagerFinishesLastAssignedSplitAfterTableRemoval() throws Exception {
        TestingSplitReader reader = new TestingSplitReader();
        FlussSourceFetcherManager manager =
                new FlussSourceFetcherManager(new FutureCompletingBlockingQueue<>(), () -> reader);
        FlussLogSplit lastSplit = new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 10L);
        try {
            manager.addSplits(Collections.singletonList(lastSplit));
            assertThat(reader.splitAdded.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.fetchStarted.await(10, TimeUnit.SECONDS)).isTrue();
            manager.removeTables(Collections.singleton(TABLE_PATH));
            assertThat(reader.removalRequested.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.removedTables).containsExactly(TABLE_PATH);
            assertThat(reader.wakeUps).hasValue(1);
            assertThat(awaitRecords(manager.getQueue(), lastSplit.splitId()).finishedSplits())
                    .containsExactly(lastSplit.splitId());
        } finally {
            manager.close(1000L);
        }
    }

    @Test
    void testFetcherManagerFinishesLastAssignedSplitAfterPartitionRemoval() throws Exception {
        TestingSplitReader reader = new TestingSplitReader();
        FlussSourceFetcherManager manager =
                new FlussSourceFetcherManager(new FutureCompletingBlockingQueue<>(), () -> reader);
        PhysicalTablePath partition = PhysicalTablePath.of(TABLE_PATH, "expired");
        FlussLogSplit lastSplit =
                new FlussLogSplit(partition, new TableBucket(TABLE_ID, 11L, 0), 10L);
        try {
            manager.addSplits(Collections.singletonList(lastSplit));
            assertThat(reader.splitAdded.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.fetchStarted.await(10, TimeUnit.SECONDS)).isTrue();

            manager.removePartitions(Collections.singleton(partition));

            assertThat(reader.removalRequested.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.removedPartitions).containsExactly(partition);
            assertThat(reader.removedTables).isEmpty();
            assertThat(reader.wakeUps).hasValue(1);
            assertThat(awaitRecords(manager.getQueue(), lastSplit.splitId()).finishedSplits())
                    .containsExactly(lastSplit.splitId());
        } finally {
            manager.close(1000L);
        }
    }

    private static void assertTableIdValidation(FlussSplitBase split) {
        assertThatCode(() -> FlussSplitReader.validateTableId(split, TABLE_ID))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> FlussSplitReader.validateTableId(split, OTHER_TABLE_ID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Table ID mismatch for split test_db.test_table.0: split table ID is 1001, but table test_db.test_table has ID 2002.");
    }

    @SuppressWarnings("unchecked")
    private static Map<TablePath, Table> tableResources(FlussSplitReader reader) throws Exception {
        return (Map<TablePath, Table>) getField(reader, "tables");
    }

    @SuppressWarnings("unchecked")
    private static Map<TablePath, RowType> tableRowTypes(FlussSplitReader reader) throws Exception {
        return (Map<TablePath, RowType>) getField(reader, "tableRowTypes");
    }

    @SuppressWarnings("unchecked")
    private static Map<TablePath, List<String>> tablePrimaryKeyNames(FlussSplitReader reader)
            throws Exception {
        return (Map<TablePath, List<String>>) getField(reader, "tablePrimaryKeyNames");
    }

    @SuppressWarnings("unchecked")
    private static Map<TablePath, List<String>> tablePartitionKeyNames(FlussSplitReader reader)
            throws Exception {
        return (Map<TablePath, List<String>>) getField(reader, "tablePartitionKeyNames");
    }

    @SuppressWarnings("unchecked")
    private static Map<TableBucket, FlussSplitBase> bucketToSplit(FlussSplitReader reader)
            throws Exception {
        return (Map<TableBucket, FlussSplitBase>) getField(reader, "bucketToSplit");
    }

    @SuppressWarnings("unchecked")
    private static Queue<FlussSplitBase> boundedSplits(FlussSplitReader reader) throws Exception {
        return (Queue<FlussSplitBase>) getField(reader, "boundedSplits");
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

    private static RecordsWithSplitIds<FlussSourceRecord> awaitRecords(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue,
            String expectedFinishedSplitId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        RecordsWithSplitIds<FlussSourceRecord> records;
        while (System.nanoTime() < deadline) {
            records = queue.poll();
            if (records != null && records.finishedSplits().contains(expectedFinishedSplitId)) {
                return records;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for finished split " + expectedFinishedSplitId);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static class TestingSplitReader extends FlussSplitReader {

        private final CountDownLatch splitAdded = new CountDownLatch(1);
        private final CountDownLatch fetchStarted = new CountDownLatch(1);
        private final CountDownLatch removalRequested = new CountDownLatch(1);
        private final CountDownLatch wakeUp = new CountDownLatch(1);
        private final AtomicInteger wakeUps = new AtomicInteger();
        private Set<TablePath> removedTables = Collections.emptySet();
        private Set<PhysicalTablePath> removedPartitions = Collections.emptySet();
        private String assignedSplitId;
        private String finishedSplitId;

        private TestingSplitReader() {
            super(new Configuration(), null, null);
        }

        @Override
        public RecordsWithSplitIds<FlussSourceRecord> fetch() {
            if (finishedSplitId != null) {
                RecordsBySplits.Builder<FlussSourceRecord> builder =
                        new RecordsBySplits.Builder<>();
                builder.addFinishedSplit(finishedSplitId);
                finishedSplitId = null;
                return builder.build();
            }
            fetchStarted.countDown();
            try {
                wakeUp.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new RecordsBySplits.Builder<FlussSourceRecord>().build();
        }

        @Override
        void removeTables(Set<TablePath> tablePaths) {
            removedTables = tablePaths;
            finishedSplitId = assignedSplitId;
            removalRequested.countDown();
        }

        @Override
        void removePartitions(Set<PhysicalTablePath> partitions) {
            removedPartitions = partitions;
            finishedSplitId = assignedSplitId;
            removalRequested.countDown();
        }

        @Override
        public void handleSplitsChanges(SplitsChange<FlussSplitBase> splitsChanges) {
            assignedSplitId = splitsChanges.splits().get(0).splitId();
            splitAdded.countDown();
        }

        @Override
        public void wakeUp() {
            wakeUps.incrementAndGet();
            wakeUp.countDown();
        }
    }
}
