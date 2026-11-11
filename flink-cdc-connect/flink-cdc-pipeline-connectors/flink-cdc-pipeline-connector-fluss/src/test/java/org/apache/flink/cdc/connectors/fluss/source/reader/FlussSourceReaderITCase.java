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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.event.TableSubscriptionEvent;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitSerializer;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableRecords;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Tests checkpoint recovery across empty log batches using a real Fluss cluster. */
class FlussSourceReaderITCase {

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testCheckpointAndRestoreAfterEmptyLogBatch() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "empty_log_progress");
        try (Connection connection =
                ConnectionFactory.createConnection(CLUSTER.getClientConfig())) {
            connection
                    .getAdmin()
                    .createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .build())
                                    .distributedBy(1)
                                    .logFormat(LogFormat.INDEXED)
                                    .property(TABLE_REPLICATION_FACTOR, 1)
                                    .build(),
                            false)
                    .get();
            try (Table table = connection.getTable(tablePath)) {
                long tableId = table.getTableInfo().getTableId();
                TableBucket bucket = new TableBucket(tableId, 0);
                CLUSTER.waitUntilTableReady(tableId);
                table.newAppend().createWriter().append(GenericRow.of(7)).get();

                FlussSourceReaderTest.TestingReaderContext context =
                        new FlussSourceReaderTest.TestingReaderContext();
                WrapperFlussMetricRegistry metrics =
                        new WrapperFlussMetricRegistry(
                                context.metricGroup(), Collections.emptySet());
                FlussSourceFetcherManager manager =
                        new FlussSourceFetcherManager(
                                new FutureCompletingBlockingQueue<>(), () -> null) {
                            @Override
                            public void addSplits(List<FlussSplitBase> splits) {}
                        };
                FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                        manager.getQueue();
                FlussRecordEmitterTest.SimpleReaderOutput<Event> output =
                        new FlussRecordEmitterTest.SimpleReaderOutput<>();
                FlussLogSplit split =
                        new FlussLogSplit(PhysicalTablePath.of(tablePath), bucket, 0L);
                try (FlussSplitReader splitReader =
                                new FlussSplitReader(
                                        CLUSTER.getClientConfig(),
                                        metrics,
                                        new FlussSourceReaderMetrics(context.metricGroup()));
                        FlussSourceReader<Event> reader =
                                new FlussSourceReader<>(
                                        queue,
                                        context,
                                        metrics,
                                        new FlussRecordEmitter<>(
                                                new FlussRecordDeserializer(),
                                                context.metricGroup()
                                                        .getIOMetricGroup()
                                                        .getNumRecordsInCounter()),
                                        manager)) {
                    reader.handleSourceEvents(
                            new TableSubscriptionEvent(
                                    Collections.singleton(tablePath), Collections.emptyMap()));
                    reader.addSplits(Collections.singletonList(split));
                    splitReader.handleSplitsChanges(
                            new SplitsAddition<>(Collections.singletonList(split)));
                    Field scannerField =
                            FlussSplitReader.class.getDeclaredField("currentLogScanner");
                    scannerField.setAccessible(true);
                    MultiTableLogScanner scanner =
                            (MultiTableLogScanner) scannerField.get(splitReader);
                    AtomicLong consumedOffset = new AtomicLong(-1);
                    MultiTableLogScanner observingScanner =
                            (MultiTableLogScanner)
                                    Proxy.newProxyInstance(
                                            getClass().getClassLoader(),
                                            new Class<?>[] {MultiTableLogScanner.class},
                                            (proxy, method, arguments) -> {
                                                Object result;
                                                try {
                                                    result = method.invoke(scanner, arguments);
                                                } catch (InvocationTargetException e) {
                                                    throw e.getCause();
                                                }
                                                if (result instanceof MultiTableRecords) {
                                                    Long offset =
                                                            ((MultiTableRecords) result)
                                                                    .consumedUpToOffset(
                                                                            tablePath, bucket);
                                                    if (offset != null) {
                                                        consumedOffset.set(offset);
                                                    }
                                                }
                                                return result;
                                            });
                    scannerField.set(splitReader, observingScanner);
                    await().atMost(Duration.ofSeconds(30))
                            .untilAsserted(
                                    () -> {
                                        queue.put(0, splitReader.fetch());
                                        reader.pollNext(output);
                                        assertThat(offset(reader)).isEqualTo(1L);
                                    });
                    assertThat(output.getCollectedEvents()).hasSize(2);
                    assertThat(
                                    context.metricGroup()
                                            .getIOMetricGroup()
                                            .getNumRecordsInCounter()
                                            .getCount())
                            .isEqualTo(1L);
                    appendEmptyBatch(table, bucket);
                    await().atMost(Duration.ofSeconds(30))
                            .untilAsserted(
                                    () -> {
                                        RecordsWithSplitIds<FlussSourceRecord> fetched =
                                                splitReader.fetch();
                                        assertThat(consumedOffset).hasValue(2L);
                                        queue.put(0, fetched);
                                    });
                    // Fetching alone must not checkpoint progress that is still queued.
                    assertThat(offset(reader)).isEqualTo(1L);
                    reader.pollNext(output);
                    FlussSplitBase saved = reader.snapshotState(2L).get(0);
                    assertThat(saved.asLogSplit().getStartingOffset()).isEqualTo(2L);
                    assertThat(output.getCollectedEvents()).hasSize(2);
                    assertThat(
                                    context.metricGroup()
                                            .getIOMetricGroup()
                                            .getNumRecordsInCounter()
                                            .getCount())
                            .isEqualTo(1L);
                    FlussSplitSerializer serializer = new FlussSplitSerializer();
                    FlussSplitBase restored =
                            serializer.deserialize(
                                    serializer.getVersion(), serializer.serialize(saved));
                    assertThat(restored.asLogSplit().getStartingOffset()).isEqualTo(2L);
                    try (FlussSplitReader recovered =
                            new FlussSplitReader(
                                    CLUSTER.getClientConfig(),
                                    metrics,
                                    new FlussSourceReaderMetrics(context.metricGroup()))) {
                        recovered.handleSplitsChanges(
                                new SplitsAddition<>(Collections.singletonList(restored)));
                        table.newAppend().createWriter().append(GenericRow.of(8)).get();
                        await().atMost(Duration.ofSeconds(30))
                                .untilAsserted(
                                        () -> {
                                            RecordsWithSplitIds<FlussSourceRecord> fetched =
                                                    recovered.fetch();
                                            assertThat(fetched.nextSplit())
                                                    .isEqualTo(restored.splitId());
                                            FlussSourceRecord first = fetched.nextRecordFromSplit();
                                            assertThat(first.isProgressOnly()).isFalse();
                                            assertThat(first.getScanRecord().logOffset())
                                                    .isEqualTo(2L);
                                            assertThat(first.getScanRecord().getRow().getInt(0))
                                                    .isEqualTo(8);
                                            assertThat(fetched.nextRecordFromSplit()).isNull();
                                        });
                    }
                }
            }
        }
    }

    private static long offset(FlussSourceReader<Event> reader) {
        return reader.snapshotState(1L).get(0).asLogSplit().getStartingOffset();
    }

    private static void appendEmptyBatch(Table table, TableBucket bucket) throws Exception {
        BytesView bytes =
                MemoryLogRecordsIndexedBuilder.builder(
                                table.getTableInfo().getSchemaId(),
                                1024,
                                new UnmanagedPagedOutputView(1024),
                                true)
                        .build();
        byte[] raw = new byte[bytes.getBytesLength()];
        bytes.getByteBuf().getBytes(0, raw);
        MemoryLogRecords.pointToBytes(raw).batches().iterator().next().ensureValid();
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(bucket.getTableId())
                        .setAcks(-1)
                        .setTimeoutMs(10000);
        request.addAllBucketsReqs(
                Collections.singletonList(
                        new PbProduceLogReqForBucket()
                                .setBucketId(bucket.getBucket())
                                .setRecordsBytesView(bytes)));
        ProduceLogResponse response =
                CLUSTER.newTabletServerClientForNode(CLUSTER.waitAndGetLeader(bucket))
                        .produceLog(request)
                        .get();
        assertThat(response.getBucketsRespsList().get(0).hasErrorCode()).isFalse();
    }
}
