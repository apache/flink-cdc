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

package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.handler.TiDBErrorHandler;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.relational.TableId;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Cdcpb;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests realtime and idempotent TiDB stream cleanup. */
class TiDBStreamLifecycleTest {

    @Test
    void shouldStopReaderContextWhenClosedRepeatedly() {
        EventSourceReader reader = createReader();
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        reader.context = context;

        reader.close();
        reader.close();

        assertThat(context.isRunning()).isFalse();
        assertThat(reader.isClosed()).isTrue();
    }

    @Test
    void shouldAllowClosingTaskBeforeItStarts() {
        TiDBStreamFetchTask task = new TiDBStreamFetchTask(createStreamSplit());

        task.close();
        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void shouldFailSourceWhenEmissionFails() {
        TiDBSourceConfig sourceConfig = createSourceConfig();
        ChangeEventQueue<Object> queue =
                new ChangeEventQueue.Builder<>()
                        .pollInterval(Duration.ofMillis(10))
                        .maxBatchSize(1)
                        .maxQueueSize(1)
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("tidb", "test", "stream"))
                        .build();
        TiDBErrorHandler errorHandler =
                new TiDBErrorHandler(sourceConfig.getDbzConnectorConfig(), queue);
        EventSourceReader reader = createReader(sourceConfig, errorHandler);
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        reader.context = context;
        RuntimeException emissionFailure = new RuntimeException("Failed to convert row");

        reader.handleEmissionFailure(emissionFailure);

        assertThat(context.isRunning()).isFalse();
        assertThat(errorHandler.getProducerThrowable()).isSameAs(emissionFailure);
        assertThatThrownBy(queue::poll)
                .isInstanceOf(ConnectException.class)
                .hasCause(emissionFailure);
    }

    @Test
    void shouldBackpressureWhenCommittedEventQueueIsFull() throws Exception {
        EventSourceReader reader = createReader(createSourceConfig(1), null);
        reader.initializeCommittedEventsQueue();
        Cdcpb.Event.Row first = Cdcpb.Event.Row.newBuilder().setStartTs(1L).build();
        Cdcpb.Event.Row second = Cdcpb.Event.Row.newBuilder().setStartTs(2L).build();
        reader.enqueueCommittedEvent(first);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            CountDownLatch producerStarted = new CountDownLatch(1);
            Future<?> blockedProducer =
                    executor.submit(
                            () -> {
                                producerStarted.countDown();
                                reader.enqueueCommittedEvent(second);
                                return null;
                            });

            assertThat(producerStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> blockedProducer.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            assertThat(reader.takeCommittedEvent()).isEqualTo(first);
            blockedProducer.get(1, TimeUnit.SECONDS);
            assertThat(reader.takeCommittedEvent()).isEqualTo(second);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldInterruptProducerBlockedByCommittedEventQueue() throws Exception {
        EventSourceReader reader = createReader(createSourceConfig(1), null);
        reader.initializeCommittedEventsQueue();
        reader.enqueueCommittedEvent(Cdcpb.Event.Row.getDefaultInstance());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch producerStarted = new CountDownLatch(1);
        CountDownLatch producerInterrupted = new CountDownLatch(1);

        try {
            Future<?> blockedProducer =
                    executor.submit(
                            () -> {
                                producerStarted.countDown();
                                try {
                                    reader.enqueueCommittedEvent(
                                            Cdcpb.Event.Row.getDefaultInstance());
                                } catch (InterruptedException e) {
                                    producerInterrupted.countDown();
                                    Thread.currentThread().interrupt();
                                }
                            });

            assertThat(producerStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> blockedProducer.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            blockedProducer.cancel(true);
            assertThat(producerInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldFailExplicitlyWhenCommitHasNoMatchingPrewrite() throws Exception {
        EventSourceReader reader = createReader();
        reader.context = new StoppableChangeEventSourceContext();
        reader.initializeCommittedEventsQueue();
        initializeEventBuffers(reader);
        Cdcpb.Event.Row commitRow = createRow(Cdcpb.Event.LogType.COMMIT, 10L, 20L);
        invokeHandleRow(reader, commitRow);

        assertThatThrownBy(() -> reader.flushRows(20L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing PREWRITE event for COMMIT")
                .hasMessageContaining("startTs=10")
                .hasMessageContaining("commitTs=20")
                .hasMessageContaining("resolvedTs=20");
        assertThat(getCommittedEventsQueue(reader)).isEmpty();
    }

    @Test
    void shouldEnqueueCommitWithMatchingPrewrite() throws Exception {
        EventSourceReader reader = createReader();
        reader.context = new StoppableChangeEventSourceContext();
        reader.initializeCommittedEventsQueue();
        initializeEventBuffers(reader);
        Cdcpb.Event.Row prewriteRow = createRow(Cdcpb.Event.LogType.PREWRITE, 10L, 0L);
        Cdcpb.Event.Row commitRow = createRow(Cdcpb.Event.LogType.COMMIT, 10L, 20L);
        invokeHandleRow(reader, prewriteRow);
        invokeHandleRow(reader, commitRow);

        reader.flushRows(20L);

        assertThat(reader.takeCommittedEvent()).isEqualTo(prewriteRow);
        assertThat(getCommittedEventsQueue(reader)).isEmpty();
    }

    @Test
    void shouldDrainOnlyCommittedRowsAtOrBeforeBackfillEnd() throws Exception {
        EventSourceReader reader = createReader();
        reader.context = new StoppableChangeEventSourceContext();
        initializeEventBuffers(reader);
        Cdcpb.Event.Row firstPrewrite = createRow(Cdcpb.Event.LogType.PREWRITE, 10L, 0L, 1L);
        Cdcpb.Event.Row firstCommit = createRow(Cdcpb.Event.LogType.COMMIT, 10L, 20L, 1L);
        Cdcpb.Event.Row secondPrewrite = createRow(Cdcpb.Event.LogType.PREWRITE, 30L, 0L, 2L);
        Cdcpb.Event.Row secondCommit = createRow(Cdcpb.Event.LogType.COMMIT, 30L, 40L, 2L);
        invokeHandleRow(reader, firstPrewrite);
        invokeHandleRow(reader, firstCommit);
        invokeHandleRow(reader, secondPrewrite);
        invokeHandleRow(reader, secondCommit);

        assertThat(reader.drainCommittedRows(20L)).containsExactly(firstPrewrite);
        assertThat(reader.drainCommittedRows(40L)).containsExactly(secondPrewrite);
    }

    @Test
    void shouldCompleteBoundedBackfillWithoutEmittingPastEndingOffset() throws Exception {
        TiDBSourceConfig sourceConfig = createSourceConfig();
        StreamSplit boundedSplit =
                createStreamSplit(new EventOffset("0", "10"), new EventOffset("0", "40"));
        Cdcpb.Event.Row firstPrewrite = createRow(Cdcpb.Event.LogType.PREWRITE, 10L, 0L, 1L);
        Cdcpb.Event.Row firstCommit = createRow(Cdcpb.Event.LogType.COMMIT, 10L, 20L, 1L);
        Cdcpb.Event.Row secondPrewrite = createRow(Cdcpb.Event.LogType.PREWRITE, 30L, 0L, 2L);
        Cdcpb.Event.Row secondCommit = createRow(Cdcpb.Event.LogType.COMMIT, 30L, 60L, 2L);
        TestingEventSourceReader reader =
                new TestingEventSourceReader(
                        sourceConfig,
                        boundedSplit,
                        50L,
                        firstPrewrite,
                        firstCommit,
                        secondPrewrite,
                        secondCommit);
        initializeEventBuffers(reader);
        setField(reader, "resolvedTs", 10L);
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();

        boolean completed = reader.executeBackfill(context, null, null);

        assertThat(completed).isTrue();
        assertThat(reader.startedAt).isEqualTo(10L);
        assertThat(reader.emittedRows).containsExactly(firstPrewrite);
        assertThat(reader.drainCommittedRows(100L)).containsExactly(secondPrewrite);
    }

    private Cdcpb.Event.Row createRow(Cdcpb.Event.LogType type, long startTs, long commitTs) {
        return createRow(type, startTs, commitTs, 1L);
    }

    private Cdcpb.Event.Row createRow(
            Cdcpb.Event.LogType type, long startTs, long commitTs, long handle) {
        return Cdcpb.Event.Row.newBuilder()
                .setType(type)
                .setKey(RowKey.toRowKey(1L, handle).toByteString())
                .setStartTs(startTs)
                .setCommitTs(commitTs)
                .build();
    }

    private void initializeEventBuffers(EventSourceReader reader) throws Exception {
        setField(reader, "prewrites", new TreeMap<>());
        setField(reader, "commits", new TreeMap<>());
    }

    private void invokeHandleRow(EventSourceReader reader, Cdcpb.Event.Row row) throws Exception {
        Method handleRow = EventSourceReader.class.getDeclaredMethod("handleRow", row.getClass());
        handleRow.setAccessible(true);
        handleRow.invoke(reader, row);
    }

    private BlockingQueue<?> getCommittedEventsQueue(EventSourceReader reader) throws Exception {
        Field field = EventSourceReader.class.getDeclaredField("committedEvents");
        field.setAccessible(true);
        return (BlockingQueue<?>) field.get(reader);
    }

    private void setField(EventSourceReader reader, String fieldName, Object value)
            throws Exception {
        Field field = EventSourceReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(reader, value);
    }

    private EventSourceReader createReader() {
        TiDBSourceConfig sourceConfig = createSourceConfig();
        return createReader(sourceConfig, null);
    }

    private TiDBSourceConfig createSourceConfig() {
        return createSourceConfig(null);
    }

    private TiDBSourceConfig createSourceConfig(Integer maxQueueSize) {
        TiDBSourceConfigFactory configFactory = new TiDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(4000);
        configFactory.username("root");
        configFactory.password("");
        configFactory.databaseList("inventory");
        configFactory.tableList("inventory.products");
        configFactory.pdAddresses("localhost:2379");
        if (maxQueueSize != null) {
            Properties debeziumProperties = new Properties();
            debeziumProperties.setProperty("max.queue.size", String.valueOf(maxQueueSize));
            debeziumProperties.setProperty("max.batch.size", String.valueOf(maxQueueSize));
            configFactory.jdbcProperties(debeziumProperties);
        }
        return configFactory.create(0);
    }

    private EventSourceReader createReader(
            TiDBSourceConfig sourceConfig, TiDBErrorHandler errorHandler) {
        return new EventSourceReader(
                sourceConfig.getDbzConnectorConfig(),
                null,
                errorHandler,
                null,
                createStreamSplit());
    }

    private StreamSplit createStreamSplit() {
        return createStreamSplit(EventOffset.INITIAL_OFFSET, EventOffset.NO_STOPPING_OFFSET);
    }

    private StreamSplit createStreamSplit(EventOffset startingOffset, EventOffset endingOffset) {
        return new StreamSplit(
                "stream-split",
                startingOffset,
                endingOffset,
                Collections.emptyList(),
                Collections.singletonMap(new TableId("inventory", null, "products"), null),
                0);
    }

    private static class TestingEventSourceReader extends EventSourceReader {
        private final Queue<Cdcpb.Event.Row> rows = new ArrayDeque<>();
        private final long minResolvedTs;
        private final List<Cdcpb.Event.Row> emittedRows = new ArrayList<>();
        private long startedAt = -1L;

        private TestingEventSourceReader(
                TiDBSourceConfig sourceConfig,
                StreamSplit split,
                long minResolvedTs,
                Cdcpb.Event.Row... rows) {
            super(sourceConfig.getDbzConnectorConfig(), null, null, null, split);
            this.minResolvedTs = minResolvedTs;
            Collections.addAll(this.rows, rows);
        }

        @Override
        protected void assureNonEmptySchema() {}

        @Override
        protected void startCdcClient(long startTs) {
            startedAt = startTs;
        }

        @Override
        protected Cdcpb.Event.Row pollChangeEvent() {
            return rows.poll();
        }

        @Override
        protected long getMinResolvedTs() {
            return minResolvedTs;
        }

        @Override
        protected void emitChangeEvent(
                TiDBPartition partition, EventOffsetContext offsetContext, Cdcpb.Event.Row row) {
            emittedRows.add(row);
        }
    }
}
