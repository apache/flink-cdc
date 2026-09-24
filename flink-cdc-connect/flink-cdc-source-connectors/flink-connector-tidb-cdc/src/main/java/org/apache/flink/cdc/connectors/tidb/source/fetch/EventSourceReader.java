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

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.utils.TableKeyRangeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.tikv.common.codec.TableCodec.decodeObjects;
import static org.tikv.common.codec.TiDBRowV2Decoder.decodeObjectsPreservingBinary;

/** TiDB streaming change event source reader. */
public class EventSourceReader
        implements StreamingChangeEventSource<TiDBPartition, EventOffsetContext> {
    private static final Logger LOG = LoggerFactory.getLogger(EventSourceReader.class);
    private final StreamSplit split;
    private final TiDBConnectorConfig connectorConfig;
    private final TiConfiguration ticonf;
    private final JdbcSourceEventDispatcher<TiDBPartition> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final TiDBSourceFetchTaskContext taskContext;
    private final Map<TableSchema, Map<String, Integer>> fieldIndexMap = new HashMap<>();
    public volatile ChangeEventSourceContext context;

    private static final long STREAMING_VERSION_START_EPOCH = 0L;

    /** Task local variables. */
    private transient TiSession session = null;

    private transient Coprocessor.KeyRange keyRange = null;
    private transient CDCClient cdcClient = null;
    private transient volatile long resolvedTs = -1L;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> prewrites = null;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits = null;
    private transient BlockingQueue<Cdcpb.Event.Row> committedEvents = null;
    private transient TableId tableId;
    private transient TiTableInfo tableInfo;

    private transient volatile boolean running;
    private transient volatile Thread executionThread;
    private transient ExecutorService executorService;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public EventSourceReader(
            TiDBConnectorConfig connectorConfig,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            TiDBSourceFetchTaskContext taskContext,
            StreamSplit split) {
        this.connectorConfig = connectorConfig;
        this.ticonf = connectorConfig.getSourceConfig().getTiConfiguration();
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.split = split;
    }

    @Override
    public synchronized void init() throws InterruptedException {
        if (closed.get()) {
            return;
        }
        StreamingChangeEventSource.super.init();
        try {
            session = TiSession.create(ticonf);
            Set<TableId> tableIds = this.split.getTableSchemas().keySet();
            if (tableIds.size() != 1) {
                throw new IllegalStateException(
                        "Currently only single table ingest is supported, but found "
                                + tableIds.size()
                                + " tables.");
            }
            this.tableId = tableIds.stream().findFirst().get();
            this.tableInfo = session.getCatalog().getTable(tableId.catalog(), tableId.table());
            if (tableInfo == null) {
                throw new RuntimeException(
                        String.format(
                                "Table %s.%s does not exist.", tableId.catalog(), tableId.table()));
            }
            keyRange = TableKeyRangeUtils.getTableKeyRange(tableInfo.getId(), 1, 0);
            cdcClient = new CDCClient(session, keyRange);
            prewrites = new TreeMap<>();
            commits = new TreeMap<>();
            // cdc event will lose if pull cdc event block when region split
            // use queue to separate read and write to ensure pull event unblock.
            // since sink jdbc is slow, 5000W queue size may be safe size.
            committedEvents = new LinkedBlockingQueue<>();
            resolvedTs = EventOffset.getStartTs(this.split.getStartingOffset());
            ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setNameFormat("tidb-source-function-0").build();
            executorService = Executors.newSingleThreadExecutor(threadFactory);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            EventOffsetContext offsetContext)
            throws InterruptedException {
        if (closed.get()) {
            return;
        }
        this.context = context;
        this.executionThread = Thread.currentThread();
        running = true;
        try {
            if (connectorConfig.getSourceConfig().getStartupOptions().isSnapshotOnly()) {
                LOG.info("Streaming is not enabled in current configuration");
                return;
            }
            this.taskContext.getDatabaseSchema().assureNonEmptySchema();
            cdcClient.start(resolvedTs);
            EventOffsetContext effectiveOffsetContext =
                    offsetContext != null
                            ? offsetContext
                            : EventOffsetContext.initial(this.connectorConfig);
            readChangeEvents(partition, effectiveOffsetContext);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw e;
            }
        } catch (Exception e) {
            if (!closed.get()) {
                this.errorHandler.setProducerThrowable(e);
            }
        } finally {
            running = false;
            executionThread = null;
        }
    }

    protected void readChangeEvents(TiDBPartition partition, EventOffsetContext offsetContext)
            throws Exception {
        LOG.info("read change event from resolvedTs:{}", resolvedTs);
        // child thread to sink committed rows.
        executorService.execute(
                () -> {
                    while (running && context.isRunning()) {
                        try {
                            Cdcpb.Event.Row committedRow = committedEvents.take();
                            emitChangeEvent(partition, offsetContext, committedRow);
                            // use startTs of row as messageTs, use commitTs of row as fetchTs
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception e) {
                            if (running && context.isRunning()) {
                                LOG.error("Read change events error.", e);
                            }
                        }
                    }
                });
        while (running && context.isRunning() && resolvedTs >= STREAMING_VERSION_START_EPOCH) {
            for (int i = 0; i < 1000; i++) {
                final Cdcpb.Event.Row row = cdcClient.get();
                if (row == null) {
                    break;
                }
                handleRow(row);
            }
            resolvedTs = cdcClient.getMaxResolvedTs();
            if (commits.size() > 0) {
                flushRows(resolvedTs);
            }
        }
    }

    protected void emitChangeEvent(
            TiDBPartition partition, EventOffsetContext offsetContext, final Cdcpb.Event.Row row)
            throws Exception {
        if (!context.isRunning()) {
            LOG.info("sourceContext is not running, skip emit change event.");
            return;
        }
        if (tableId == null) {
            LOG.warn("No valid tableId found, skipping log message: {}", row);
            return;
        }
        TableSchema tableSchema = taskContext.getDatabaseSchema().schemaFor(tableId);
        if (tableSchema == null) {
            LOG.warn("No table schema found, skipping log message: {}", row);
            return;
        }
        offsetContext.event(tableSchema.id(), Instant.ofEpochMilli(row.getCommitTs()));
        Set<Integer> fieldIndex = fieldIndexConverter(tableInfo.getColumns(), tableSchema);

        Serializable[] before = null;
        Serializable[] after = null;
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        Envelope.Operation operation = getOperation(row);
        switch (operation) {
            case CREATE:
                after =
                        (Serializable[])
                                getSerializableObject(
                                        handle, row.getValue(), tableInfo, fieldIndex);
                break;
            case UPDATE:
                before =
                        (Serializable[])
                                getSerializableObject(
                                        handle, row.getOldValue(), tableInfo, fieldIndex);
                after =
                        (Serializable[])
                                getSerializableObject(
                                        handle, row.getValue(), tableInfo, fieldIndex);
                break;
            case DELETE:
                before =
                        (Serializable[])
                                getSerializableObject(
                                        handle, row.getOldValue(), tableInfo, fieldIndex);

                break;
            default:
                LOG.error("Row data opType is not supported,row:{}.", row);
        }
        eventDispatcher.dispatchDataChangeEvent(
                partition,
                tableSchema.id(),
                new EventEmitter(partition, offsetContext, Clock.SYSTEM, operation, before, after));
    }

    private Object[] getSerializableObject(
            long handle, final ByteString value, TiTableInfo tableInfo, Set<Integer> fieldIndex) {
        Object[] serializableObject = new Serializable[fieldIndex.size()];
        try {
            if (value == null) {
                return null;
            }

            Object[] tiKVValueAfter;
            if (value != null && !value.isEmpty()) {
                byte[] encodedValue = value.toByteArray();
                tiKVValueAfter =
                        Byte.toUnsignedInt(encodedValue[0]) == org.tikv.common.codec.RowV2.CODEC_VER
                                ? decodeObjectsPreservingBinary(encodedValue, handle, tableInfo)
                                : decodeObjects(encodedValue, handle, tableInfo);
            } else {
                return null;
            }
            for (int index : fieldIndex) {
                serializableObject[index] = tiKVValueAfter[index];
            }
            return serializableObject;
        } catch (Exception e) {
            LOG.error("decode object error", e);
            return null;
        }
    }

    private Set<Integer> fieldIndexConverter(
            List<TiColumnInfo> tiColumnInfos, TableSchema tableSchema) {
        Map<String, Integer> fieldIndex =
                fieldIndexMap.computeIfAbsent(
                        tableSchema,
                        schema ->
                                IntStream.range(0, schema.valueSchema().fields().size())
                                        .boxed()
                                        .collect(
                                                Collectors.toMap(
                                                        i ->
                                                                schema.valueSchema()
                                                                        .fields()
                                                                        .get(i)
                                                                        .name(),
                                                        i -> i)));
        Set<Integer> fieldIndexSet = new HashSet<>();
        for (TiColumnInfo tiColumnInfo : tiColumnInfos) {
            if (fieldIndex.keySet().stream()
                    .anyMatch(key -> key.equalsIgnoreCase(tiColumnInfo.getName()))) {
                fieldIndexSet.add(tiColumnInfo.getOffset());
            }
        }
        return fieldIndexSet;
    }

    private Envelope.Operation getOperation(final Cdcpb.Event.Row row) {
        if (row.getOpType() == Cdcpb.Event.Row.OpType.PUT) { // create ，update
            if (row.getValue() != null && !row.getOldValue().isEmpty()) {
                return Envelope.Operation.UPDATE;
            } else {
                return Envelope.Operation.CREATE;
            }
        } else if (row.getOpType() == Cdcpb.Event.Row.OpType.DELETE) { // delete
            return Envelope.Operation.DELETE;
        } else {
            LOG.error("Row data opType is not supported,row:{}.", row);
            return null;
        }
    }

    protected void flushRows(final long timestamp) throws Exception {
        Preconditions.checkState(context != null, "sourceContext shouldn't be null");
        synchronized (context) {
            while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
                final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
                final Cdcpb.Event.Row prewriteRow =
                        prewrites.remove(RowKeyWithTs.ofStart(commitRow));
                // if pull cdc event block when region split, cdc event will lose.
                committedEvents.offer(prewriteRow);
            }
        }
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return;
        }
        LOG.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                LOG.warn("Unsupported row type:" + row.getType());
        }
    }

    @Override
    public boolean executeIteration(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            EventOffsetContext offsetContext)
            throws InterruptedException {
        return StreamingChangeEventSource.super.executeIteration(context, partition, offsetContext);
    }

    /** Stops event production and releases all TiDB resources. This operation is idempotent. */
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        running = false;
        ChangeEventSourceContext currentContext = context;
        if (currentContext instanceof StoppableChangeEventSourceContext) {
            ((StoppableChangeEventSourceContext) currentContext).stopChangeEventSource();
        }

        Thread currentExecutionThread = executionThread;
        if (currentExecutionThread != null && currentExecutionThread != Thread.currentThread()) {
            currentExecutionThread.interrupt();
        }

        if (cdcClient != null) {
            try {
                cdcClient.close();
            } catch (RuntimeException e) {
                LOG.warn("Failed to close TiDB CDC client.", e);
            } finally {
                cdcClient = null;
            }
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("Failed to close TiDB session.", e);
            } finally {
                session = null;
            }
        }
    }

    boolean isClosed() {
        return closed.get();
    }

    // ---------------------------------------
    // static Utils classes
    // ---------------------------------------
    private static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }
}
