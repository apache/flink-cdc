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

package org.apache.flink.cdc.connectors.gaussdb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;
import io.debezium.connector.gaussdb.GaussDBSchema;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** A fetch task for reading streaming changes from GaussDB using logical replication. */
public class GaussDBStreamFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBStreamFetchTask.class);

    private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    private static final int DEFAULT_STATUS_UPDATE_INTERVAL_MS = 10000;
    private static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 5;
    private static final int DEFAULT_MESSAGE_LOG_INTERVAL = 1000;

    private final StreamSplit streamSplit;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private volatile ReplicationStream replicationStream;
    private volatile long lastHeartbeatTime;
    private volatile long lastStatusUpdateTime;

    private volatile LazyPgConnectionSupplier pgConnectionSupplier;

    public GaussDBStreamFetchTask(StreamSplit streamSplit) {
        this.streamSplit = streamSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        if (stopped.get()) {
            LOG.debug(
                    "StreamFetchTask for split {} is already stopped and cannot be executed",
                    streamSplit);
            return;
        }

        LOG.info("Starting StreamFetchTask for split: {}", streamSplit);
        if (!(context instanceof GaussDBSourceFetchTaskContext)) {
            throw new FlinkRuntimeException(
                    "Unsupported fetch task context type: " + context.getClass().getName());
        }
        GaussDBSourceFetchTaskContext ctx = (GaussDBSourceFetchTaskContext) context;

        running.set(true);
        lastHeartbeatTime = System.currentTimeMillis();
        lastStatusUpdateTime = System.currentTimeMillis();

        try {
            // Create replication stream
            replicationStream = createReplicationStream(ctx);

            // Start streaming changes
            streamChanges(ctx, replicationStream);

        } catch (Exception e) {
            if (!stopped.get()) {
                LOG.error("Error during streaming for split: {}", streamSplit, e);
                throw new FlinkRuntimeException(
                        "Failed to stream changes from GaussDB for split: " + streamSplit, e);
            } else {
                LOG.debug("StreamFetchTask stopped during execution for split: {}", streamSplit);
            }
        } finally {
            running.set(false);
            closeReplicationStream();
            closePgConnectionSupplier();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public SourceSplitBase getSplit() {
        return streamSplit;
    }

    @Override
    public void close() {
        LOG.debug("Closing StreamFetchTask for split: ", streamSplit);
        stopped.set(true);
        running.set(false);
        closeReplicationStream();
    }

    /** Creates a replication stream for reading WAL changes from GaussDB. */
    private ReplicationStream createReplicationStream(GaussDBSourceFetchTaskContext context)
            throws SQLException {
        return createReplicationStream(context, null);
    }

    private ReplicationStream createReplicationStream(
            GaussDBSourceFetchTaskContext context,
            io.debezium.connector.postgresql.connection.Lsn overrideStartLsn)
            throws SQLException {
        String slotName = context.getSlotName();
        String pluginName = context.getDecodingPluginName();

        LOG.info("Creating replication stream with slot: {}, plugin: {}", slotName, pluginName);

        // Get the replication connection from the context
        io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection
                replicationConnection = context.getReplicationConnection();

        if (replicationConnection == null) {
            throw new SQLException("Replication connection is not initialized in context");
        }

        // Get the starting LSN from the stream split
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset startOffset =
                (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset)
                        streamSplit.getStartingOffset();
        io.debezium.connector.gaussdb.connection.Lsn gaussdbLsn =
                startOffset != null ? startOffset.getLsn() : null;

        // Convert GaussDB LSN to PostgreSQL LSN for the replication API
        io.debezium.connector.postgresql.connection.Lsn startLsn = overrideStartLsn;
        if (startLsn == null && gaussdbLsn != null && gaussdbLsn.isValid()) {
            startLsn = io.debezium.connector.postgresql.connection.Lsn.valueOf(gaussdbLsn.asLong());
        }

        // Start streaming from the specified LSN
        try {
            return replicationConnection.startStreaming(startLsn, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while starting replication stream", e);
        }
    }

    /** Streams changes from the replication stream and processes them. */
    private void streamChanges(GaussDBSourceFetchTaskContext context, ReplicationStream stream)
            throws Exception {

        final AtomicLong messageCount = new AtomicLong(0);
        final AtomicLong emittedCount = new AtomicLong(0);
        final AtomicReference<ReplicationStream> streamRef = new AtomicReference<>(stream);
        final AtomicReference<io.debezium.connector.postgresql.connection.Lsn> lastProcessedLsn =
                new AtomicReference<>(stream.lastReceivedLsn());

        final ChangeEventQueue<DataChangeEvent> queue = requireQueue(context);
        final GaussDBConnectorConfig connectorConfig = context.getDbzConnectorConfig();
        final GaussDBSchema gaussDBSchema =
                new GaussDBSchema(connectorConfig, context.getConnection());
        this.pgConnectionSupplier = new LazyPgConnectionSupplier(connectorConfig);

        // Create a message processor that will be called for each message
        io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor
                processor =
                        (message) -> {
                            messageCount.incrementAndGet();
                            try {
                                processReplicationMessage(
                                        context,
                                        gaussDBSchema,
                                        queue,
                                        streamRef,
                                        lastProcessedLsn,
                                        message,
                                        emittedCount);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw ie;
                            } catch (Exception e) {
                                handleMessageProcessingFailure(
                                        context,
                                        gaussDBSchema,
                                        queue,
                                        streamRef,
                                        lastProcessedLsn,
                                        message,
                                        emittedCount,
                                        e);
                            }

                            if (messageCount.get() % DEFAULT_MESSAGE_LOG_INTERVAL == 0) {
                                LOG.debug(
                                        "Processed {} messages (emitted {}) for split: {}",
                                        messageCount.get(),
                                        emittedCount.get(),
                                        streamSplit.splitId());
                            }
                        };

        try {
            int consecutiveFailures = 0;
            while (running.get() && !stopped.get()) {
                boolean messageProcessed;
                try {
                    // Read and process messages from the replication stream
                    messageProcessed = streamRef.get().readPending(processor);
                    consecutiveFailures = 0;
                } catch (SQLException e) {
                    if (!running.get() || stopped.get()) {
                        break;
                    }

                    consecutiveFailures++;
                    if (consecutiveFailures > DEFAULT_MAX_CONSECUTIVE_FAILURES) {
                        throw new FlinkRuntimeException(
                                String.format(
                                        "Failed to read pending messages for split %s after %d retries",
                                        streamSplit.splitId(), consecutiveFailures),
                                e);
                    }

                    final io.debezium.connector.postgresql.connection.Lsn resumeLsn =
                            lastProcessedLsn.get();
                    LOG.warn(
                            "Read pending messages failed for split {} (attempt {}/{}), retrying from LSN {}",
                            streamSplit.splitId(),
                            consecutiveFailures,
                            DEFAULT_MAX_CONSECUTIVE_FAILURES,
                            resumeLsn,
                            e);
                    reconnectReplicationStream(context, streamRef, resumeLsn, consecutiveFailures);
                    continue;
                }

                if (!messageProcessed) {
                    // No message available, sleep briefly to avoid busy waiting
                    Thread.sleep(10);
                }

                // Send heartbeat if needed
                maybeUpdateHeartbeat(streamRef.get());

                // Update replication slot status if needed
                maybeUpdateStatus(streamRef.get());

                // Check if we should stop (for bounded streams)
                if (shouldStop(context, streamRef.get())) {
                    final GaussDBOffset endOffset = computeEndOffset(streamRef.get());
                    if (endOffset != null) {
                        enqueueWatermarkEvent(
                                queue,
                                context,
                                streamSplit.splitId(),
                                endOffset,
                                WatermarkKind.END);
                    }
                    LOG.info(
                            "Reached end of bounded stream for split: {} at {}",
                            streamSplit.splitId(),
                            endOffset);
                    break;
                }
            }

            LOG.info(
                    "Finished streaming {} messages (emitted {}) for split: {}",
                    messageCount.get(),
                    emittedCount.get(),
                    streamSplit.splitId());
        } finally {
            gaussDBSchema.close();
        }
    }

    private void handleMessageProcessingFailure(
            GaussDBSourceFetchTaskContext context,
            GaussDBSchema gaussDBSchema,
            ChangeEventQueue<DataChangeEvent> queue,
            AtomicReference<ReplicationStream> streamRef,
            AtomicReference<io.debezium.connector.postgresql.connection.Lsn> lastProcessedLsn,
            ReplicationMessage message,
            AtomicLong emittedCount,
            Exception error)
            throws SQLException, InterruptedException {
        final TableId tableId = parseTableId(message != null ? message.getTable() : null);
        if (tableId != null) {
            try {
                gaussDBSchema.refreshTable(tableId);
                LOG.debug(
                        "Refreshed table schema for {} after message processing failure", tableId);
            } catch (Exception refreshError) {
                error.addSuppressed(refreshError);
            }
        }

        // Retry once after schema refresh.
        try {
            processReplicationMessage(
                    context,
                    gaussDBSchema,
                    queue,
                    streamRef,
                    lastProcessedLsn,
                    message,
                    emittedCount);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (Exception retryError) {
            retryError.addSuppressed(error);
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to process replication message for split %s",
                            streamSplit.splitId()),
                    retryError);
        }
    }

    private void processReplicationMessage(
            GaussDBSourceFetchTaskContext context,
            GaussDBSchema gaussDBSchema,
            ChangeEventQueue<DataChangeEvent> queue,
            AtomicReference<ReplicationStream> streamRef,
            AtomicReference<io.debezium.connector.postgresql.connection.Lsn> lastProcessedLsn,
            ReplicationMessage message,
            AtomicLong emittedCount)
            throws SQLException, InterruptedException {
        if (message == null) {
            return;
        }

        // Skip non-DML messages.
        final ReplicationMessage.Operation operation = message.getOperation();
        if (operation == ReplicationMessage.Operation.BEGIN
                || operation == ReplicationMessage.Operation.COMMIT
                || operation == ReplicationMessage.Operation.NOOP) {
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        final TableId tableId = parseTableId(message.getTable());
        if (tableId == null) {
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        if (!context.getTableFilter().isIncluded(tableId)) {
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        final TableSchema tableSchema = gaussDBSchema.schemaFor(tableId);
        final Table table = gaussDBSchema.tableFor(tableId);
        if (tableSchema == null || table == null) {
            LOG.debug("Skip message for unknown/untracked table: {}", tableId);
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        final int rowSize = maxColumnPosition(table);
        final Object[] afterRow = rowSize > 0 ? new Object[rowSize] : new Object[0];
        final Object[] beforeRow = rowSize > 0 ? new Object[rowSize] : new Object[0];

        final BaseConnection pgConn = pgConnectionSupplier.get();
        final io.debezium.connector.postgresql.PostgresStreamingChangeEventSource
                        .PgConnectionSupplier
                pgSupplier = () -> pgConn;
        final boolean includeUnknownDatatypes = true;

        if (operation == ReplicationMessage.Operation.INSERT
                || operation == ReplicationMessage.Operation.UPDATE) {
            fillRowFromTuple(
                    table,
                    afterRow,
                    message.getNewTupleList(),
                    pgSupplier,
                    includeUnknownDatatypes);
        }
        if (operation == ReplicationMessage.Operation.DELETE
                || operation == ReplicationMessage.Operation.UPDATE) {
            fillRowFromTuple(
                    table,
                    beforeRow,
                    message.getOldTupleList(),
                    pgSupplier,
                    includeUnknownDatatypes);
            // When old tuple only contains keys, valueFromColumnData will produce a Struct with
            // nulls for non-key columns. This keeps UPDATE/DELETE records deserializable.
        }
        if (operation == ReplicationMessage.Operation.DELETE
                && (isAllNullRow(beforeRow) && !isAllNullRow(afterRow))) {
            // Fallback: some decoders provide DELETE values in new tuple.
            System.arraycopy(afterRow, 0, beforeRow, 0, afterRow.length);
        }
        if (operation == ReplicationMessage.Operation.UPDATE && isAllNullRow(beforeRow)) {
            // Fallback: if old tuple is missing, emit a best-effort before image from after.
            System.arraycopy(afterRow, 0, beforeRow, 0, afterRow.length);
        }

        final Struct keyStruct =
                !isAllNullRow(afterRow)
                        ? tableSchema.keyFromColumnData(afterRow)
                        : tableSchema.keyFromColumnData(beforeRow);
        final Struct afterStruct =
                !isAllNullRow(afterRow) ? tableSchema.valueFromColumnData(afterRow) : null;
        final Struct beforeStruct =
                !isAllNullRow(beforeRow) ? tableSchema.valueFromColumnData(beforeRow) : null;

        final io.debezium.connector.postgresql.connection.Lsn currentLsn =
                streamRef.get().lastReceivedLsn();
        final Map<String, Object> sourceOffset = lsnOffset(currentLsn);
        final Map<String, ?> sourcePartition = context.getPartition().getSourcePartition();
        final String topic = topicFor(context.getDbzConnectorConfig(), tableId);

        final Schema sourceSchema =
                Objects.requireNonNull(
                        context.getDbzConnectorConfig().getSourceInfoStructMaker().schema(),
                        "source info schema");
        final Struct sourceStruct =
                buildSourceStruct(sourceSchema, context.getDbzConnectorConfig(), tableId, message);

        final Envelope envelope = tableSchema.getEnvelopeSchema();
        final Instant fetchTs = Instant.now();
        final Struct valueStruct;
        switch (operation) {
            case INSERT:
                valueStruct = envelope.create(afterStruct, sourceStruct, fetchTs);
                break;
            case UPDATE:
                valueStruct = envelope.update(beforeStruct, afterStruct, sourceStruct, fetchTs);
                break;
            case DELETE:
                valueStruct = envelope.delete(beforeStruct, sourceStruct, fetchTs);
                break;
            default:
                // Shouldn't happen due to earlier filter.
                updateLastProcessedLsn(streamRef, lastProcessedLsn);
                return;
        }

        final SourceRecord record =
                new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        tableSchema.keySchema(),
                        keyStruct,
                        envelope.schema(),
                        valueStruct);
        queue.enqueue(new DataChangeEvent(record));
        emittedCount.incrementAndGet();
        updateLastProcessedLsn(streamRef, lastProcessedLsn);
    }

    private static Struct buildSourceStruct(
            Schema sourceSchema,
            GaussDBConnectorConfig connectorConfig,
            TableId tableId,
            ReplicationMessage message) {
        final Struct sourceStruct = new Struct(sourceSchema);

        // Set version field (required by Debezium source info schema)
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.DEBEZIUM_VERSION_KEY,
                "unknown");

        // Set connector name (required field)
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY,
                "gaussdb");

        // Set logical name (required field) - use server name from config
        String logicalName = connectorConfig.getLogicalName();
        if (logicalName == null || logicalName.isEmpty()) {
            logicalName = "gaussdb";
        }
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.SERVER_NAME_KEY,
                logicalName);

        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY,
                connectorConfig.getJdbcConfig().getDatabase());
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY,
                tableId.schema());
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY,
                tableId.table());

        final Instant commitTime =
                message.getCommitTime() != null ? message.getCommitTime() : Instant.now();
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.TIMESTAMP_KEY,
                commitTime.toEpochMilli());

        // Set snapshot field to "false" for streaming records
        putIfPresent(sourceStruct, io.debezium.connector.AbstractSourceInfo.SNAPSHOT_KEY, "false");

        return sourceStruct;
    }

    private static void putIfPresent(Struct struct, String fieldName, Object value) {
        if (struct.schema().field(fieldName) != null) {
            struct.put(fieldName, value);
        }
    }

    private static void fillRowFromTuple(
            Table table,
            Object[] row,
            java.util.List<ReplicationMessage.Column> tuple,
            io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier
                    pgSupplier,
            boolean includeUnknownDatatypes)
            throws SQLException {
        if (tuple == null || tuple.isEmpty() || table == null || row == null) {
            return;
        }
        for (ReplicationMessage.Column column : tuple) {
            if (column == null) {
                continue;
            }
            final String columnName = column.getName();
            if (columnName == null) {
                continue;
            }
            final io.debezium.relational.Column tableColumn = table.columnWithName(columnName);
            if (tableColumn == null) {
                continue;
            }
            final int pos = tableColumn.position();
            if (pos <= 0 || pos > row.length) {
                continue;
            }
            row[pos - 1] = column.getValue(pgSupplier, includeUnknownDatatypes);
        }
    }

    private static int maxColumnPosition(Table table) {
        if (table == null || table.columns() == null || table.columns().isEmpty()) {
            return 0;
        }
        int max = 0;
        for (io.debezium.relational.Column c : table.columns()) {
            if (c != null && c.position() > max) {
                max = c.position();
            }
        }
        return max;
    }

    private static boolean isAllNullRow(Object[] row) {
        if (row == null) {
            return true;
        }
        for (Object o : row) {
            if (o != null) {
                return false;
            }
        }
        return true;
    }

    private static TableId parseTableId(String messageTable) {
        if (messageTable == null || messageTable.trim().isEmpty()) {
            return null;
        }
        try {
            return TableId.parse(messageTable.trim());
        } catch (Exception ignored) {
            // Fallback for schema.table format without quotes.
            try {
                String normalized = messageTable.trim().replace("\"", "");
                String[] parts = normalized.split("\\.");
                if (parts.length == 2) {
                    return new TableId(null, parts[0], parts[1]);
                }
                if (parts.length == 1) {
                    return new TableId(null, null, parts[0]);
                }
            } catch (Exception ignored2) {
            }
            return null;
        }
    }

    private static String topicFor(GaussDBConnectorConfig config, TableId tableId) {
        final String prefix =
                config != null && !Strings.isNullOrEmpty(config.getLogicalName())
                        ? config.getLogicalName()
                        : "gaussdb";
        if (tableId == null) {
            return prefix;
        }
        if (!Strings.isNullOrEmpty(tableId.schema())) {
            return prefix + "." + tableId.schema() + "." + tableId.table();
        }
        return prefix + "." + tableId.table();
    }

    private static Map<String, Object> lsnOffset(
            io.debezium.connector.postgresql.connection.Lsn lsn) {
        final Map<String, Object> offsets = new HashMap<>(1);
        if (lsn != null && lsn.isValid()) {
            offsets.put(GaussDBOffset.LSN_KEY, lsn.asLong());
        } else {
            offsets.put(
                    GaussDBOffset.LSN_KEY,
                    io.debezium.connector.gaussdb.connection.Lsn.INVALID_LSN.asLong());
        }
        return offsets;
    }

    private static void updateLastProcessedLsn(
            AtomicReference<ReplicationStream> streamRef,
            AtomicReference<io.debezium.connector.postgresql.connection.Lsn> lastProcessedLsn) {
        try {
            io.debezium.connector.postgresql.connection.Lsn lsn = streamRef.get().lastReceivedLsn();
            if (lsn != null && lsn.isValid()) {
                lastProcessedLsn.set(lsn);
            }
        } catch (Exception ignored) {
        }
    }

    private void reconnectReplicationStream(
            GaussDBSourceFetchTaskContext context,
            AtomicReference<ReplicationStream> streamRef,
            io.debezium.connector.postgresql.connection.Lsn resumeLsn,
            int failureCount)
            throws SQLException, InterruptedException {
        final long backoffMs = Math.min(5_000L, 200L * (1L << Math.min(4, failureCount)));
        final long jitter = ThreadLocalRandom.current().nextLong(0, 200L);
        Thread.sleep(backoffMs + jitter);

        ReplicationStream old = streamRef.get();
        try {
            old.close();
        } catch (Exception e) {
            LOG.debug(
                    "Failed to close old replication stream for split {}",
                    streamSplit.splitId(),
                    e);
        }

        ReplicationStream newStream = createReplicationStream(context, resumeLsn);
        streamRef.set(newStream);
        this.replicationStream = newStream;
        lastHeartbeatTime = System.currentTimeMillis();
        lastStatusUpdateTime = System.currentTimeMillis();
        LOG.info(
                "Reconnected replication stream for split {} from LSN {}",
                streamSplit.splitId(),
                resumeLsn);
    }

    private static ChangeEventQueue<DataChangeEvent> requireQueue(
            GaussDBSourceFetchTaskContext ctx) {
        ChangeEventQueue<DataChangeEvent> queue = ctx.getQueue();
        if (queue == null) {
            throw new FlinkRuntimeException(
                    "ChangeEventQueue is not initialized in GaussDBSourceFetchTaskContext");
        }
        return queue;
    }

    private static void enqueueWatermarkEvent(
            ChangeEventQueue<DataChangeEvent> queue,
            GaussDBSourceFetchTaskContext context,
            String splitId,
            GaussDBOffset offset,
            WatermarkKind kind)
            throws InterruptedException {
        final SourceRecord watermark =
                WatermarkEvent.create(
                        context.getPartition().getSourcePartition(),
                        "gaussdb-watermark",
                        splitId,
                        kind,
                        offset);
        queue.enqueue(new DataChangeEvent(watermark));
    }

    private GaussDBOffset computeEndOffset(ReplicationStream stream) {
        if (stream == null) {
            return null;
        }
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset endingOffset =
                (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset)
                        streamSplit.getEndingOffset();
        io.debezium.connector.postgresql.connection.Lsn last = stream.lastReceivedLsn();
        if (last != null && last.isValid()) {
            GaussDBOffset current =
                    new GaussDBOffset(
                            io.debezium.connector.gaussdb.connection.Lsn.valueOf(last.asLong()));
            if (endingOffset != null
                    && endingOffset.getLsn() != null
                    && endingOffset.getLsn().isValid()) {
                GaussDBOffset end = new GaussDBOffset(endingOffset.getLsn());
                return current.compareTo(end) >= 0 ? current : end;
            }
            return current;
        }
        return endingOffset;
    }

    /** Sends a heartbeat to keep the replication connection alive. */
    private void maybeUpdateHeartbeat(ReplicationStream stream) {
        long now = System.currentTimeMillis();
        long timeSinceLastHeartbeat = now - lastHeartbeatTime;

        if (timeSinceLastHeartbeat >= DEFAULT_HEARTBEAT_INTERVAL.toMillis()) {
            try {
                // Send heartbeat by flushing the last received LSN
                // This keeps the replication connection alive and updates the server
                io.debezium.connector.postgresql.connection.Lsn lastLsn = stream.lastReceivedLsn();
                if (lastLsn != null && lastLsn.isValid()) {
                    stream.flushLsn(lastLsn);
                    LOG.trace(
                            "Sent heartbeat for split: {} at LSN: {}",
                            streamSplit.splitId(),
                            lastLsn);
                }
                lastHeartbeatTime = now;
            } catch (Exception e) {
                LOG.warn("Failed to send heartbeat for split: {}", streamSplit.splitId(), e);
            }
        }
    }

    /** Updates the replication slot status to acknowledge processed messages. */
    private void maybeUpdateStatus(ReplicationStream stream) {
        long now = System.currentTimeMillis();
        long timeSinceLastUpdate = now - lastStatusUpdateTime;

        if (timeSinceLastUpdate >= DEFAULT_STATUS_UPDATE_INTERVAL_MS) {
            try {
                // Update replication slot status by flushing the last received LSN
                // This acknowledges to the server that we've processed messages up to this point
                io.debezium.connector.postgresql.connection.Lsn lastLsn = stream.lastReceivedLsn();
                if (lastLsn != null && lastLsn.isValid()) {
                    stream.flushLsn(lastLsn);
                    LOG.trace(
                            "Updated replication status for split: {} at LSN: {}",
                            streamSplit.splitId(),
                            lastLsn);
                }
                lastStatusUpdateTime = now;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to update replication status for split: {}",
                        streamSplit.splitId(),
                        e);
            }
        }
    }

    /** Checks if the stream should stop (for bounded streams). */
    private boolean shouldStop(GaussDBSourceFetchTaskContext context, ReplicationStream stream) {
        // For unbounded streams, never stop
        // For bounded streams, check if we've reached the end offset
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset endingOffset =
                (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset)
                        streamSplit.getEndingOffset();

        // Check if this is an unbounded stream
        if (endingOffset == null
                || org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset
                        .NO_STOPPING_OFFSET
                        .equals(endingOffset)) {
            return false;
        }

        // For bounded streams, check if we've reached or exceeded the end LSN
        io.debezium.connector.postgresql.connection.Lsn lastReceivedLsn = stream.lastReceivedLsn();
        if (lastReceivedLsn != null && lastReceivedLsn.isValid()) {
            io.debezium.connector.gaussdb.connection.Lsn currentLsn =
                    io.debezium.connector.gaussdb.connection.Lsn.valueOf(lastReceivedLsn.asLong());
            io.debezium.connector.gaussdb.connection.Lsn endLsn = endingOffset.getLsn();

            if (endLsn != null && endLsn.isValid()) {
                return currentLsn.compareTo(endLsn) >= 0;
            }
        }

        return false;
    }

    /** Closes the replication stream and releases resources. */
    private void closeReplicationStream() {
        if (replicationStream != null) {
            try {
                replicationStream.close();
                LOG.debug("Closed replication stream for split: {}", streamSplit.splitId());
            } catch (Exception e) {
                LOG.warn(
                        "Error closing replication stream for split: {}", streamSplit.splitId(), e);
            } finally {
                replicationStream = null;
            }
        }
    }

    private void closePgConnectionSupplier() {
        LazyPgConnectionSupplier supplier = pgConnectionSupplier;
        if (supplier != null) {
            try {
                supplier.close();
            } catch (Exception e) {
                LOG.debug(
                        "Failed to close pg connection supplier for split {}",
                        streamSplit.splitId(),
                        e);
            } finally {
                pgConnectionSupplier = null;
            }
        }
    }

    private static final class LazyPgConnectionSupplier
            implements io.debezium.connector.postgresql.PostgresStreamingChangeEventSource
                            .PgConnectionSupplier,
                    AutoCloseable {
        private final String url;
        private final String user;
        private final String password;
        private volatile BaseConnection connection;

        private LazyPgConnectionSupplier(GaussDBConnectorConfig config) {
            String host = config.getJdbcConfig().getHostname();
            int port = config.getJdbcConfig().getPort();
            String db = config.getJdbcConfig().getDatabase();
            this.url = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
            this.user = config.getJdbcConfig().getUser();
            this.password = config.getJdbcConfig().getPassword();
        }

        @Override
        public BaseConnection get() throws SQLException {
            BaseConnection existing = connection;
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
            synchronized (this) {
                existing = connection;
                if (existing != null && !existing.isClosed()) {
                    return existing;
                }
                java.sql.Connection created =
                        java.sql.DriverManager.getConnection(url, user, password);
                if (!(created instanceof BaseConnection)) {
                    created.close();
                    throw new SQLException(
                            "PostgreSQL JDBC driver must be used to provide BaseConnection for decoding");
                }
                connection = (BaseConnection) created;
                return connection;
            }
        }

        @Override
        public void close() throws Exception {
            BaseConnection c = connection;
            connection = null;
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Commits the current offset to the GaussDB replication slot. This is called when a checkpoint
     * is completed to acknowledge that all changes up to this offset have been processed.
     */
    public void commitCurrentOffset(
            org.apache.flink.cdc.connectors.base.source.meta.offset.Offset offset) {
        try {
            if (offset != null && replicationStream != null) {
                LOG.debug("Committing offset {} for split: {}", offset, streamSplit.splitId());

                // Convert the offset to GaussDB LSN
                org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset gaussDBOffset =
                        (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset)
                                offset;
                io.debezium.connector.gaussdb.connection.Lsn lsnToCommit = gaussDBOffset.getLsn();

                if (lsnToCommit != null && lsnToCommit.isValid()) {
                    // Convert GaussDB LSN to PostgreSQL LSN for the replication stream API
                    io.debezium.connector.postgresql.connection.Lsn pgLsn =
                            io.debezium.connector.postgresql.connection.Lsn.valueOf(
                                    lsnToCommit.asLong());

                    // Flush the LSN to the replication slot
                    // This acknowledges to GaussDB that we've successfully processed up to this
                    // point
                    replicationStream.flushLsn(pgLsn);

                    LOG.info(
                            "Successfully committed offset {} (LSN: {}) for split: {}",
                            offset,
                            lsnToCommit.asString(),
                            streamSplit.splitId());
                } else {
                    LOG.warn(
                            "Invalid LSN in offset {}, skipping commit for split: {}",
                            offset,
                            streamSplit.splitId());
                }
            } else if (offset != null) {
                LOG.debug(
                        "Replication stream not available, cannot commit offset {} for split: {}",
                        offset,
                        streamSplit.splitId());
            }
        } catch (Exception e) {
            // Log warning but don't throw - checkpoint commit failures should not fail the job
            LOG.warn("Failed to commit offset {} for split: {}", offset, streamSplit.splitId(), e);
        }
    }
}
