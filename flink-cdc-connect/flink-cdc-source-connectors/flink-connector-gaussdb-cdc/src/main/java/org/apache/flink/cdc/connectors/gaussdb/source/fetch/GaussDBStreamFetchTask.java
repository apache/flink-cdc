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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A fetch task for reading streaming changes from GaussDB using logical
 * replication.
 */
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

        // Determine if this is a bounded read (backfill) or unbounded read (continuous
        // streaming)
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset endOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) streamSplit
                .getEndingOffset();
        boolean isBoundedRead =
                endOffset != null
                        && endOffset.getLsn() != null
                        && endOffset.getLsn().isValid()
                        && !endOffset.getLsn()
                                .equals(io.debezium.connector.gaussdb.connection.Lsn.NO_STOPPING_LSN);

        // Get the appropriate replication connection from the context
        // Use backfill connection for bounded reads to avoid slot contention
        io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection replicationConnection;
        if (isBoundedRead && context.getBackfillReplicationConnection() != null) {
            replicationConnection = context.getBackfillReplicationConnection();
            LOG.info("Using backfill replication connection for bounded read");
        } else {
            replicationConnection = context.getReplicationConnection();
            LOG.info("Using main replication connection for unbounded read");
        }

        if (replicationConnection == null) {
            throw new SQLException("Replication connection is not initialized in context");
        }

        // Get the starting LSN from the stream split
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset startOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) streamSplit
                .getStartingOffset();
        io.debezium.connector.gaussdb.connection.Lsn gaussdbLsn = startOffset != null ? startOffset.getLsn() : null;

        // Convert GaussDB LSN to PostgreSQL LSN for the replication API
        io.debezium.connector.postgresql.connection.Lsn startLsn = overrideStartLsn;
        if (startLsn == null && gaussdbLsn != null && gaussdbLsn.isValid()) {
            startLsn = io.debezium.connector.postgresql.connection.Lsn.valueOf(gaussdbLsn.asLong());
        }

        // Start streaming from the specified LSN
        try {
            // Set the ending position for bounded reads to avoid stream close/reopen
            // This enables the reachEnd() check in ReplicationStream to return NoopMessage
            // when target LSN is reached, instead of requiring stream restart
            if (endOffset != null && endOffset.getLsn() != null && endOffset.getLsn().isValid()) {
                io.debezium.connector.gaussdb.connection.Lsn endingLsn = endOffset.getLsn();
                LOG.info("Setting ending position for bounded read: {}", endingLsn);
                replicationConnection.setEndingPos(
                        io.debezium.connector.postgresql.connection.Lsn.valueOf(endingLsn.asLong()));
            } else {
                // Clear ending position for unbounded reads
                replicationConnection.setEndingPos(null);
            }

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
        final AtomicReference<io.debezium.connector.postgresql.connection.Lsn> lastProcessedLsn = new AtomicReference<>(
                stream.lastReceivedLsn());

        final ChangeEventQueue<DataChangeEvent> queue = requireQueue(context);
        final GaussDBConnectorConfig connectorConfig = context.getDbzConnectorConfig();
        final GaussDBSchema gaussDBSchema = new GaussDBSchema(connectorConfig, context.getConnection());
        this.pgConnectionSupplier = new LazyPgConnectionSupplier(connectorConfig);

        // Create a message processor that will be called for each message
        io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor processor = (
                message) -> {
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

                    final io.debezium.connector.postgresql.connection.Lsn resumeLsn = lastProcessedLsn.get();
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

        final boolean traceDataFlow = operation == ReplicationMessage.Operation.INSERT
                || operation == ReplicationMessage.Operation.UPDATE;
        TableId tableId = parseTableId(message.getTable());
        if (traceDataFlow) {
            LOG.info(
                    "TableId before catalog补全: catalog='{}', schema='{}', table='{}'",
                    tableId != null ? tableId.catalog() : null,
                    tableId != null ? tableId.schema() : null,
                    tableId != null ? tableId.table() : null);
        }
        if (tableId != null && Strings.isNullOrEmpty(tableId.catalog())) {
            String database = context.getDbzConnectorConfig().getJdbcConfig().getDatabase();
            if (traceDataFlow) {
                LOG.info("Database from config: '{}'", database);
            }
            if (!Strings.isNullOrEmpty(database)) {
                tableId = new TableId(database, tableId.schema(), tableId.table());
                if (traceDataFlow) {
                    LOG.info(
                            "TableId after catalog补全: catalog='{}', schema='{}', table='{}'",
                            tableId.catalog(),
                            tableId.schema(),
                            tableId.table());
                }
            }
        }
        final TableId messageTableId = tableId;
        if (traceDataFlow) {
            LOG.info(
                    "Raw replication message op={} table={} newTuple={} oldTuple={}",
                    operation,
                    tableId,
                    message.getNewTupleList(),
                    message.getOldTupleList());
        }
        if (tableId == null) {
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        boolean included = context.getTableFilter().isIncluded(tableId);
        if (traceDataFlow) {
            LOG.info("Table filter check: tableId={}, included={}", tableId, included);
            if (!included) {
                LOG.info("Skip message due to table filter exclusion: {}", tableId);
            }
        }
        if (!included) {
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }

        if (traceDataFlow) {
            io.debezium.connector.postgresql.connection.Lsn currentLsn = null;
            try {
                currentLsn = streamRef.get() != null ? streamRef.get().lastReceivedLsn() : null;
            } catch (Exception ignored) {
            }
            LOG.info(
                    "Processing replication message op={} table={} lsn={}",
                    operation,
                    tableId,
                    currentLsn);
        }

        TableSchema tableSchema = gaussDBSchema.schemaFor(tableId);
        Table table = gaussDBSchema.tableFor(tableId);
        if ((tableSchema == null || table == null) && messageTableId != null
                && !messageTableId.equals(tableId)) {
            tableSchema = gaussDBSchema.schemaFor(messageTableId);
            table = gaussDBSchema.tableFor(messageTableId);
            if (traceDataFlow && tableSchema != null && table != null) {
                LOG.info(
                        "Recovered table schema using message TableId {} after filter remap {}",
                        messageTableId,
                        tableId);
            }
        }
        if (tableSchema == null || table == null) {
            LOG.info("Skip message for unknown/untracked table: {}", tableId);
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            return;
        }
        if (traceDataFlow) {
            LOG.info(
                    "Resolved table schema for {} columns={} rowSizeHint={}",
                    tableId,
                    table.columns().stream()
                            .filter(Objects::nonNull)
                            .map(io.debezium.relational.Column::name)
                            .collect(Collectors.toList()),
                    maxColumnPosition(table));
        }

        final int rowSize = maxColumnPosition(table);
        final Object[] afterRow = rowSize > 0 ? new Object[rowSize] : new Object[0];
        final Object[] beforeRow = rowSize > 0 ? new Object[rowSize] : new Object[0];

        try {
            // Use null supplier to avoid connection issues in replication stream context
            // The replication message already contains decoded values, no need for additional connection
            final io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier pgSupplier = () -> null;
            final boolean includeUnknownDatatypes = true;

            if (operation == ReplicationMessage.Operation.INSERT
                    || operation == ReplicationMessage.Operation.UPDATE) {
                if (traceDataFlow) {
                    LOG.info(
                            "Before fillRowFromTuple (afterRow) op={} table={} data={}",
                            operation,
                            tableId,
                            Arrays.toString(afterRow));
                }
                fillRowFromTuple(
                        table,
                        afterRow,
                        message.getNewTupleList(),
                        pgSupplier,
                        includeUnknownDatatypes,
                        traceDataFlow,
                        "after",
                        tableId,
                        operation);
                if (traceDataFlow) {
                    LOG.info(
                            "After fillRowFromTuple (afterRow) op={} table={} data={}",
                            operation,
                            tableId,
                            Arrays.toString(afterRow));
                }
            }
            if (operation == ReplicationMessage.Operation.DELETE
                    || operation == ReplicationMessage.Operation.UPDATE) {
                if (traceDataFlow) {
                    LOG.info(
                            "Before fillRowFromTuple (beforeRow) op={} table={} data={}",
                            operation,
                            tableId,
                            Arrays.toString(beforeRow));
                }
                fillRowFromTuple(
                        table,
                        beforeRow,
                        message.getOldTupleList(),
                        pgSupplier,
                        includeUnknownDatatypes,
                        traceDataFlow,
                        "before",
                        tableId,
                        operation);
                // When old tuple only contains keys, valueFromColumnData will produce a Struct
                // with
                // nulls for non-key columns. This keeps UPDATE/DELETE records deserializable.
                if (traceDataFlow) {
                    LOG.info(
                            "After fillRowFromTuple (beforeRow) op={} table={} data={}",
                            operation,
                            tableId,
                            Arrays.toString(beforeRow));
                }
            }
            if (operation == ReplicationMessage.Operation.DELETE
                    && (isAllNullRow(beforeRow) && !isAllNullRow(afterRow))) {
                // Fallback: some decoders provide DELETE values in new tuple.
                System.arraycopy(afterRow, 0, beforeRow, 0, afterRow.length);
                if (traceDataFlow) {
                    LOG.info("DELETE fallback copied afterRow into beforeRow for {}", tableId);
                }
            }
            if (operation == ReplicationMessage.Operation.UPDATE && isAllNullRow(beforeRow)) {
                // Fallback: if old tuple is missing, emit a best-effort before image from
                // after.
                System.arraycopy(afterRow, 0, beforeRow, 0, afterRow.length);
                if (traceDataFlow) {
                    LOG.info("UPDATE fallback copied afterRow into beforeRow for {}", tableId);
                }
            }

            final Struct keyStruct = !isAllNullRow(afterRow)
                    ? tableSchema.keyFromColumnData(afterRow)
                    : tableSchema.keyFromColumnData(beforeRow);
            final Struct afterStruct = !isAllNullRow(afterRow) ? tableSchema.valueFromColumnData(afterRow) : null;
            final Struct beforeStruct = !isAllNullRow(beforeRow) ? tableSchema.valueFromColumnData(beforeRow) : null;
            if (traceDataFlow) {
                LOG.info(
                        "Constructed keyStruct op={} table={} struct={}",
                        operation,
                        tableId,
                        keyStruct);
                LOG.info(
                        "Constructed beforeStruct op={} table={} struct={}",
                        operation,
                        tableId,
                        beforeStruct);
                LOG.info(
                        "Constructed afterStruct op={} table={} struct={}",
                        operation,
                        tableId,
                        afterStruct);
            }

            final io.debezium.connector.postgresql.connection.Lsn currentLsn = streamRef.get().lastReceivedLsn();
            final Map<String, Object> sourceOffset = lsnOffset(currentLsn);
            final Map<String, ?> sourcePartition = context.getPartition().getSourcePartition();
            final String topic = topicFor(context.getDbzConnectorConfig(), tableId);
            final String schemaName = tableId.schema();
            final String tableName = tableId.table();

            final Schema sourceSchema = Objects.requireNonNull(
                    context.getDbzConnectorConfig().getSourceInfoStructMaker().schema(),
                    "source info schema");
            final Struct sourceStruct = buildSourceStruct(
                    sourceSchema, context.getDbzConnectorConfig(), schemaName, tableName, message);

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

            final SourceRecord record = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topic,
                    tableSchema.keySchema(),
                    keyStruct,
                    envelope.schema(),
                    valueStruct);
            if (traceDataFlow) {
                LOG.info(
                        "Built envelope op={} before={} after={}",
                        operation,
                        beforeStruct,
                        afterStruct);
                LOG.info(
                        "Enqueuing SourceRecord op={} table={} topic={} partition={} offset={} key={} value={}",
                        operation,
                        tableId,
                        topic,
                        sourcePartition,
                        sourceOffset,
                        keyStruct,
                        valueStruct);
            }
            queue.enqueue(new DataChangeEvent(record));
            if (traceDataFlow) {
                LOG.info(
                        "SourceRecord enqueued op={} table={} offset={} emittedCount={}",
                        operation,
                        tableId,
                        sourceOffset,
                        emittedCount.get() + 1);
            }
            emittedCount.incrementAndGet();
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
        } catch (Exception e) {
            LOG.error("Failed to process message op={} table={}: {}", operation, tableId, e.getMessage(), e);
            updateLastProcessedLsn(streamRef, lastProcessedLsn);
            throw e;
        }
    }

    private static Struct buildSourceStruct(
            Schema sourceSchema,
            GaussDBConnectorConfig connectorConfig,
            String schemaName,
            String tableName,
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
                schemaName);
        putIfPresent(
                sourceStruct,
                io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY,
                tableName);

        final Instant commitTime = message.getCommitTime() != null ? message.getCommitTime() : Instant.now();
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
            io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier pgSupplier,
            boolean includeUnknownDatatypes,
            boolean logDetails,
            String tupleLabel,
            TableId tableId,
            ReplicationMessage.Operation operation)
            throws SQLException {
        if (tuple == null || tuple.isEmpty() || table == null || row == null) {
            if (logDetails) {
                LOG.info(
                        "fillRowFromTuple skipped due to empty input op={} table={} label={} tupleSize={}",
                        operation,
                        tableId,
                        tupleLabel,
                        tuple == null ? null : tuple.size());
            }
            return;
        }
        final java.util.List<io.debezium.relational.Column> tableColumns = table.columns();
        if (tableColumns == null || tableColumns.isEmpty()) {
            if (logDetails) {
                LOG.info(
                        "fillRowFromTuple skipped because table columns are empty op={} table={} label={}",
                        operation,
                        tableId,
                        tupleLabel);
            }
            return;
        }
        final Map<String, Integer> tableColumnIndex = new HashMap<>();
        for (io.debezium.relational.Column c : tableColumns) {
            if (c == null) {
                continue;
            }
            final int position = c.position();
            if (position <= 0) {
                continue;
            }
            final int index = position - 1;
            final String normalizedTableName = normalizeColumnName(c.name());
            if (!Strings.isNullOrEmpty(normalizedTableName)) {
                tableColumnIndex.put(normalizedTableName.toLowerCase(Locale.ROOT), index);
            }
            tableColumnIndex.put(c.name().toLowerCase(Locale.ROOT), index);
        }
        int mappedCount = 0;
        final java.util.List<String> mapped = logDetails ? new java.util.ArrayList<>() : null;
        final java.util.List<String> skipped = logDetails ? new java.util.ArrayList<>() : null;
        for (ReplicationMessage.Column column : tuple) {
            if (column == null) {
                continue;
            }
            final String rawName = column.getName();
            final String normalizedName = normalizeColumnName(rawName);
            final String lookupKey =
                    !Strings.isNullOrEmpty(normalizedName)
                            ? normalizedName.toLowerCase(Locale.ROOT)
                            : null;
            Integer index = lookupKey != null ? tableColumnIndex.get(lookupKey) : null;
            if (index == null && rawName != null) {
                index = tableColumnIndex.get(rawName.toLowerCase(Locale.ROOT));
            }
            if (index == null || index < 0 || index >= row.length) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Skip column mapping for '{}' (normalized='{}') on table {} with row size {}",
                            rawName,
                            normalizedName,
                            table.id(),
                            row.length);
                }
                if (logDetails && skipped != null) {
                    skipped.add(rawName + "->" + index);
                }
                continue;
            }
            final Object value = column.getValue(pgSupplier, includeUnknownDatatypes);
            row[index] = value;
            mappedCount++;
            if (logDetails && mapped != null) {
                mapped.add(rawName + "=>" + index + ":" + value);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Mapped column '{}' (normalized='{}') to index {} with value {}",
                        rawName,
                        normalizedName,
                        index,
                        value);
            }
        }
        if (logDetails) {
            LOG.info(
                    "fillRowFromTuple summary op={} table={} label={} mapped={} skipped={} finalRow={}",
                    operation,
                    tableId,
                    tupleLabel,
                    mapped != null ? mapped : mappedCount,
                    skipped,
                    Arrays.toString(row));
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

    private static String normalizeColumnName(String columnName) {
        if (Strings.isNullOrEmpty(columnName)) {
            return null;
        }
        String normalized = columnName.trim();
        final int lastDot = normalized.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < normalized.length() - 1) {
            normalized = normalized.substring(lastDot + 1);
        }
        if ((normalized.startsWith("\"") && normalized.endsWith("\""))
                || (normalized.startsWith("`") && normalized.endsWith("`"))
                || (normalized.startsWith("'") && normalized.endsWith("'"))) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return normalized;
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
            String trimmed = messageTable.trim();
            // GaussDB message formats: "schema"."table" or catalog."schema"."table"
            // Strip surrounding double quotes to align with table filters.
            String[] parts = trimmed.split("\\.");
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].replaceAll("^\"|\"$", "");
            }
            if (parts.length == 2) {
                return new TableId(null, parts[0], parts[1]);
            } else if (parts.length == 3) {
                return new TableId(parts[0], parts[1], parts[2]);
            } else {
                return new TableId(null, null, trimmed.replaceAll("^\"|\"$", ""));
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse table name: {}", messageTable, e);
            return null;
        }
    }

    private static String topicFor(GaussDBConnectorConfig config, TableId tableId) {
        final String prefix = config != null && !Strings.isNullOrEmpty(config.getLogicalName())
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
        final SourceRecord watermark = WatermarkEvent.create(
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
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset endingOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) streamSplit
                .getEndingOffset();
        io.debezium.connector.postgresql.connection.Lsn last = stream.lastReceivedLsn();
        if (last != null && last.isValid()) {
            GaussDBOffset current = new GaussDBOffset(
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
                // This acknowledges to the server that we've processed messages up to this
                // point
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
        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset endingOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) streamSplit
                .getEndingOffset();

        // Check if this is an unbounded stream
        if (endingOffset == null
                || org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset.NO_STOPPING_OFFSET
                        .equals(endingOffset)) {
            return false;
        }

        // For bounded streams, check if we've reached or exceeded the end LSN
        io.debezium.connector.postgresql.connection.Lsn lastReceivedLsn = stream.lastReceivedLsn();
        if (lastReceivedLsn != null && lastReceivedLsn.isValid()) {
            io.debezium.connector.gaussdb.connection.Lsn currentLsn = io.debezium.connector.gaussdb.connection.Lsn
                    .valueOf(lastReceivedLsn.asLong());
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
            implements io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier,
            AutoCloseable {
        private final String url;
        private final String user;
        private final String password;
        private volatile BaseConnection connection;

        private LazyPgConnectionSupplier(GaussDBConnectorConfig config) {
            String host = config.getJdbcConfig().getHostname();
            int port = config.getJdbcConfig().getPort() + 1; // Use HA port for replication
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
                java.sql.Connection created = java.sql.DriverManager.getConnection(url, user, password);
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
     * Commits the current offset to the GaussDB replication slot. This is called
     * when a checkpoint
     * is completed to acknowledge that all changes up to this offset have been
     * processed.
     */
    public void commitCurrentOffset(
            org.apache.flink.cdc.connectors.base.source.meta.offset.Offset offset) {
        try {
            if (offset != null && replicationStream != null) {
                LOG.debug("Committing offset {} for split: {}", offset, streamSplit.splitId());

                // Convert the offset to GaussDB LSN
                org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset gaussDBOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) offset;
                io.debezium.connector.gaussdb.connection.Lsn lsnToCommit = gaussDBOffset.getLsn();

                if (lsnToCommit != null && lsnToCommit.isValid()) {
                    // Convert GaussDB LSN to PostgreSQL LSN for the replication stream API
                    io.debezium.connector.postgresql.connection.Lsn pgLsn = io.debezium.connector.postgresql.connection.Lsn
                            .valueOf(
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
            // Log warning but don't throw - checkpoint commit failures should not fail the
            // job
            LOG.warn("Failed to commit offset {} for split: {}", offset, streamSplit.splitId(), e);
        }
    }
}
