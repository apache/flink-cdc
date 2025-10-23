/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.coordinator;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.hudi.sink.event.CreateTableOperatorEvent;
import org.apache.flink.cdc.connectors.hudi.sink.event.EnhancedWriteMetadataEvent;
import org.apache.flink.cdc.connectors.hudi.sink.util.RowDataUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.CoordinationResponseSerDe;
import org.apache.hudi.sink.utils.EventBuffers;
import org.apache.hudi.sink.utils.ExplicitClassloaderThreadFactory;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A custom OperatorCoordinator that manages Hudi writes for multiple tables.
 *
 * <p>This coordinator extends the default {@link StreamWriteOperatorCoordinator}. The parent class
 * is designed for a single destination table, so its core logic (e.g., for commits and
 * checkpointing) cannot be reused directly for a multi-table sink.
 *
 * <p>Therefore, this implementation overrides the essential lifecycle methods to manage a
 * collection of per-table resources. It dynamically creates and manages a dedicated {@link
 * HoodieFlinkWriteClient}, {@link EventBuffers}, and timeline for each table that appears in the
 * upstream CDC data.
 */
public class MultiTableStreamWriteOperatorCoordinator extends StreamWriteOperatorCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTableStreamWriteOperatorCoordinator.class);

    /**
     * A custom coordination request that includes the TableId to request an instant for a specific
     * table.
     */
    public static class MultiTableInstantTimeRequest implements CoordinationRequest, Serializable {
        private static final long serialVersionUID = 1L;
        private final long checkpointId;
        private final TableId tableId;

        public MultiTableInstantTimeRequest(long checkpointId, TableId tableId) {
            this.checkpointId = checkpointId;
            this.tableId = tableId;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public TableId getTableId() {
            return tableId;
        }
    }

    /**
     * Encapsulates all state and resources for a single table. This simplifies management by
     * grouping related objects, making the coordinator logic cleaner and less prone to errors.
     */
    private static class TableContext implements Serializable {
        private static final long serialVersionUID = 1L;

        final transient HoodieFlinkWriteClient<?> writeClient;
        final EventBuffers eventBuffers;
        final TableState tableState;
        final String tablePath;

        TableContext(
                HoodieFlinkWriteClient<?> writeClient,
                EventBuffers eventBuffers,
                TableState tableState,
                String tablePath) {
            this.writeClient = writeClient;
            this.eventBuffers = eventBuffers;
            this.tableState = tableState;
            this.tablePath = tablePath;
        }

        void close() {
            if (writeClient != null) {
                try {
                    writeClient.close();
                } catch (Exception e) {
                    LOG.error("Error closing write client for table path: {}", tablePath, e);
                }
            }
        }
    }

    /** A container for table-specific configuration and state. */
    private static class TableState implements Serializable {
        private static final long serialVersionUID = 1L;
        final String commitAction;
        final boolean isOverwrite;
        final WriteOperationType operationType;

        TableState(Configuration conf) {
            this.operationType =
                    WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
            this.commitAction =
                    CommitUtils.getCommitActionType(
                            this.operationType,
                            HoodieTableType.valueOf(
                                    conf.getString(FlinkOptions.TABLE_TYPE).toUpperCase()));
            this.isOverwrite = WriteOperationType.isOverwrite(this.operationType);
        }
    }

    /** The base Flink configuration. */
    private final Configuration baseConfig;

    /**
     * A single, unified map holding the context for each managed table. The key is the {@link
     * TableId}, providing a centralized place for all per-table resources.
     */
    private final Map<TableId, TableContext> tableContexts = new ConcurrentHashMap<>();

    /** A reverse lookup map from table path to TableId for efficient event routing. */
    private final Map<String, TableId> pathToTableId = new ConcurrentHashMap<>();

    /** Cache of schemas per table for config creation. */
    private final Map<TableId, Schema> tableSchemas = new ConcurrentHashMap<>();

    /**
     * Gateways for sending events to sub-tasks. This field is necessary because the parent's
     * `gateways` array is private and not initialized if we don't call super.start().
     */
    private transient SubtaskGateway[] gateways;

    /**
     * A dedicated write client whose only job is to run the embedded timeline server. This ensures
     * there is only one timeline server for the entire job.
     */
    private transient HoodieFlinkWriteClient<?> timelineServerClient;

    /** A single-thread executor to handle instant time requests, mimicking the parent behavior. */
    private transient NonThrownExecutor instantRequestExecutor;

    public MultiTableStreamWriteOperatorCoordinator(Configuration conf, Context context) {
        super(conf, context);
        conf.setString("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        this.baseConfig = conf;
        LOG.info(
                "MultiTableStreamWriteOperatorCoordinator initialized for operator: {} with config: {}",
                context.getOperatorId(),
                baseConfig);
    }

    @Override
    public void start() throws Exception {
        // Hadoop's FileSystem API uses Java's ServiceLoader to find implementations for
        // URI schemes (like 'file://'). The ServiceLoader relies on the thread's context
        // classloader. The parent class sets this, but our overridden start() method must
        // do so as well to ensure file system implementations can be found.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        // Initialize the executor service, which is a protected field in the parent class.
        // This logic is borrowed from the parent's start() method as we cannot call super.start().
        this.executor =
                NonThrownExecutor.builder(LOG)
                        .threadFactory(
                                new ExplicitClassloaderThreadFactory(
                                        "multi-table-coord-event-handler",
                                        context.getUserCodeClassloader()))
                        .exceptionHook(
                                (errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
                        .waitForTasksFinish(true)
                        .build();

        // Executor for handling instant requests.
        this.instantRequestExecutor =
                NonThrownExecutor.builder(LOG)
                        .threadFactory(
                                new ExplicitClassloaderThreadFactory(
                                        "multi-table-instant-request",
                                        context.getUserCodeClassloader()))
                        .exceptionHook(
                                (errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
                        .build();

        // Initialize the gateways array to avoid NullPointerException when subtasks are ready.
        this.gateways = new SubtaskGateway[context.currentParallelism()];

        // Initialize a single write client for the coordinator path.
        // Its primary role is to start and manage the embedded timeline server.
        try {
            // The baseConfig points to the dummy coordinator path.
            // A .hoodie directory is required for the timeline server to start.
            StreamerUtil.initTableIfNotExists(this.baseConfig);
            this.timelineServerClient = FlinkWriteClients.createWriteClient(this.baseConfig);
            LOG.info("Successfully started timeline server on coordinator.");
        } catch (Exception e) {
            LOG.error("Failed to start timeline server on coordinator.", e);
            context.failJob(e);
            return;
        }

        // Re-initialize transient fields after deserialization from a Flink checkpoint.
        // When the coordinator is restored, the `tableContexts` map is deserialized, but all
        // `writeClient` fields within it will be null because they are transient.
        for (Map.Entry<TableId, TableContext> entry : tableContexts.entrySet()) {
            TableId tableId = entry.getKey();
            TableContext oldContext = entry.getValue();

            try {
                Configuration tableConfig = createTableSpecificConfig(tableId);
                // Ensure the table's filesystem structure exists before creating a client.
                StreamerUtil.initTableIfNotExists(tableConfig);
                HoodieFlinkWriteClient<?> writeClient =
                        FlinkWriteClients.createWriteClient(tableConfig);

                // Replace the old context (with a null client) with a new one containing the live
                // client.
                tableContexts.put(
                        tableId,
                        new TableContext(
                                writeClient,
                                oldContext.eventBuffers,
                                oldContext.tableState,
                                oldContext.tablePath));
                LOG.info(
                        "Successfully re-initialized write client for recovered table: {}",
                        tableId);
            } catch (Exception e) {
                LOG.error(
                        "Failed to re-initialize write client for recovered table: {}", tableId, e);
                context.failJob(e);
                return; // Exit if initialization fails for any table
            }
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        if (request instanceof MultiTableInstantTimeRequest) {
            CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
            instantRequestExecutor.execute(
                    () -> {
                        MultiTableInstantTimeRequest instantRequest =
                                (MultiTableInstantTimeRequest) request;
                        TableId tableId = instantRequest.getTableId();
                        long checkpointId = instantRequest.getCheckpointId();

                        TableContext tableContext = tableContexts.get(tableId);
                        if (tableContext == null) {
                            String errorMsg =
                                    String.format(
                                            "Received instant request for unknown table %s. The sink function should send a CreateTableEvent first.",
                                            tableId);
                            LOG.error(errorMsg);
                            future.completeExceptionally(new IllegalStateException(errorMsg));
                            return;
                        }

                        Pair<String, WriteMetadataEvent[]> instantAndBuffer =
                                tableContext.eventBuffers.getInstantAndEventBuffer(checkpointId);
                        final String instantTime;

                        if (instantAndBuffer == null) {
                            // No instant yet for this checkpoint, create a new one.
                            instantTime = startInstantForTable(tableContext);
                            tableContext.eventBuffers.initNewEventBuffer(
                                    checkpointId, instantTime, context.currentParallelism());
                            LOG.info(
                                    "Created new instant [{}] for table [{}] at checkpoint [{}].",
                                    instantTime,
                                    tableId,
                                    checkpointId);
                        } else {
                            // Instant already exists for this checkpoint, reuse it.
                            instantTime = instantAndBuffer.getLeft();
                            LOG.info(
                                    "Reusing instant [{}] for table [{}] at checkpoint [{}].",
                                    instantTime,
                                    tableId,
                                    checkpointId);
                        }
                        future.complete(
                                CoordinationResponseSerDe.wrap(
                                        Correspondent.InstantTimeResponse.getInstance(
                                                instantTime)));
                    },
                    "handling instant time request for checkpoint %d",
                    ((MultiTableInstantTimeRequest) request).getCheckpointId());
            return future;
        } else {
            LOG.warn("Received an unknown coordination request: {}", request.getClass().getName());
            return super.handleCoordinationRequest(request);
        }
    }

    private String startInstantForTable(TableContext tableContext) {
        HoodieFlinkWriteClient<?> writeClient = tableContext.writeClient;
        TableState tableState = tableContext.tableState;
        HoodieTableMetaClient metaClient = writeClient.getHoodieTable().getMetaClient();

        metaClient.reloadActiveTimeline();
        final String newInstant = writeClient.startCommit(tableState.commitAction, metaClient);
        metaClient
                .getActiveTimeline()
                .transitionRequestedToInflight(tableState.commitAction, newInstant);
        return newInstant;
    }

    @Override
    public void handleEventFromOperator(
            int subtask, int attemptNumber, OperatorEvent operatorEvent) {
        executor.execute(
                () -> {
                    if (operatorEvent instanceof CreateTableOperatorEvent) {
                        handleCreateTableEvent((CreateTableOperatorEvent) operatorEvent);
                    } else if (operatorEvent instanceof EnhancedWriteMetadataEvent) {
                        handleEnhancedWriteMetadataEvent(
                                (EnhancedWriteMetadataEvent) operatorEvent);
                    } else {
                        LOG.warn(
                                "Received an unhandled or non-enhanced OperatorEvent: {}",
                                operatorEvent);
                    }
                },
                "handling operator event %s",
                operatorEvent);
    }

    private void handleCreateTableEvent(CreateTableOperatorEvent createTableOperatorEvent) {
        CreateTableEvent event = createTableOperatorEvent.getCreateTableEvent();
        TableId tableId = event.tableId();

        // Store the schema for this table
        tableSchemas.put(tableId, event.getSchema());
        LOG.info(
                "Cached schema for table {}: {} columns",
                tableId,
                event.getSchema().getColumnCount());

        tableContexts.computeIfAbsent(
                tableId,
                tId -> {
                    LOG.info("New table detected: {}. Initializing Hudi resources.", tId);
                    try {
                        Configuration tableConfig = createTableSpecificConfig(tId);
                        String tablePath = tableConfig.getString(FlinkOptions.PATH);
                        pathToTableId.put(tablePath, tId);

                        StreamerUtil.initTableIfNotExists(tableConfig);
                        HoodieFlinkWriteClient<?> writeClient =
                                FlinkWriteClients.createWriteClient(tableConfig);
                        TableState tableState = new TableState(tableConfig);
                        EventBuffers eventBuffers = EventBuffers.getInstance(tableConfig);

                        LOG.info(
                                "Successfully initialized resources for table: {} at path: {}",
                                tId,
                                tablePath);
                        return new TableContext(writeClient, eventBuffers, tableState, tablePath);
                    } catch (Exception e) {
                        LOG.error("Failed to initialize Hudi table resources for: {}", tId, e);
                        context.failJob(
                                new HoodieException(
                                        "Failed to initialize Hudi writer for table " + tId, e));
                        return null;
                    }
                });
    }

    private void handleEnhancedWriteMetadataEvent(EnhancedWriteMetadataEvent enhancedEvent) {
        String tablePath = enhancedEvent.getTablePath();
        WriteMetadataEvent event = enhancedEvent.getOriginalEvent();
        TableId tableId = pathToTableId.get(tablePath);

        if (tableId == null) {
            LOG.warn("No tableId found for path: {}. Cannot process event.", tablePath);
            return;
        }

        TableContext tableContext = tableContexts.get(tableId);
        if (tableContext == null) {
            LOG.error("FATAL: Inconsistent state. No TableContext for table: {}.", tableId);
            context.failJob(new IllegalStateException("No TableContext for table " + tableId));
            return;
        }
        // The instant should have been created by handleCoordinationRequest
        if (tableContext.eventBuffers.getInstantAndEventBuffer(event.getCheckpointId()) == null) {
            LOG.error(
                    "FATAL: Received WriteMetadataEvent for table {} at checkpoint {} before an instant was created. "
                            + "This should not happen. The sink function must request an instant before sending data.",
                    tableId,
                    event.getCheckpointId());
            context.failJob(
                    new IllegalStateException(
                            "Received data for table "
                                    + tableId
                                    + " at checkpoint "
                                    + event.getCheckpointId()
                                    + " without a valid Hudi instant."));
            return;
        }

        LOG.debug(
                "Buffering event for table: {}, checkpoint: {}", tableId, event.getCheckpointId());
        tableContext.eventBuffers.addEventToBuffer(event);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        executor.execute(
                () -> {
                    try {
                        Map<TableId, Map<Long, Pair<String, WriteMetadataEvent[]>>> allStates =
                                new HashMap<>();
                        tableContexts.forEach(
                                (tableId, tableContext) -> {
                                    allStates.put(
                                            tableId,
                                            tableContext.eventBuffers.getAllCompletedEvents());
                                });

                        // Create a wrapper that includes both event buffers AND schemas
                        Map<String, Object> checkpointState = new HashMap<>();
                        checkpointState.put("eventBuffers", allStates);
                        checkpointState.put("schemas", new HashMap<>(tableSchemas));

                        byte[] serializedState = SerializationUtils.serialize(checkpointState);
                        result.complete(serializedState);
                        LOG.info(
                                "Successfully checkpointed coordinator state with {} schemas for checkpoint {}",
                                tableSchemas.size(),
                                checkpointId);
                    } catch (Throwable t) {
                        LOG.error(
                                "Failed to checkpoint coordinator state for checkpoint {}",
                                checkpointId,
                                t);
                        result.completeExceptionally(t);
                    }
                },
                "checkpointing coordinator state %d",
                checkpointId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        if (checkpointData == null) {
            LOG.info("No coordinator checkpoint data to restore for checkpoint {}.", checkpointId);
            return;
        }
        try {
            Map<String, Object> checkpointState = SerializationUtils.deserialize(checkpointData);
            Map<TableId, Map<Long, Pair<String, WriteMetadataEvent[]>>> allStates =
                    (Map<TableId, Map<Long, Pair<String, WriteMetadataEvent[]>>>)
                            checkpointState.get("eventBuffers");
            Map<TableId, Schema> restoredSchemas =
                    (Map<TableId, Schema>) checkpointState.get("schemas");

            // Restore schemas
            if (restoredSchemas != null && !restoredSchemas.isEmpty()) {
                tableSchemas.clear();
                tableSchemas.putAll(restoredSchemas);
                LOG.info(
                        "Restored {} schemas from checkpoint: {}",
                        tableSchemas.size(),
                        tableSchemas.keySet());
            }

            allStates.forEach(
                    (tableId, completedEvents) -> {
                        // Lazily create table context if it doesn't exist.
                        // The actual write client is initialized in start().
                        tableContexts.computeIfAbsent(
                                tableId,
                                tId -> {
                                    Configuration tableConfig = createTableSpecificConfig(tId);
                                    String tablePath = tableConfig.getString(FlinkOptions.PATH);
                                    pathToTableId.put(tablePath, tId);
                                    TableState tableState = new TableState(tableConfig);
                                    EventBuffers eventBuffers =
                                            EventBuffers.getInstance(tableConfig);
                                    return new TableContext(
                                            null, eventBuffers, tableState, tablePath);
                                });
                        TableContext tableContext = tableContexts.get(tableId);
                        tableContext.eventBuffers.addEventsToBuffer(completedEvents);
                    });
            LOG.info("Successfully restored coordinator state from checkpoint {}", checkpointId);
        } catch (Throwable t) {
            LOG.error("Failed to restore coordinator state from checkpoint {}", checkpointId, t);
            context.failJob(new RuntimeException("Failed to restore coordinator state", t));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        executor.execute(
                () -> {
                    LOG.info(
                            "Checkpoint {} completed. Committing instants for all managed tables.",
                            checkpointId);
                    for (Map.Entry<TableId, TableContext> entry : tableContexts.entrySet()) {
                        TableId tableId = entry.getKey();
                        TableContext tableContext = entry.getValue();

                        tableContext
                                .eventBuffers
                                .getEventBufferStream()
                                .filter(e -> e.getKey() < checkpointId)
                                .forEach(
                                        bufferEntry -> {
                                            long ckpId = bufferEntry.getKey();
                                            String instant = bufferEntry.getValue().getLeft();
                                            WriteMetadataEvent[] events =
                                                    bufferEntry.getValue().getRight();
                                            try {
                                                commitInstantForTable(
                                                        tableId,
                                                        tableContext,
                                                        ckpId,
                                                        instant,
                                                        events);
                                            } catch (Exception e) {
                                                LOG.error(
                                                        "Exception while committing instant {} for table {}",
                                                        instant,
                                                        tableId,
                                                        e);
                                                MultiTableStreamWriteOperatorCoordinator.this
                                                        .context.failJob(e);
                                            }
                                        });
                    }
                },
                "committing instants for checkpoint %d",
                checkpointId);
    }

    private void commitInstantForTable(
            TableId tableId,
            TableContext tableContext,
            long checkpointId,
            String instant,
            WriteMetadataEvent[] eventBuffer) {

        if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
            LOG.info("No events for instant {}, table {}. Resetting buffer.", instant, tableId);
            tableContext.eventBuffers.reset(checkpointId);
            // Even with no events, we must clean up the inflight instant.
            // A simple rollback handles this.
            tableContext.writeClient.rollback(instant);
            return;
        }

        List<WriteStatus> writeStatuses =
                Arrays.stream(eventBuffer)
                        .filter(Objects::nonNull)
                        .map(WriteMetadataEvent::getWriteStatuses)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        if (writeStatuses.isEmpty() && !OptionsResolver.allowCommitOnEmptyBatch(baseConfig)) {
            LOG.info(
                    "No data written for instant {}, table {}. Aborting commit and rolling back.",
                    instant,
                    tableId);
            tableContext.eventBuffers.reset(checkpointId);
            tableContext.writeClient.rollback(instant);
            return;
        }

        doCommit(tableId, tableContext, checkpointId, instant, writeStatuses);
    }

    @SuppressWarnings("unchecked")
    private void doCommit(
            TableId tableId,
            TableContext tableContext,
            long checkpointId,
            String instant,
            List<WriteStatus> writeStatuses) {

        TableState state = tableContext.tableState;
        final Map<String, List<String>> partitionToReplacedFileIds =
                state.isOverwrite
                        ? tableContext.writeClient.getPartitionToReplacedFileIds(
                                state.operationType, writeStatuses)
                        : Collections.emptyMap();

        HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
        StreamerUtil.addFlinkCheckpointIdIntoMetaData(
                baseConfig, checkpointCommitMetadata, checkpointId);

        boolean success =
                tableContext.writeClient.commit(
                        instant,
                        writeStatuses,
                        Option.of(checkpointCommitMetadata),
                        state.commitAction,
                        partitionToReplacedFileIds);

        if (success) {
            tableContext.eventBuffers.reset(checkpointId);
            LOG.info("Successfully committed instant [{}] for table [{}]", instant, tableId);
        } else {
            LOG.error("Failed to commit instant [{}] for table [{}]", instant, tableId);
            MultiTableStreamWriteOperatorCoordinator.this.context.failJob(
                    new HoodieException(
                            String.format(
                                    "Commit failed for instant %s, table %s", instant, tableId)));
        }
    }

    @Override
    public void close() throws Exception {
        if (timelineServerClient != null) {
            timelineServerClient.close();
        }
        if (instantRequestExecutor != null) {
            instantRequestExecutor.close();
        }
        tableContexts.values().forEach(TableContext::close);
        tableContexts.clear();
        pathToTableId.clear();
        super.close();
        LOG.info("MultiTableStreamWriteOperatorCoordinator closed.");
    }

    @Override
    public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
        // Since the parent's `gateways` field is private, we must manage our own.
        if (this.gateways == null) {
            this.gateways = new SubtaskGateway[context.currentParallelism()];
        }
        this.gateways[i] = subtaskGateway;
    }

    // --- Helper Methods ---
    private Configuration createTableSpecificConfig(TableId tableId) {
        Configuration tableConfig = new Configuration(baseConfig);
        String coordinatorPath = baseConfig.getString(FlinkOptions.PATH);
        // Use the same logic as MultiTableEventStreamWriteFunction to strip "/coordinator"
        String rootPath = coordinatorPath.split("/coordinator")[0];
        String tablePath =
                String.format(
                        "%s/%s/%s", rootPath, tableId.getSchemaName(), tableId.getTableName());
        tableConfig.setString(FlinkOptions.PATH, tablePath);
        tableConfig.setString(FlinkOptions.TABLE_NAME, tableId.getTableName());
        tableConfig.setString(FlinkOptions.DATABASE_NAME, tableId.getSchemaName());

        // Set the table-specific schema from the cached schemas
        Schema cdcSchema = tableSchemas.get(tableId);
        if (cdcSchema != null) {
            RowType rowType = RowDataUtils.toRowType(cdcSchema);
            String tableAvroSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
            tableConfig.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, tableAvroSchema);
            LOG.info(
                    "Set schema for table {} in coordinator config: {} business fields",
                    tableId,
                    rowType.getFieldCount());
        } else {
            LOG.warn(
                    "No schema found in cache for table {}. WriteClient may use incorrect schema!",
                    tableId);
        }

        // Disable both embedded timeline server and metadata table for per-table clients.
        // The central coordinator manages the only timeline server.
        tableConfig.setBoolean(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), false);

        // Use memory-based file system view since each client is lightweight.
        tableConfig.setString(FileSystemViewStorageConfig.VIEW_TYPE.key(), "MEMORY");

        return tableConfig;
    }

    /** Provider for {@link MultiTableStreamWriteOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {
        private final OperatorID operatorId;
        private final Configuration conf;

        public Provider(OperatorID operatorId, Configuration conf) {
            this.operatorId = operatorId;
            this.conf = conf;
        }

        @Override
        public OperatorID getOperatorId() {
            return this.operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new MultiTableStreamWriteOperatorCoordinator(this.conf, context);
        }
    }
}
