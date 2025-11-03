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

package org.apache.flink.cdc.connectors.hudi.sink.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.hudi.sink.event.CreateTableOperatorEvent;
import org.apache.flink.cdc.connectors.hudi.sink.event.EnhancedWriteMetadataEvent;
import org.apache.flink.cdc.connectors.hudi.sink.event.HudiRecordEventSerializer;
import org.apache.flink.cdc.connectors.hudi.sink.event.TableAwareCorrespondent;
import org.apache.flink.cdc.connectors.hudi.sink.util.RowDataUtils;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.ViewStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Multi-table wrapper function that routes events to table-specific EventBucketStreamWriteFunction
 * instances. This approach maintains table isolation by creating dedicated function instances per
 * table while keeping the core write functions single-table focused.
 */
public class MultiTableEventStreamWriteFunction extends AbstractStreamWriteFunction<Event>
        implements EventProcessorFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTableEventStreamWriteFunction.class);

    /** Table-specific write functions created dynamically when new tables are encountered. */
    private transient Map<TableId, EventBucketStreamWriteFunction> tableFunctions;

    /** Track tables that have been initialized to avoid duplicate initialization. */
    private transient Map<TableId, Boolean> initializedTables;

    /** Cache of schemas per table for RowType generation. */
    private transient Map<TableId, Schema> schemaMaps;

    /** Persistent state for schemas to survive checkpoints/savepoints. */
    private transient ListState<Tuple2<TableId, Schema>> schemaState;

    private transient Map<TableId, Configuration> tableConfigurations;

    /** Schema evolution client to communicate with SchemaOperator. */
    private transient SchemaEvolutionClient schemaEvolutionClient;

    /** Store the function initialization context for table functions. */
    private transient FunctionInitializationContext functionInitializationContext;

    public MultiTableEventStreamWriteFunction(Configuration config) {
        super(config);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        this.functionInitializationContext = context;

        // Initialize schema map before restoring state
        if (this.schemaMaps == null) {
            this.schemaMaps = new HashMap<>();
        }

        // Initialize schema state for persistence across checkpoints/savepoints
        // Using operator state since this is not a keyed stream
        @SuppressWarnings({"unchecked", "rawtypes"})
        TupleSerializer<Tuple2<TableId, Schema>> tupleSerializer =
                new TupleSerializer(
                        Tuple2.class,
                        new org.apache.flink.api.common.typeutils.TypeSerializer[] {
                            TableIdSerializer.INSTANCE, SchemaSerializer.INSTANCE
                        });
        ListStateDescriptor<Tuple2<TableId, Schema>> schemaStateDescriptor =
                new ListStateDescriptor<>("schemaState", tupleSerializer);
        this.schemaState = context.getOperatorStateStore().getUnionListState(schemaStateDescriptor);

        // Restore schemas from state if this is a restore operation
        if (context.isRestored()) {
            LOG.info("Restoring schemas from state");
            for (Tuple2<TableId, Schema> entry : schemaState.get()) {
                schemaMaps.put(entry.f0, entry.f1);
                LOG.info("Restored schema for table: {}", entry.f0);
            }
            LOG.info("Restored {} schemas from state", schemaMaps.size());
        }

        LOG.info("MultiTableEventStreamWriteFunction state initialized");
    }

    /**
     * Sets the SchemaEvolutionClient from the operator level since functions don't have direct
     * access to TaskOperatorEventGateway.
     */
    public void setSchemaEvolutionClient(SchemaEvolutionClient schemaEvolutionClient) {
        this.schemaEvolutionClient = schemaEvolutionClient;
        LOG.info("SchemaEvolutionClient set for MultiTableEventStreamWriteFunction");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.tableFunctions = new HashMap<>();
        this.initializedTables = new HashMap<>();
        // Don't reinitialize schemaMaps if it already has restored schemas from state
        if (this.schemaMaps == null) {
            this.schemaMaps = new HashMap<>();
        }
        this.tableConfigurations = new HashMap<>();
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<RowData> out) throws Exception {
        LOG.debug("Processing event of type: {}", event.getClass().getSimpleName());

        // Route event to appropriate handler based on type
        if (event instanceof DataChangeEvent) {
            processDataChange((DataChangeEvent) event);
        } else if (event instanceof SchemaChangeEvent) {
            processSchemaChange((SchemaChangeEvent) event);
        } else if (event instanceof FlushEvent) {
            processFlush((FlushEvent) event);
        } else {
            LOG.warn("Received unknown event type: {}", event.getClass().getName());
        }
    }

    /**
     * Processes schema events. For a {@link CreateTableEvent}, it ensures that the coordinator is
     * notified and the physical Hudi table is created. For a {@link SchemaChangeEvent}, it updates
     * the local schema cache.
     *
     * <p>Implements {@link EventProcessorFunction#processSchemaChange(SchemaChangeEvent)}.
     */
    @Override
    public void processSchemaChange(SchemaChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        try {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemaMaps.put(tableId, createTableEvent.getSchema());
                LOG.debug("Cached schema for new table: {}", tableId);

                initializedTables.computeIfAbsent(
                        tableId,
                        tId -> {
                            try {
                                // Send an explicit event to the coordinator so it can prepare
                                // resources *before* we attempt to write any data.
                                getOperatorEventGateway()
                                        .sendEventToCoordinator(
                                                new CreateTableOperatorEvent(createTableEvent));
                                LOG.info(
                                        "Sent CreateTableOperatorEvent to coordinator for new table: {}",
                                        tId);

                                // Now, create the physical dir for Hudi table.
                                Configuration tableConfig = createTableSpecificConfig(tId);
                                createHudiTablePath(tableConfig, tId);
                            } catch (Exception e) {
                                // Re-throw to fail the Flink task if initialization fails.
                                throw new RuntimeException(
                                        "Failed during first-time initialization for table: " + tId,
                                        e);
                            }
                            return true; // Mark as initialized for this function instance.
                        });
                // Ensure tableFunction is initialized
                getOrCreateTableFunction(tableId);
            } else if (event instanceof SchemaChangeEvent) {
                SchemaChangeEvent schemaChangeEvent = event;
                Schema existingSchema = schemaMaps.get(tableId);
                if (existingSchema != null
                        && !SchemaUtils.isSchemaChangeEventRedundant(
                                existingSchema, schemaChangeEvent)) {

                    LOG.info(
                            "Schema change event received for table {}: {}",
                            tableId,
                            schemaChangeEvent);
                    LOG.info(
                            "Existing schema for table {} has {} columns: {}",
                            tableId,
                            existingSchema.getColumnCount(),
                            existingSchema.getColumnNames());

                    Schema newSchema =
                            SchemaUtils.applySchemaChangeEvent(existingSchema, schemaChangeEvent);

                    LOG.info(
                            "New schema for table {} has {} columns: {}",
                            tableId,
                            newSchema.getColumnCount(),
                            newSchema.getColumnNames());

                    schemaMaps.put(tableId, newSchema);

                    // Invalidate cached table configuration so it gets recreated with NEW
                    // schema
                    // The tableConfigurations cache holds FlinkOptions.SOURCE_AVRO_SCHEMA which
                    // must be updated
                    tableConfigurations.remove(tableId);
                    LOG.info(
                            "Invalidated cached table configuration for {} to pick up new schema",
                            tableId);

                    // If table function exists, flush buffers and update its rowType
                    EventBucketStreamWriteFunction tableFunction = tableFunctions.get(tableId);
                    if (tableFunction != null) {
                        LOG.info(
                                "Schema changed for table {}, flushing buffers with OLD schema and updating to NEW RowType",
                                tableId);
                        // NOTE: Capture the OLD RowType before any changes
                        // Buffered records were created with this schema
                        RowType oldRowType = convertSchemaToFlinkRowType(existingSchema);

                        // Flush existing buffers using the OLD schema
                        // This ensures records buffered with N columns are read with N-column
                        // schema
                        tableFunction.flushAllBuckets(oldRowType);

                        // Now safe to update to the NEW schema
                        // Future records will use this new schema
                        RowType newRowType = convertSchemaToFlinkRowType(newSchema);
                        tableFunction.updateRowType(newRowType);

                        String newAvroSchema =
                                AvroSchemaConverter.convertToSchema(newRowType).toString();

                        LOG.info(
                                "Updating write client for table: {} with new schema: {}",
                                tableId,
                                newAvroSchema);

                        // Update write client's source avro schema with new schema
                        tableFunction.updateWriteClientWithNewSchema(newAvroSchema);

                        LOG.info("Successfully handled schema change for table: {}", tableId);
                    }

                    LOG.debug("Updated schema for table: {}", tableId);
                }
            }

            // Forward the event to tableFunction so that schemaMap for serializer is updated
            tableFunctions.get(event.tableId()).processSchemaChange(event);
        } catch (Exception e) {
            LOG.error("Failed to process schema event for table: {}", tableId, e);
            throw new RuntimeException("Failed to process schema event for table: " + tableId, e);
        }
    }

    /**
     * Processes change events (ChangeEvent) for writing. This triggers the actual Hudi write
     * operations as side effects by delegating to table-specific functions.
     *
     * <p>Implements {@link EventProcessorFunction#processDataChange(DataChangeEvent)}.
     */
    @Override
    public void processDataChange(DataChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        try {
            LOG.debug("Processing change event for table: {}", tableId);

            // Get or create table-specific function to handle this event
            EventBucketStreamWriteFunction tableFunction = getOrCreateTableFunction(tableId);

            // Use the table function to process the change event
            // This will convert the event to HoodieFlinkInternalRow and buffer it for writing
            tableFunction.processDataChange(event);

            LOG.debug("Successfully processed change event for table: {}", tableId);

        } catch (Exception e) {
            LOG.error("Failed to process change event for table: {}", tableId, e);
            throw new RuntimeException("Failed to process change event for table: " + tableId, e);
        }
    }

    public static void createHudiTablePath(Configuration config, TableId tableId)
            throws IOException {
        String tablePath = config.get(FlinkOptions.PATH);
        Path path = Paths.get(tablePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    /**
     * Processes flush events for coordinated flushing across table functions. This handles both
     * table-specific and global flush operations.
     *
     * <p>Implements {@link EventProcessorFunction#processFlush(FlushEvent)}.
     */
    @Override
    public void processFlush(FlushEvent event) throws Exception {
        List<TableId> tableIds = event.getTableIds();
        try {
            if (tableIds == null || tableIds.isEmpty()) {
                LOG.info(
                        "Received global flush event, flushing all {} table functions",
                        tableFunctions.size());
                for (Map.Entry<TableId, EventBucketStreamWriteFunction> entry :
                        tableFunctions.entrySet()) {
                    entry.getValue().flushRemaining(false);
                    LOG.debug("Flushed table function for: {}", entry.getKey());
                }
            } else {
                LOG.info("Received flush event for {} specific tables", tableIds.size());
                for (TableId tableId : tableIds) {
                    EventBucketStreamWriteFunction tableFunction = tableFunctions.get(tableId);
                    if (tableFunction != null) {
                        tableFunction.flushRemaining(false);
                        LOG.debug("Flushed table function for: {}", tableId);
                    }
                }
            }

            if (schemaEvolutionClient == null) {
                return;
            }

            int sinkSubtaskId = getRuntimeContext().getIndexOfThisSubtask();
            int sourceSubtaskId = event.getSourceSubTaskId();

            try {
                schemaEvolutionClient.notifyFlushSuccess(sinkSubtaskId, sourceSubtaskId);
                LOG.info(
                        "Sent FlushSuccessEvent to SchemaOperator from sink subtask {} for source subtask {}",
                        sinkSubtaskId,
                        sourceSubtaskId);
            } catch (Exception e) {
                LOG.error("Failed to send FlushSuccessEvent to SchemaOperator", e);
                throw new RuntimeException("Failed to send FlushSuccessEvent to SchemaOperator", e);
            }

        } catch (Exception e) {
            LOG.error("Failed to process flush event", e);
            throw new RuntimeException("Failed to process flush event", e);
        }
    }

    private EventBucketStreamWriteFunction getOrCreateTableFunction(TableId tableId) {
        EventBucketStreamWriteFunction existingFunction = tableFunctions.get(tableId);
        if (existingFunction != null) {
            return existingFunction;
        }

        LOG.info("Creating new EventBucketStreamWriteFunction for table: {}", tableId);
        try {
            EventBucketStreamWriteFunction tableFunction = createTableFunction(tableId);
            tableFunctions.put(tableId, tableFunction);
            initializedTables.put(tableId, true);
            LOG.info("Successfully created and cached table function for: {}", tableId);
            return tableFunction;
        } catch (Exception e) {
            LOG.error("Failed to create table function for table: {}", tableId, e);
            throw new RuntimeException("Failed to create table function for table: " + tableId, e);
        }
    }

    private EventBucketStreamWriteFunction createTableFunction(TableId tableId) throws Exception {
        Schema schema = schemaMaps.get(tableId);
        if (schema == null) {
            throw new IllegalStateException(
                    "No schema found for table: "
                            + tableId
                            + ". CreateTableEvent must arrive before data events.");
        }

        if (functionInitializationContext == null) {
            throw new IllegalStateException(
                    "FunctionInitializationContext not available for creating table function: "
                            + tableId);
        }

        Configuration tableConfig = createTableSpecificConfig(tableId);
        RowType rowType = convertSchemaToFlinkRowType(schema);

        EventBucketStreamWriteFunction tableFunction =
                new EventBucketStreamWriteFunction(tableConfig, rowType);

        tableFunction.setRuntimeContext(getRuntimeContext());

        // Create a table-aware correspondent that can send MultiTableInstantTimeRequest
        // Get the operator ID from the runtime context
        TableAwareCorrespondent tableCorrespondent =
                TableAwareCorrespondent.getInstance(correspondent, tableId);
        tableFunction.setCorrespondent(tableCorrespondent);
        tableFunction.setTableId(tableId);

        // This is the key change: instead of passing the raw gateway, we pass a proxy
        // that intercepts and enhances events with the table path.
        String tablePath = tableConfig.getString(FlinkOptions.PATH);
        tableFunction.setOperatorEventGateway(
                new InterceptingGateway(this.getOperatorEventGateway(), tablePath));

        try {
            tableFunction.initializeState(functionInitializationContext);
            LOG.info("Successfully initialized state for table function: {}", tableId);
        } catch (Exception e) {
            LOG.error("Failed to initialize state for table function: {}", tableId, e);
            throw new RuntimeException(
                    "Failed to initialize state for table function: " + tableId, e);
        }

        tableFunction.open(tableConfig);

        if (tableFunction.getRecordSerializer() instanceof HudiRecordEventSerializer) {
            HudiRecordEventSerializer serializer =
                    (HudiRecordEventSerializer) tableFunction.getRecordSerializer();
            serializer.setSchema(tableId, schema);
            LOG.debug("Set schema for table function serializer: {}", tableId);
        }

        LOG.debug("Successfully created table function for: {}", tableId);
        return tableFunction;
    }

    private RowType convertSchemaToFlinkRowType(Schema cdcSchema) {
        return RowDataUtils.toRowType(cdcSchema);
    }

    private Configuration createTableSpecificConfig(TableId tableId) {
        LOG.debug("Creating table specific config for table: {}", tableId);
        return tableConfigurations.computeIfAbsent(
                tableId,
                k -> {
                    Configuration localTableConfig = new Configuration();
                    localTableConfig.addAll(this.config);

                    localTableConfig.set(FlinkOptions.DATABASE_NAME, tableId.getSchemaName());
                    localTableConfig.set(FlinkOptions.TABLE_NAME, tableId.getTableName());

                    RowType rowType = convertSchemaToFlinkRowType(schemaMaps.get(tableId));
                    String tableAvroSchema =
                            AvroSchemaConverter.convertToSchema(rowType).toString();
                    localTableConfig.set(FlinkOptions.SOURCE_AVRO_SCHEMA, tableAvroSchema);

                    String rootPath = this.config.get(FlinkOptions.PATH).split("/coordinator")[0];
                    if (rootPath != null) {
                        String tableBasePath =
                                String.format(
                                        "%s/%s/%s",
                                        rootPath, tableId.getSchemaName(), tableId.getTableName());
                        localTableConfig.set(FlinkOptions.PATH, tableBasePath);
                    }

                    // Modify ViewStorageProperties to point to coordinator table
                    FileSystemViewStorageConfig viewStorageConfig =
                            ViewStorageProperties.loadFromProperties(
                                    this.config.get(FlinkOptions.PATH), localTableConfig);
                    localTableConfig.setString(
                            FileSystemViewStorageConfig.VIEW_TYPE.key(),
                            viewStorageConfig.getStorageType().name());
                    localTableConfig.setString(
                            FileSystemViewStorageConfig.REMOTE_HOST_NAME.key(),
                            viewStorageConfig.getRemoteViewServerHost());
                    localTableConfig.setString(
                            FileSystemViewStorageConfig.REMOTE_PORT_NUM.key(),
                            viewStorageConfig.getRemoteViewServerPort() + "");
                    return localTableConfig;
                });
    }

    @Override
    public void snapshotState() {
        // This function acts as a dispatcher. It should not manage its own instant or buffer.
        // Instead, it delegates the snapshot operation to each of its child, table-specific
        // functions. Each child function will then handle its own buffer flushing and state
        // snapshotting. The direct call to flushRemaining() is removed to prevent sending
        // an invalid, generic instant request to the coordinator.
        //        flushRemaining(false);

        // NOTE: This abstract method is intentionally empty for multi-table function.
        // The actual delegation happens in snapshotState(FunctionSnapshotContext)
        // to ensure child functions receive the correct checkpointId.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Persist schemas to state for recovery
        if (schemaState != null && schemaMaps != null) {
            schemaState.clear();
            for (Map.Entry<TableId, Schema> entry : schemaMaps.entrySet()) {
                schemaState.add(new Tuple2<>(entry.getKey(), entry.getValue()));
                LOG.debug("Persisted schema for table: {}", entry.getKey());
            }
            LOG.info("Persisted {} schemas to state", schemaMaps.size());
        }

        // Delegate snapshot to table functions
        // Child functions are composition objects, not Flink operators, so they shouldn't
        // go through the full snapshotState(FunctionSnapshotContext) lifecycle which
        // includes state reloading. Instead, we:
        // 1. Call their abstract snapshotState() to flush buffers
        // 2. Manually update their checkpointId for instant requests
        long checkpointId = context.getCheckpointId();
        for (Map.Entry<TableId, EventBucketStreamWriteFunction> entry : tableFunctions.entrySet()) {
            try {
                EventBucketStreamWriteFunction tableFunction = entry.getValue();
                LOG.debug(
                        "Delegating snapshotState for table: {} with checkpointId: {}",
                        entry.getKey(),
                        checkpointId);

                // Call abstract snapshotState() to flush buffers
                tableFunction.snapshotState();

                // Update the child function's checkpointId using reflection
                // This is necessary because child functions need the current checkpointId
                // when requesting instants from the coordinator
                setCheckpointId(tableFunction, checkpointId);

                LOG.debug("Successfully snapshotted state for table: {}", entry.getKey());
            } catch (Exception e) {
                LOG.error("Failed to snapshot state for table: {}", entry.getKey(), e);
                throw new RuntimeException(
                        "Failed to snapshot state for table: " + entry.getKey(), e);
            }
        }
    }

    /**
     * Sets the checkpointId field on a child AbstractStreamWriteFunction using reflection. This is
     * necessary because checkpointId is protected and child functions are composition objects that
     * need the current checkpoint ID for coordinator communication.
     */
    private void setCheckpointId(AbstractStreamWriteFunction<?> function, long checkpointId) {
        try {
            Field checkpointIdField =
                    AbstractStreamWriteFunction.class.getDeclaredField("checkpointId");
            checkpointIdField.setAccessible(true);
            checkpointIdField.setLong(function, checkpointId);
        } catch (Exception e) {
            LOG.error("Failed to set checkpointId on child function using reflection", e);
            throw new RuntimeException("Failed to set checkpointId on child function", e);
        }
    }

    protected void flushRemaining(boolean endInput) {
        boolean hasData = !tableFunctions.isEmpty();
        this.currentInstant = instantToWrite(hasData);

        if (this.currentInstant == null) {
            if (hasData) {
                throw new RuntimeException(
                        "No inflight instant when flushing data for multi-table function!");
            }
        }

        LOG.debug(
                "Multi-table function requested instant: {} for {} table functions",
                this.currentInstant,
                tableFunctions.size());

        // This method is intentionally overridden to be a no-op.
        // The MultiTableEventStreamWriteFunction is a dispatcher and does not have its own
        // data buffers to flush. Flushing is handled by the individual, table-specific
        // write functions it manages. Calling the parent's flushRemaining would cause
        // an erroneous, non-table-specific instant request to be sent to the coordinator,
        // resulting in the NullPointerException.
    }

    @Override
    public void close() throws Exception {
        for (EventBucketStreamWriteFunction func : tableFunctions.values()) {
            try {
                func.close();
            } catch (Exception e) {
                LOG.error("Failed to close table function", e);
            }
        }
        super.close();
    }

    public void endInput() {
        super.endInput();
        flushRemaining(true);
        for (EventBucketStreamWriteFunction func : tableFunctions.values()) {
            try {
                func.endInput();
            } catch (Exception e) {
                LOG.error("Failed to complete endInput for table function", e);
            }
        }
    }

    /**
     * A proxy {@link OperatorEventGateway} that intercepts {@link WriteMetadataEvent}s from child
     * functions. It wraps them in an {@link EnhancedWriteMetadataEvent} to add the table path,
     * which is essential for the multi-table coordinator to route the event correctly.
     */
    private static class InterceptingGateway implements OperatorEventGateway {
        private final OperatorEventGateway delegate;
        private final String tablePath;

        InterceptingGateway(OperatorEventGateway delegate, String tablePath) {
            this.delegate = delegate;
            this.tablePath = tablePath;
        }

        @Override
        public void sendEventToCoordinator(OperatorEvent event) {
            if (event instanceof WriteMetadataEvent) {
                // Wrap the original event with the table path so the coordinator knows
                // which table this metadata belongs to.
                EnhancedWriteMetadataEvent enhancedEvent =
                        new EnhancedWriteMetadataEvent((WriteMetadataEvent) event, tablePath);
                delegate.sendEventToCoordinator(enhancedEvent);
            } else {
                delegate.sendEventToCoordinator(event);
            }
        }
    }
}
