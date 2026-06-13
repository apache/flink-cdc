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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.lancedb.client.DefaultLanceDbClient;
import org.apache.flink.cdc.connectors.lancedb.client.LanceDbClient;
import org.apache.flink.cdc.connectors.lancedb.serde.LanceDbOperation;
import org.apache.flink.cdc.connectors.lancedb.serde.LanceDbRecordConverter;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbPathUtils;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbRetryUtils;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbTypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** LanceDB sink writer. */
public class LanceDbEventWriter implements SinkWriter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceDbEventWriter.class);

    private final LanceDbDataSinkConfig config;
    private final LanceDbSinkMetrics metrics;
    private final LanceDbClient client;
    private final Map<TableId, Schema> schemas;
    private final Map<TableId, LanceDbRecordConverter> converters;
    private final Map<String, TableId> datasetOwners;
    private final Map<String, Schema> datasetSchemas;
    private final List<LanceDbOperation> pendingOperations;
    private long lastFlushTimestamp;

    public LanceDbEventWriter(LanceDbDataSinkConfig config) {
        this(config, LanceDbSinkMetrics.NOOP);
    }

    LanceDbEventWriter(LanceDbDataSinkConfig config, LanceDbSinkMetrics metrics) {
        this(config, metrics, new DefaultLanceDbClient(config));
    }

    LanceDbEventWriter(
            LanceDbDataSinkConfig config, LanceDbSinkMetrics metrics, LanceDbClient client) {
        this.config = config;
        this.metrics = metrics;
        this.client = client;
        this.schemas = new HashMap<>();
        this.converters = new HashMap<>();
        this.datasetOwners = new HashMap<>();
        this.datasetSchemas = new HashMap<>();
        this.pendingOperations = new ArrayList<>();
        this.lastFlushTimestamp = System.currentTimeMillis();
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        try {
            if (event instanceof SchemaChangeEvent) {
                flush();
                applySchemaChange((SchemaChangeEvent) event);
            } else if (event instanceof DataChangeEvent) {
                applyDataChange((DataChangeEvent) event);
            } else {
                throw new UnsupportedOperationException("Unsupported event: " + event);
            }
            flushIfNeeded();
        } catch (Exception e) {
            throw new IOException("Failed to write event to LanceDB sink: " + event, e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        try {
            flush();
        } catch (RuntimeException e) {
            throw new IOException("Failed to flush LanceDB sink.", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            client.close();
        }
    }

    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            String datasetPath = registerDatasetOwner(event.tableId());
            Schema schema = ((CreateTableEvent) event).getSchema();
            validateSchema(schema);
            schemas.put(event.tableId(), schema);
            datasetSchemas.put(datasetPath, schema);
            refreshConverter(event.tableId());
            return;
        }
        if (event.getType() == SchemaChangeEventType.ALTER_TABLE_COMMENT) {
            return;
        }
        if (event.getType() != SchemaChangeEventType.ADD_COLUMN) {
            throw new UnsupportedOperationException(
                    "LanceDB sink writer does not support schema evolution type "
                            + event.getType()
                            + " for table "
                            + event.tableId()
                            + ".");
        }
        if (!config.isSchemaEvolutionEnabled()) {
            throw new IllegalStateException(
                    "LanceDB ADD_COLUMN requires schema.evolution.enabled=true.");
        }
        Schema oldSchema = requireSchema(event.tableId());
        String datasetPath = registerDatasetOwner(event.tableId());
        Schema newSchema = applyAddColumnEventIdempotently(oldSchema, (AddColumnEvent) event);
        validateSchema(newSchema);
        schemas.put(event.tableId(), newSchema);
        datasetSchemas.put(datasetPath, newSchema);
        refreshConverter(event.tableId());
    }

    private void applyDataChange(DataChangeEvent event) {
        requireSchema(event.tableId());
        validateChangelogMode(event);
        LanceDbOperation operation = requireConverter(event.tableId()).convert(event);
        pendingOperations.add(operation);
        metrics.setPendingRows(pendingOperations.size());
    }

    private void validateChangelogMode(DataChangeEvent event) {
        OperationType op = event.op();
        String mode = config.getChangelogMode();
        if ("reject".equals(mode)) {
            throw new IllegalStateException(
                    "LanceDB sink rejects all data changes because sink.changelog-mode=reject.");
        }
        if ("append-only".equals(mode)
                && (op == OperationType.UPDATE || op == OperationType.DELETE)) {
            throw new IllegalStateException(
                    "LanceDB append-only mode rejects "
                            + op
                            + " events. Use sink.changelog-mode=append-with-metadata to persist CDC logs.");
        }
        if ((op == OperationType.INSERT
                        || op == OperationType.REPLACE
                        || op == OperationType.UPDATE)
                && event.after() == null) {
            throw new IllegalStateException(op + " event must contain after record.");
        }
        if (op == OperationType.DELETE && event.before() == null) {
            throw new IllegalStateException("DELETE event must contain before record.");
        }
    }

    private void flushIfNeeded() {
        long now = System.currentTimeMillis();
        if (pendingOperations.size() >= config.getFlushMaxRows()
                || now - lastFlushTimestamp >= config.getFlushInterval().toMillis()) {
            flush();
        }
    }

    private void flush() {
        if (pendingOperations.isEmpty()) {
            return;
        }
        int rows = pendingOperations.size();
        long startMillis = System.currentTimeMillis();
        RuntimeException failure = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                executePendingOperationsAsOrderedBatches();
                lastFlushTimestamp = System.currentTimeMillis();
                metrics.recordSuccessfulFlush(rows, lastFlushTimestamp - startMillis);
                return;
            } catch (RuntimeException e) {
                failure = e;
                metrics.recordFlushFailure();
                if (!LanceDbRetryUtils.isRetryable(e) || attempt >= config.getMaxRetries()) {
                    break;
                }
                metrics.recordRetry();
                LOG.warn(
                        "Failed to flush LanceDB batch. Retry {}/{}.",
                        attempt + 1,
                        config.getMaxRetries(),
                        e);
                sleepBeforeRetry(e);
                reopenPendingDatasets();
            }
        }
        metrics.recordRecordsOutErrors(pendingOperations.size());
        throw failure;
    }

    private void executePendingOperationsAsOrderedBatches() {
        while (!pendingOperations.isEmpty()) {
            LanceDbOperation first = pendingOperations.get(0);
            int batchSize =
                    Math.min(
                            countLeadingBatchSize(first.getDatasetPath()),
                            config.getMaxRowsPerCommit());
            List<List<Object>> rows = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                rows.add(pendingOperations.get(i).getValues());
            }
            org.apache.arrow.vector.types.pojo.Schema arrowSchema =
                    LanceDbTypeUtils.toArrowSchema(
                            requireDatasetSchema(first.getDatasetPath()),
                            isAppendWithMetadataMode());
            int written = client.appendRows(first.getDatasetPath(), arrowSchema, rows);
            if (written <= 0 || written > batchSize) {
                throw new IllegalStateException(
                        "LanceDB client returned invalid written row count "
                                + written
                                + " for batch size "
                                + batchSize
                                + ".");
            }
            pendingOperations.subList(0, written).clear();
            metrics.setPendingRows(pendingOperations.size());
        }
    }

    private int countLeadingBatchSize(String datasetPath) {
        int batchSize = 1;
        while (batchSize < pendingOperations.size()
                && datasetPath.equals(pendingOperations.get(batchSize).getDatasetPath())) {
            batchSize++;
        }
        return batchSize;
    }

    private Schema requireDatasetSchema(String datasetPath) {
        Schema schema = datasetSchemas.get(datasetPath);
        if (schema != null) {
            return schema;
        }
        throw new IllegalStateException("Schema for Lance dataset " + datasetPath + " is missing.");
    }

    private void reopenPendingDatasets() {
        List<String> reopened = new ArrayList<>();
        for (LanceDbOperation operation : pendingOperations) {
            if (reopened.contains(operation.getDatasetPath())) {
                continue;
            }
            try {
                client.reopen(operation.getDatasetPath());
                metrics.recordReopen();
                reopened.add(operation.getDatasetPath());
            } catch (RuntimeException e) {
                LOG.warn("Failed to reopen Lance dataset {}.", operation.getDatasetPath(), e);
            }
        }
    }

    private void sleepBeforeRetry(RuntimeException cause) {
        long backoffMillis = config.getRetryBackoff().toMillis();
        if (backoffMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting to retry LanceDB flush.", cause);
        }
    }

    private Schema applyAddColumnEventIdempotently(Schema oldSchema, AddColumnEvent event) {
        List<AddColumnEvent.ColumnWithPosition> columnsToAdd = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            if (columnWithPosition.getPosition() != AddColumnEvent.ColumnPosition.LAST) {
                throw new IllegalStateException(
                        "LanceDB sink only supports ADD_COLUMN at LAST position.");
            }
            Column newColumn = columnWithPosition.getAddColumn();
            java.util.Optional<Column> existingColumn = oldSchema.getColumn(newColumn.getName());
            if (!existingColumn.isPresent()) {
                columnsToAdd.add(columnWithPosition);
                continue;
            }
            if (!existingColumn.get().getType().equals(newColumn.getType())
                    || existingColumn.get().isPhysical() != newColumn.isPhysical()) {
                throw new IllegalStateException(
                        "LanceDB writer schema already contains column "
                                + newColumn.getName()
                                + " but it is incompatible with replayed ADD_COLUMN.");
            }
        }
        if (columnsToAdd.isEmpty()) {
            return oldSchema;
        }
        return SchemaUtils.applySchemaChangeEvent(
                oldSchema, new AddColumnEvent(event.tableId(), columnsToAdd));
    }

    private Schema requireSchema(TableId tableId) {
        Schema schema = schemas.get(tableId);
        if (schema == null) {
            throw new IllegalStateException(
                    "Schema for table " + tableId + " is missing in LanceDB sink writer.");
        }
        return schema;
    }

    private LanceDbRecordConverter requireConverter(TableId tableId) {
        return converters.computeIfAbsent(tableId, this::createConverter);
    }

    private LanceDbRecordConverter createConverter(TableId tableId) {
        return new LanceDbRecordConverter(
                requireSchema(tableId), config, isAppendWithMetadataMode());
    }

    private void refreshConverter(TableId tableId) {
        converters.put(tableId, createConverter(tableId));
    }

    private String registerDatasetOwner(TableId tableId) {
        String datasetPath = LanceDbPathUtils.resolveDatasetPath(tableId, config);
        TableId existing = datasetOwners.putIfAbsent(datasetPath, tableId);
        if (existing != null && !existing.equals(tableId)) {
            throw new IllegalStateException(
                    "Lance dataset path "
                            + datasetPath
                            + " is already owned by source table "
                            + existing
                            + " and cannot also be used by "
                            + tableId
                            + ". Configure table.path.mapping to unique dataset paths.");
        }
        return datasetPath;
    }

    private void validateSchema(Schema schema) {
        LanceDbTypeUtils.toArrowSchema(schema, isAppendWithMetadataMode());
    }

    private boolean isAppendWithMetadataMode() {
        return "append-with-metadata".equals(config.getChangelogMode());
    }
}
