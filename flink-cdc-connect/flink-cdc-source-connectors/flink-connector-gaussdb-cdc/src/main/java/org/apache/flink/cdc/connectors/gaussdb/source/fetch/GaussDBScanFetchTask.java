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

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.CustomGaussDBSchema;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.GaussDBQueryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;
import io.debezium.connector.gaussdb.GaussDBSchema;
import io.debezium.connector.gaussdb.connection.GaussDBConnection;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** A fetch task for reading snapshot splits from GaussDB tables. */
public class GaussDBScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBScanFetchTask.class);

    public GaussDBScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        LOG.info(
                "=== GaussDBScanFetchTask.executeDataSnapshot STARTED for split: {} ===",
                snapshotSplit.splitId());
        GaussDBSourceFetchTaskContext ctx = requireGaussDbContext(context);
        LOG.info("=== Got GaussDBSourceFetchTaskContext ===");

        if (!(ctx.getConnection() instanceof GaussDBConnection)) {
            throw new FlinkRuntimeException(
                    "Unsupported connection type: " + ctx.getConnection().getClass().getName());
        }
        GaussDBConnection jdbcConnection = (GaussDBConnection) ctx.getConnection();
        LOG.info("=== Got GaussDBConnection ===");

        CustomGaussDBSchema schema = ctx.getCustomSchema();
        LOG.info("=== Got CustomGaussDBSchema ===");

        TableId tableId = snapshotSplit.getTableId();
        LOG.info("=== TableId: {} ===", tableId);

        TableChange tableChange = schema.getTableSchema(tableId);
        LOG.info("=== Got TableChange ===");

        if (tableChange == null) {
            throw new FlinkRuntimeException(
                    "Table schema not found for "
                            + tableId
                            + ". Table may not have a primary key.");
        }

        Table table = tableChange.getTable();
        if (table == null) {
            throw new FlinkRuntimeException("Table definition is null for " + tableId);
        }

        // Use GaussDBSchema to get the Debezium TableSchema for data conversion
        final GaussDBConnectorConfig connectorConfig = ctx.getDbzConnectorConfig();
        LOG.info("=== Got GaussDBConnectorConfig ===");
        final GaussDBSchema databaseSchema = new GaussDBSchema(connectorConfig, jdbcConnection);
        LOG.info("=== Created GaussDBSchema ===");
        final io.debezium.relational.TableSchema tableSchema = databaseSchema.schemaFor(tableId);
        LOG.info("=== Got Debezium TableSchema ===");
        if (tableSchema == null) {
            databaseSchema.close();
            throw new FlinkRuntimeException("Debezium TableSchema not found for " + tableId);
        }

        try {
            LOG.info("=== About to call readTableSplitData ===");
            readTableSplitData(ctx, jdbcConnection, table, tableSchema);
            LOG.info("=== readTableSplitData completed ===");
        } finally {
            databaseSchema.close();
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        LOG.info("Starting backfill for split: {}", backfillStreamSplit.splitId());
        // Re-configure the context to initialize streaming resources (e.g. replication connection).
        context.configure(backfillStreamSplit);
        GaussDBStreamFetchTask backfillStreamTask = new GaussDBStreamFetchTask(backfillStreamSplit);
        backfillStreamTask.execute(context);
        LOG.info("Completed backfill for split: {}", backfillStreamSplit.splitId());
    }

    /** Reads data from a single snapshot split using JDBC. */
    private void readTableSplitData(
            GaussDBSourceFetchTaskContext context,
            GaussDBConnection jdbcConnection,
            Table table,
            io.debezium.relational.TableSchema tableSchema)
            throws SQLException, InterruptedException {

        final long startTime = System.currentTimeMillis();
        final TableId tableId = table.id();
        final String schemaName = tableId != null ? tableId.schema() : null;
        final String tableName = tableId != null ? tableId.table() : null;
        LOG.info("=== Starting readTableSplitData for table: {} ===", tableId);
        final ChangeEventQueue<DataChangeEvent> queue =
                requireQueue(context, snapshotSplit.splitId());

        final GaussDBConnectorConfig connectorConfig = context.getDbzConnectorConfig();

        final Map<String, ?> sourcePartition = context.getPartition().getSourcePartition();
        final String topic = topicFor(connectorConfig, tableId);
        final Envelope envelope = tableSchema.getEnvelopeSchema();
        final Map<String, Object> snapshotOffset =
                Collections.singletonMap(
                        org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset.LSN_KEY,
                        io.debezium.connector.gaussdb.connection.Lsn.INVALID_LSN.asLong());

        // Identify UUID columns for proper casting in queries
        List<String> uuidFields =
                snapshotSplit.getSplitKeyType().getFieldNames().stream()
                        .filter(field -> isUuidColumn(table, field))
                        .collect(Collectors.toList());

        // Build the split scan query

        final String selectSql =
                GaussDBQueryUtils.buildSplitScanQuery(
                        tableId,
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null,
                        uuidFields);

        LOG.info("=== Split scan query: {} ===", selectSql);
        final int fetchSize = context.getSourceConfig().getSplitSize();

        try (PreparedStatement statement =
                        GaussDBQueryUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType().getFieldCount(),
                                fetchSize);
                ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rowCount = 0;
            final long logInterval = 10000;
            long nextLogThreshold = logInterval;

            LOG.info("=== Starting to read rows from ResultSet ===");
            while (rs.next() && taskRunning) {
                rowCount++;

                // Extract row data
                final Object[] rowData = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column column = columnArray.getColumns()[i];
                    int position = column.position();
                    rowData[position - 1] = readColumnValue(rs, i + 1, column);
                }

                // Emit snapshot row as Debezium READ record.
                final org.apache.kafka.connect.data.Struct keyStruct =
                        tableSchema.keyFromColumnData(rowData);
                final org.apache.kafka.connect.data.Struct afterStruct =
                        tableSchema.valueFromColumnData(rowData);

                final org.apache.kafka.connect.data.Schema sourceSchema =
                        connectorConfig.getSourceInfoStructMaker().schema();
                final org.apache.kafka.connect.data.Struct sourceStruct =
                        buildSnapshotSourceStruct(
                                sourceSchema, connectorConfig, schemaName, tableName);

                final org.apache.kafka.connect.data.Struct valueStruct =
                        envelope.read(afterStruct, sourceStruct, Instant.now());
                final org.apache.kafka.connect.source.SourceRecord snapshotRecord =
                        new org.apache.kafka.connect.source.SourceRecord(
                                sourcePartition,
                                snapshotOffset,
                                topic,
                                tableSchema.keySchema(),
                                keyStruct,
                                envelope.schema(),
                                valueStruct);
                queue.enqueue(new DataChangeEvent(snapshotRecord));

                if (rowCount == 1) {
                    LOG.info("=== Successfully enqueued first row ===");
                }

                // Log progress periodically
                if (rowCount >= nextLogThreshold) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    LOG.info(
                            "Exported {} records for split '{}' after {}",
                            rowCount,
                            snapshotSplit.splitId(),
                            Strings.duration(elapsed));
                    nextLogThreshold += logInterval;
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;
            LOG.info(
                    "Finished exporting {} records for split '{}', total duration: {}",
                    rowCount,
                    snapshotSplit.splitId(),
                    Strings.duration(totalTime));

        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Failed to read snapshot split for table " + tableId, e);
        }
    }

    /** Reads a column value from the ResultSet, handling NULL values correctly. */
    private Object readColumnValue(ResultSet rs, int columnIndex, Column column)
            throws SQLException {
        Object value = rs.getObject(columnIndex);

        // Check if the value is NULL
        if (rs.wasNull()) {
            return null;
        }

        // Normalize UUID values for better type compatibility.
        if (isUuidColumn(column) && value instanceof String) {
            try {
                return UUID.fromString((String) value);
            } catch (IllegalArgumentException ignored) {
                // Fallback to original string.
                return value;
            }
        }

        // Normalize SQL arrays to Java arrays.
        if (value instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) value;
            try {
                return array.getArray();
            } finally {
                try {
                    array.free();
                } catch (Exception ignored) {
                }
            }
        }

        return value;
    }

    /** Checks if a column is of UUID type. */
    private boolean isUuidColumn(Table table, String fieldName) {
        Column column = table.columnWithName(fieldName);
        if (column == null) {
            return false;
        }
        String typeName = column.typeName();
        if (typeName == null) {
            return false;
        }
        return typeName.toLowerCase().contains("uuid");
    }

    private boolean isUuidColumn(Column column) {
        if (column == null || column.typeName() == null) {
            return false;
        }
        return column.typeName().toLowerCase().contains("uuid");
    }

    private static org.apache.kafka.connect.data.Struct buildSnapshotSourceStruct(
            org.apache.kafka.connect.data.Schema sourceSchema,
            GaussDBConnectorConfig connectorConfig,
            String schemaName,
            String tableName) {
        final org.apache.kafka.connect.data.Struct sourceStruct =
                new org.apache.kafka.connect.data.Struct(sourceSchema);

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
        // Snapshot records have message timestamp 0.
        putIfPresent(sourceStruct, io.debezium.connector.AbstractSourceInfo.TIMESTAMP_KEY, 0L);

        // Set snapshot field to indicate this is a snapshot record (must be String, not Boolean)
        putIfPresent(sourceStruct, io.debezium.connector.AbstractSourceInfo.SNAPSHOT_KEY, "true");

        return sourceStruct;
    }

    private static void putIfPresent(
            org.apache.kafka.connect.data.Struct struct, String fieldName, Object value) {
        if (struct.schema().field(fieldName) != null) {
            struct.put(fieldName, value);
        }
    }

    private static ChangeEventQueue<DataChangeEvent> requireQueue(
            GaussDBSourceFetchTaskContext context, String splitId) {
        ChangeEventQueue<DataChangeEvent> queue = context.getQueue();
        if (queue == null) {
            throw new FlinkRuntimeException(
                    "ChangeEventQueue is not initialized for split " + splitId);
        }
        return queue;
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

    @Override
    protected void dispatchLowWaterMarkEvent(
            Context context, SourceSplitBase split, Offset lowWatermark)
            throws InterruptedException {
        LOG.info(
                "=== dispatchLowWaterMarkEvent for split: {} with offset: {} ===",
                split.splitId(),
                lowWatermark);
        GaussDBSourceFetchTaskContext taskContext = requireGaussDbContext(context);
        ChangeEventQueue<DataChangeEvent> queue = requireQueue(taskContext, split.splitId());
        queue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                taskContext.getPartition().getSourcePartition(),
                                "gaussdb-watermark",
                                split.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));
    }

    @Override
    protected void dispatchHighWaterMarkEvent(
            Context context, SourceSplitBase split, Offset highWatermark)
            throws InterruptedException {
        LOG.info(
                "=== dispatchHighWaterMarkEvent for split: {} with offset: {} ===",
                split.splitId(),
                highWatermark);
        GaussDBSourceFetchTaskContext taskContext = requireGaussDbContext(context);
        ChangeEventQueue<DataChangeEvent> queue = requireQueue(taskContext, split.splitId());
        queue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                taskContext.getPartition().getSourcePartition(),
                                "gaussdb-watermark",
                                split.splitId(),
                                WatermarkKind.HIGH,
                                highWatermark)));
    }

    @Override
    protected void dispatchEndWaterMarkEvent(
            Context context, SourceSplitBase split, Offset endWatermark)
            throws InterruptedException {
        LOG.info(
                "=== dispatchEndWaterMarkEvent for split: {} with offset: {} ===",
                split.splitId(),
                endWatermark);
        GaussDBSourceFetchTaskContext taskContext = requireGaussDbContext(context);
        ChangeEventQueue<DataChangeEvent> queue = requireQueue(taskContext, split.splitId());
        queue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                taskContext.getPartition().getSourcePartition(),
                                "gaussdb-watermark",
                                split.splitId(),
                                WatermarkKind.END,
                                endWatermark)));
    }

    private static GaussDBSourceFetchTaskContext requireGaussDbContext(Context context) {
        if (!(context instanceof GaussDBSourceFetchTaskContext)) {
            throw new FlinkRuntimeException(
                    "Unsupported fetch task context type: " + context.getClass().getName());
        }
        return (GaussDBSourceFetchTaskContext) context;
    }
}
