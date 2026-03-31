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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresTypeUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils.toCdcTableId;

/** The {@link RecordEmitter} implementation for PostgreSQL pipeline connector. */
public class PostgresPipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final PostgresSourceConfig sourceConfig;
    private final PostgresDialect postgresDialect;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private boolean isBounded = false;
    private boolean includeDatabaseInTableId = false;

    private final Map<TableId, CreateTableEvent> createTableEventCache;

    public PostgresPipelineRecordEmitter(
            DebeziumDeserializationSchema debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            PostgresSourceConfig sourceConfig,
            OffsetFactory offsetFactory,
            PostgresDialect postgresDialect) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        this.sourceConfig = sourceConfig;
        this.includeDatabaseInTableId = sourceConfig.isIncludeDatabaseInTableId();
        this.postgresDialect = postgresDialect;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
        generateCreateTableEvent(sourceConfig);
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    public void applySplit(SourceSplitBase split) {
        if ((isBounded) && createTableEventCache.isEmpty() && split instanceof SnapshotSplit) {
            // TableSchemas in SnapshotSplit only contains one table.
            createTableEventCache.putAll(generateCreateTableEvent(sourceConfig));
        } else {
            for (Map.Entry<TableId, TableChanges.TableChange> entry :
                    split.getTableSchemas().entrySet()) {
                TableId tableId =
                        entry.getKey(); // Use the TableId from the map key which contains full info
                TableChanges.TableChange tableChange = entry.getValue();
                CreateTableEvent createTableEvent =
                        new CreateTableEvent(
                                toCdcTableId(
                                        tableId,
                                        sourceConfig.getDatabaseList().get(0),
                                        includeDatabaseInTableId),
                                buildSchemaFromTable(tableChange.getTable()));
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .applyChangeEvent(createTableEvent);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            // In snapshot mode, we simply emit all schemas at once.
            createTableEventCache.forEach(
                    (tableId, createTableEvent) -> {
                        output.collect((T) createTableEvent);
                    });
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                sendCreateTableEvent(tableId, (SourceOutput<Event>) output);
                alreadySendCreateTableTables.add(tableId);
            }
        } else {
            boolean isDataChangeRecord = isDataChangeRecord(element);
            if (isDataChangeRecord || isSchemaChangeEvent(element)) {
                TableId tableId = getTableId(element);
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
                    if (createTableEvent != null) {
                        output.collect((T) createTableEvent);
                    }
                    alreadySendCreateTableTables.add(tableId);
                }
                // In rare case, we may miss some CreateTableEvents before DataChangeEvents.
                // Don't send CreateTableEvent for SchemaChangeEvents as it's the latest schema.
                if (isDataChangeRecord && !createTableEventCache.containsKey(tableId)) {
                    CreateTableEvent createTableEvent = getCreateTableEvent(sourceConfig, tableId);
                    output.collect((T) createTableEvent);
                    createTableEventCache.put(tableId, createTableEvent);
                }
            }
        }
        super.processElement(element, output, splitState);
    }

    private Schema buildSchemaFromTable(Table table) {
        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String colName = column.name();
            DataType dataType;
            try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
                dataType =
                        PostgresTypeUtils.fromDbzColumn(
                                column,
                                this.sourceConfig.getDbzConnectorConfig(),
                                jdbc.getTypeRegistry());
            }
            if (!column.isOptional()) {
                dataType = dataType.notNull();
            }
            tableBuilder.physicalColumn(
                    colName,
                    dataType,
                    column.comment(),
                    column.defaultValueExpression().orElse(null));
        }
        tableBuilder.comment(table.comment());

        List<String> primaryKey = table.primaryKeyColumnNames();
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            tableBuilder.primaryKey(primaryKey);
        }
        return tableBuilder.build();
    }

    private void sendCreateTableEvent(TableId tableId, SourceOutput<Event> output) {
        output.collect(getCreateTableEvent(sourceConfig, tableId));
    }

    private CreateTableEvent getCreateTableEvent(
            PostgresSourceConfig sourceConfig, TableId tableId) {
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Schema schema = PostgresSchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
            return new CreateTableEvent(
                    toCdcTableId(
                            tableId,
                            sourceConfig.getDatabaseList().get(0),
                            includeDatabaseInTableId),
                    schema);
        }
    }

    private TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Field field = source.schema().field(SCHEMA_NAME_KEY);
        String schemaName = null;
        if (field != null) {
            schemaName = source.getString(SCHEMA_NAME_KEY);
        }
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    private Map<TableId, CreateTableEvent> generateCreateTableEvent(
            PostgresSourceConfig sourceConfig) {
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Map<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
            List<TableId> capturedTableIds =
                    TableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            sourceConfig.includePartitionedTables());
            for (TableId tableId : capturedTableIds) {
                Schema schema = PostgresSchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
                createTableEventCache.put(
                        tableId,
                        new CreateTableEvent(
                                toCdcTableId(
                                        tableId,
                                        this.sourceConfig.getDatabaseList().get(0),
                                        includeDatabaseInTableId),
                                schema));
            }
            return createTableEventCache;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
