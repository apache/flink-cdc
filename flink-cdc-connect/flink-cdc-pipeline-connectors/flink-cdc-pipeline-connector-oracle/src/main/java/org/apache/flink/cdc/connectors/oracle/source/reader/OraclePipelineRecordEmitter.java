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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.utils.OracleSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** The {@link RecordEmitter} implementation for Oracle pipeline connector. */
public class OraclePipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private static final long serialVersionUID = 1L;
    // Used when startup mode is initial
    private final Set<TableId> alreadySendCreateTableTables;
    private final Map<TableId, CreateTableEvent> createTableEventCache;

    // Used when startup mode is not initial
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private final boolean isBounded;
    private final OracleSourceConfig sourceConfig;

    public OraclePipelineRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            OracleSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                includeSchemaChanges,
                offsetFactory);
        List<String> tableList = sourceConfig.getTableList();
        this.sourceConfig = sourceConfig;
        this.createTableEventCache = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
        try (JdbcConnection jdbc = OracleSchemaUtils.createOracleConnection(sourceConfig)) {

            List<TableId> capturedTableIds = new ArrayList<>();
            for (String table : tableList) {
                TableId capturedTableId = TableId.parse(table.toUpperCase(Locale.ROOT));
                capturedTableIds.add(capturedTableId);
            }
            for (TableId tableId : capturedTableIds) {
                Schema schema = OracleSchemaUtils.getSchema(jdbc, tableId);
                createTableEventCache.put(
                        tableId,
                        new CreateTableEvent(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        tableId.catalog(), tableId.table()),
                                schema));
            }
            this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            for (TableId tableId : createTableEventCache.keySet()) {
                output.collect((T) createTableEventCache.get(tableId));
                alreadySendCreateTableTables.add(
                        TableId.parse(tableId.schema() + "." + tableId.table()));
            }
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (JdbcConnection jdbc = OracleSchemaUtils.createOracleConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, tableId, output);
                }
                alreadySendCreateTableTables.add(
                        TableId.parse(tableId.schema() + "." + tableId.table()));
            }
        } else {
            boolean isDataChangeRecord = isDataChangeRecord(element);
            if (isDataChangeRecord || isSchemaChangeEvent(element)) {
                TableId debeziumTableId = getTableId(element);
                TableId tableId =
                        TableId.parse(debeziumTableId.schema() + "." + debeziumTableId.table());
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
                    if (createTableEvent != null) {
                        output.collect((T) createTableEvent);
                        alreadySendCreateTableTables.add(tableId);
                    }
                }
                if (isDataChangeRecord && !createTableEventCache.containsKey(tableId)) {
                    try (JdbcConnection jdbc =
                            OracleSchemaUtils.createOracleConnection(sourceConfig)) {
                        createTableEventCache.put(
                                debeziumTableId,
                                sendCreateTableEvent(jdbc, debeziumTableId, output));
                    }
                }
            }
        }

        super.processElement(element, output, splitState);
    }

    private CreateTableEvent sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceOutput<T> output) {
        Schema schema = OracleSchemaUtils.getSchema(jdbc, tableId);
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.schema(), tableId.table()),
                        schema);
        output.collect((T) createTableEvent);
        return createTableEvent;
    }
}
