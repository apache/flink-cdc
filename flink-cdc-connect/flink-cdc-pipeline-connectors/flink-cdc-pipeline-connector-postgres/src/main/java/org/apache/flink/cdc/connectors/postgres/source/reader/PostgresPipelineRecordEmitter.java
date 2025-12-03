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
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** The {@link RecordEmitter} implementation for PostgreSQL pipeline connector. */
public class PostgresPipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final PostgresSourceConfig sourceConfig;
    private final PostgresDialect postgresDialect;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private boolean isBounded = false;

    private final List<CreateTableEvent> createTableEventCache = new ArrayList<>();

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
        this.postgresDialect = postgresDialect;
        this.alreadySendCreateTableTables = new HashSet<>();
        generateCreateTableEvent(sourceConfig);
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            // In snapshot mode, we simply emit all schemas at once.
            for (CreateTableEvent createTableEvent : createTableEventCache) {
                output.collect((T) createTableEvent);
            }
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
                    sendCreateTableEvent(jdbc, tableId, (SourceOutput<Event>) output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else {
            if (isDataChangeRecord(element) || isSchemaChangeEvent(element)) {
                TableId tableId = getTableId(element);
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    for (CreateTableEvent createTableEvent : createTableEventCache) {
                        if (createTableEvent != null) {
                            output.collect((T) createTableEvent);
                        }
                    }
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            PostgresConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = PostgresSchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
        output.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.schema(), tableId.table()),
                        schema));
    }

    private void generateCreateTableEvent(PostgresSourceConfig sourceConfig) {
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            List<TableId> capturedTableIds =
                    TableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            sourceConfig.includePartitionedTables());
            for (TableId tableId : capturedTableIds) {
                Schema schema = PostgresSchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
                createTableEventCache.add(
                        new CreateTableEvent(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        tableId.schema(), tableId.table()),
                                schema));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
