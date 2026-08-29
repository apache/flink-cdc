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

package org.apache.flink.cdc.connectors.db2.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** The {@link RecordEmitter} implementation for Db2 pipeline connector. */
public class Db2PipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    private final Db2SourceConfig sourceConfig;
    private final Db2Dialect dataSourceDialect;

    // Track tables that have already sent CreateTableEvent
    private final Set<io.debezium.relational.TableId> alreadySendCreateTableTables;

    // Cache for CreateTableEvent, using Map for O(1) lookup
    private final Map<io.debezium.relational.TableId, CreateTableEvent> createTableEventCache;

    public Db2PipelineRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            Db2SourceConfig sourceConfig,
            OffsetFactory offsetFactory) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        this.sourceConfig = sourceConfig;
        this.dataSourceDialect = new Db2Dialect(sourceConfig);
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            cacheCreateTableEventsFromSchemas(splitState.asStreamSplitState().getTableSchemas());
        }

        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, lazily send CreateTableEvent
            // to downstream to avoid checkpoint timeout.
            io.debezium.relational.TableId tableId =
                    splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            emitCreateTableEventIfNeeded(tableId, output, splitState);
        } else if (isDataChangeRecord(element)) {
            // Handle data change events, schema change events are handled downstream directly
            io.debezium.relational.TableId tableId = getTableId(element);
            emitCreateTableEventIfNeeded(tableId, output, splitState);
        }
        super.processElement(element, output, splitState);
    }

    @Override
    public void applySplit(SourceSplitBase split) {
        cacheCreateTableEventsFromSchemas(split.getTableSchemas());
    }

    @SuppressWarnings("unchecked")
    private void emitCreateTableEventIfNeeded(
            io.debezium.relational.TableId tableId,
            SourceOutput<T> output,
            SourceSplitState splitState) {
        if (alreadySendCreateTableTables.contains(tableId)) {
            return;
        }

        cacheCreateTableEventsFromSchemas(splitState.toSourceSplit().getTableSchemas());
        output.collect((T) getOrCreateCreateTableEvent(tableId, splitState));
        alreadySendCreateTableTables.add(tableId);
    }

    private CreateTableEvent getOrCreateCreateTableEvent(
            io.debezium.relational.TableId tableId, SourceSplitState splitState) {
        CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
        if (createTableEvent == null) {
            createTableEvent = getCreateTableEventFromSplit(tableId, splitState);
        }
        if (createTableEvent == null) {
            createTableEvent = getCreateTableEventFromDatabase(tableId);
        }
        createTableEventCache.put(tableId, createTableEvent);
        return createTableEvent;
    }

    private CreateTableEvent getCreateTableEventFromSplit(
            io.debezium.relational.TableId tableId, SourceSplitState splitState) {
        Map<io.debezium.relational.TableId, TableChange> tableSchemas =
                splitState.toSourceSplit().getTableSchemas();
        if (tableSchemas == null || tableSchemas.isEmpty()) {
            return null;
        }
        TableChange tableChange = tableSchemas.get(tableId);
        if (tableChange == null) {
            // The catalog carried by a split comes from the Db2 system catalog (e.g. "TESTDB"),
            // while Debezium reports the configured database name in source records (e.g.
            // "testdb"). The two may differ in case, so fall back to matching on schema and
            // table name only.
            for (Map.Entry<io.debezium.relational.TableId, TableChange> entry :
                    tableSchemas.entrySet()) {
                if (matchesIgnoringCatalog(entry.getKey(), tableId)) {
                    tableChange = entry.getValue();
                    break;
                }
            }
        }
        if (tableChange == null || tableChange.getTable() == null) {
            return null;
        }
        return buildCreateTableEvent(tableId, Db2SchemaUtils.toSchema(tableChange.getTable()));
    }

    /** Last resort: read the current table schema from the database itself. */
    private CreateTableEvent getCreateTableEventFromDatabase(
            io.debezium.relational.TableId tableId) {
        org.apache.flink.cdc.common.event.TableId cdcTableId = Db2SchemaUtils.toCdcTableId(tableId);
        try (JdbcConnection jdbc = dataSourceDialect.openJdbcConnection(sourceConfig)) {
            return buildCreateTableEvent(
                    tableId, Db2SchemaUtils.getTableSchema(cdcTableId, jdbc, dataSourceDialect));
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Cannot fetch table schema from database for " + cdcTableId, e);
        }
    }

    private static boolean matchesIgnoringCatalog(
            io.debezium.relational.TableId left, io.debezium.relational.TableId right) {
        return Objects.equals(left.schema(), right.schema())
                && Objects.equals(left.table(), right.table());
    }

    private void cacheCreateTableEventsFromSchemas(
            Map<io.debezium.relational.TableId, TableChange> tableSchemas) {
        if (tableSchemas == null || tableSchemas.isEmpty()) {
            return;
        }
        for (Map.Entry<io.debezium.relational.TableId, TableChange> entry :
                tableSchemas.entrySet()) {
            io.debezium.relational.TableId tableId = entry.getKey();
            TableChange tableChange = entry.getValue();
            if (tableId == null || tableChange == null || tableChange.getTable() == null) {
                continue;
            }
            createTableEventCache.put(
                    tableId,
                    buildCreateTableEvent(
                            tableId, Db2SchemaUtils.toSchema(tableChange.getTable())));
        }
    }

    private CreateTableEvent buildCreateTableEvent(
            io.debezium.relational.TableId tableId, Schema schema) {
        return new CreateTableEvent(Db2SchemaUtils.toCdcTableId(tableId), schema);
    }
}
