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
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.db2.source.Db2EventDeserializer;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.db2.Db2Connection;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils.createDb2Connection;

/** The {@link RecordEmitter} implementation for DB2 pipeline connector. */
public class Db2PipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final Db2SourceConfig sourceConfig;
    private final Db2Dialect db2Dialect;

    // Track tables for which CreateTableEvent has already been sent.
    private final Set<io.debezium.relational.TableId> alreadySendCreateTableTables;

    // Used when the startup mode is snapshot (bounded mode).
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private final boolean isBounded;

    // Cache CreateTableEvent instances by table for O(1) lookup.
    private final Map<io.debezium.relational.TableId, CreateTableEvent> createTableEventCache;

    public Db2PipelineRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            Db2SourceConfig sourceConfig,
            OffsetFactory offsetFactory,
            Db2Dialect db2Dialect) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        this.sourceConfig = sourceConfig;
        this.db2Dialect = db2Dialect;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = new HashMap<>();
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        // Rebuild schema cache from checkpointed split state before handling schema change
        // records.
        // The stream split checkpoints Debezium TableChange(s) (table schemas) and will be restored
        // on failover; the deserializer's local cache is runtime-only and must be reinitialized.
        if (isSchemaChangeEvent(element)
                && splitState.isStreamSplitState()
                && debeziumDeserializationSchema instanceof Db2EventDeserializer) {
            ((Db2EventDeserializer) debeziumDeserializationSchema)
                    .initializeTableSchemaCacheFromSplitSchemas(
                            splitState.asStreamSplitState().getTableSchemas());
        }

        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            // In snapshot mode, emit all schemas at once.
            ensureCreateTableEventsLoaded();
            emitAllCreateTableEvents(output);
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In the snapshot phase of INITIAL startup mode, lazily send CreateTableEvent
            // downstream to avoid checkpoint timeouts.
            io.debezium.relational.TableId tableId =
                    splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            emitCreateTableEventIfNeeded(tableId, output);
        } else if (isDataChangeRecord(element)) {
            // Handle data change events; schema change events are handled downstream directly.
            io.debezium.relational.TableId tableId = getTableId(element);
            emitCreateTableEventIfNeeded(tableId, output);
        }
        super.processElement(element, output, splitState);
    }

    @SuppressWarnings("unchecked")
    private void emitAllCreateTableEvents(SourceOutput<T> output) {
        createTableEventCache.forEach(
                (tableId, createTableEvent) -> {
                    output.collect((T) createTableEvent);
                    alreadySendCreateTableTables.add(tableId);
                });
    }

    @SuppressWarnings("unchecked")
    private void emitCreateTableEventIfNeeded(
            io.debezium.relational.TableId tableId, SourceOutput<T> output) {
        if (alreadySendCreateTableTables.contains(tableId)) {
            return;
        }

        CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
        if (createTableEvent != null) {
            output.collect((T) createTableEvent);
        } else {
            // The table is not in the cache, so fetch its schema from the database.
            try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
                createTableEvent = buildCreateTableEvent(jdbc, tableId);
                output.collect((T) createTableEvent);
                createTableEventCache.put(tableId, createTableEvent);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get table schema for " + tableId, e);
            }
        }
        alreadySendCreateTableTables.add(tableId);
    }

    private CreateTableEvent buildCreateTableEvent(
            Db2Connection jdbc, io.debezium.relational.TableId tableId) {
        Schema schema = Db2SchemaUtils.getTableSchema(tableId, jdbc, db2Dialect);
        return new CreateTableEvent(Db2SchemaUtils.toCdcTableId(tableId), schema);
    }

    private void ensureCreateTableEventsLoaded() {
        if (!createTableEventCache.isEmpty()) {
            return;
        }
        generateCreateTableEvents();
    }

    private void generateCreateTableEvents() {
        try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
            List<io.debezium.relational.TableId> capturedTableIds =
                    Db2ConnectionUtils.listTables(jdbc, sourceConfig.getTableFilters());
            for (io.debezium.relational.TableId tableId : capturedTableIds) {
                CreateTableEvent createTableEvent = buildCreateTableEvent(jdbc, tableId);
                createTableEventCache.put(tableId, createTableEvent);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
