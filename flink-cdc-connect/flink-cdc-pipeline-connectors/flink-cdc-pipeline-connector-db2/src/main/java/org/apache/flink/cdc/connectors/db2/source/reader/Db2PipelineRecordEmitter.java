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
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** The {@link RecordEmitter} implementation for Db2 pipeline connector. */
public class Db2PipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

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
        CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
        if (createTableEvent == null) {
            throw new IllegalStateException(
                    "Missing CreateTableEvent for table "
                            + tableId
                            + ". Table schema should have been restored before processing records.");
        }
        output.collect((T) createTableEvent);
        alreadySendCreateTableTables.add(tableId);
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
