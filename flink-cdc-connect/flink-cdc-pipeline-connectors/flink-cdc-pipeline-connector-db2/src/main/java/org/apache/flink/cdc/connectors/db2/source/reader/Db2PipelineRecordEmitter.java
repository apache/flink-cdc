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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils.createDb2Connection;

/** The {@link RecordEmitter} implementation for pipeline oracle connector. */
public class Db2PipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final Db2SourceConfig sourceConfig;
    private final Db2Dialect db2Dialect;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForBinlogSplit = false;
    private final List<CreateTableEvent> createTableEventCache;

    public Db2PipelineRecordEmitter(
            DebeziumDeserializationSchema debeziumDeserializationSchema,
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
        this.createTableEventCache = new ArrayList<>();

        if (!sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
                List<TableId> capturedTableIds =
                        Db2ConnectionUtils.listTables(jdbc, sourceConfig.getTableFilters());
                for (TableId tableId : capturedTableIds) {
                    Schema schema = Db2SchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
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

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (Db2Connection jdbc =
                        createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
                    sendCreateTableEvent(jdbc, tableId, (SourceOutput<Event>) output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else if (splitState.isStreamSplitState()
                && !alreadySendCreateTableForBinlogSplit
                && !sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            for (CreateTableEvent createTableEvent : createTableEventCache) {
                output.collect((T) createTableEvent);
            }
            alreadySendCreateTableForBinlogSplit = true;
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            Db2Connection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = Db2SchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
        output.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.schema(), tableId.table()),
                        schema));
    }
}
