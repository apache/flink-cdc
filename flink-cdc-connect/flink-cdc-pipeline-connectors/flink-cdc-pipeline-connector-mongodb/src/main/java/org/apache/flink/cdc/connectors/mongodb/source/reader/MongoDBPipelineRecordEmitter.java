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

package org.apache.flink.cdc.connectors.mongodb.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.mongodb.source.SchemaParseMode;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;

/** The {@link RecordEmitter} implementation for pipeline mongodb connector. */
public class MongoDBPipelineRecordEmitter extends MongoDBRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBPipelineRecordEmitter.class);

    private final SchemaParseMode schemaParseMode;
    private final MongoDBSourceConfig sourceConfig;
    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForBinlogSplit = false;
    private List<CreateTableEvent> createTableEventCache;

    public MongoDBPipelineRecordEmitter(
            SchemaParseMode schemaParseMode,
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            MongoDBSourceConfig sourceConfig,
            OffsetFactory offsetFactory) {
        super(debeziumDeserializationSchema, sourceReaderMetrics, offsetFactory);
        this.schemaParseMode = schemaParseMode;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.sourceConfig = sourceConfig;
        this.createTableEventCache = Collections.emptyList();
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, SourceSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, we lazily send CreateTableEvent to
            // downstream to avoid checkpoint timeout.
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                sendCreateTableEvent(tableId, output);
                alreadySendCreateTableTables.add(tableId);
            }
        } else if (splitState.isStreamSplitState() && !alreadySendCreateTableForBinlogSplit) {
            alreadySendCreateTableForBinlogSplit = true;
            if (sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
                // In Snapshot -> Stream transition of INITIAL startup mode, ensure all table
                // schemas have been sent to downstream. We use previously cached schema instead of
                // re-request latest schema because there might be some pending schema change events
                // in the queue, and that may accidentally emit evolved schema before corresponding
                // schema change events.
                createTableEventCache.stream()
                        .filter(
                                event ->
                                        !alreadySendCreateTableTables.contains(
                                                MongoDBSchemaUtils.toDbzTableId(event.tableId())))
                        .forEach(output::collect);
            } else {
                // In Stream only mode, we simply emit all schemas at once.
                createTableEventCache.forEach(output::collect);
            }
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(TableId tableId, SourceOutput<Event> output) {
        Schema schema = getSchema(tableId);
        output.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.catalog(), tableId.table()),
                        schema));
    }

    private Schema getSchema(TableId tableId) {
        if (schemaParseMode == SchemaParseMode.SCHEMA_LESS) {
            return MongoDBSchemaUtils.getJsonSchema();
        }
        throw new UnsupportedOperationException("Unsupported schema parse mode.");
    }
}
