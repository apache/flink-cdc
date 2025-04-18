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

package com.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import com.apache.flink.cdc.connectors.oracle.utils.DebeziumUtils;
import com.apache.flink.cdc.connectors.oracle.utils.OracleSchemaUtils;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog for
 * PostgreSQL.
 */
public class OraclePipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event> {
    private static final long serialVersionUID = 1L;
    private List<String> tableList;
    private OracleSourceConfig sourceConfig;
    private List<CreateTableEvent> createTableEventCache = null;
    private boolean alreadySendCreateTableForBinlogSplit = false;

    public OraclePipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            OracleSourceConfig sourceConfig) {
        super(debeziumDeserializationSchema, null, includeSchemaChanges, offsetFactory);
        this.tableList = sourceConfig.getTableList();
        this.sourceConfig = sourceConfig;
        this.createTableEventCache = new ArrayList<>();
        try (JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig)) {

            List<TableId> capturedTableIds = new ArrayList<>();
            for (String table : tableList) {
                TableId capturedTableId = TableId.parse(table.toUpperCase(Locale.ROOT));
                capturedTableIds.add(capturedTableId);
            }
            for (TableId tableId : capturedTableIds) {
                Schema schema = OracleSchemaUtils.getSchema(jdbc, tableId);
                createTableEventCache.add(
                        new CreateTableEvent(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        tableId.catalog(), tableId.table()),
                                schema));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, SourceSplitState splitState)
            throws Exception {
        if (!alreadySendCreateTableForBinlogSplit) {
            createTableEventCache.forEach(output::collect);
            alreadySendCreateTableForBinlogSplit = true;
        }
        super.processElement(element, output, splitState);
    }
}
