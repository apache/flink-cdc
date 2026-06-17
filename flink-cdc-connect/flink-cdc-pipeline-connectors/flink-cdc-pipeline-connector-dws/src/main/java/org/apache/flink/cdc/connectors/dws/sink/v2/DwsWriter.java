/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.dws.utils.DwsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A SinkV2 writer that writes records into checkpoint-scoped DWS staging tables. */
public class DwsWriter
        implements CommittingSinkWriter<Event, DwsCommittable>,
                StatefulSinkWriter<Event, DwsWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(DwsWriter.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final ZoneId zoneId;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final boolean enableDelete;
    private final String jobId;
    private final int subtaskId;
    private final DwsWriterState stateCache;
    private final Map<TableId, TableInfo> tableInfoCache = new HashMap<>();
    private final Map<TableId, StagingTable> stagingTables = new HashMap<>();
    private final List<DwsCommittable> sealedCommittables = new ArrayList<>();

    private transient Connection connection;
    private long lastCheckpointId;
    private long sequence;
    private int stagingTableSequence;

    public DwsWriter(
            String jdbcUrl,
            String username,
            String password,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            boolean enableDelete,
            String jobId,
            int subtaskId,
            long lastCheckpointId) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.zoneId = zoneId;
        this.caseSensitive = caseSensitive;
        this.defaultSchema = defaultSchema;
        this.enableDelete = enableDelete;
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.lastCheckpointId = lastCheckpointId;
        this.stateCache = new DwsWriterState(jobId);
    }

    @Override
    public void write(Event event, Context context) throws IOException, InterruptedException {
        if (event instanceof DataChangeEvent) {
            processDataChangeEvent((DataChangeEvent) event);
        } else if (event instanceof SchemaChangeEvent) {
            handleSchemaChangeEvent((SchemaChangeEvent) event);
        }
    }

    @Override
    public Collection<DwsCommittable> prepareCommit() throws IOException, InterruptedException {
        List<DwsCommittable> committables = new ArrayList<>(sealedCommittables);
        sealedCommittables.clear();

        List<TableId> tableIds = new ArrayList<>(stagingTables.keySet());
        for (TableId tableId : tableIds) {
            DwsCommittable committable = sealStagingTable(tableId);
            if (committable != null) {
                committables.add(committable);
            }
        }

        lastCheckpointId++;
        sequence = 0L;
        stagingTableSequence = 0;
        LOG.debug(
                "Prepared {} DWS committables for checkpoint {}.",
                committables.size(),
                lastCheckpointId);
        return committables;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        for (StagingTable stagingTable : stagingTables.values()) {
            flushStagingTable(stagingTable);
        }
    }

    @Override
    public void close() throws Exception {
        for (StagingTable stagingTable : stagingTables.values()) {
            closeStagingTable(stagingTable);
        }
        stagingTables.clear();
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @Override
    public List<DwsWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(stateCache);
    }

    private void processDataChangeEvent(DataChangeEvent event) throws IOException {
        RecordData recordData;
        String operation;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                recordData = event.after();
                operation = DwsSqlUtils.UPSERT_OPERATION;
                break;
            case DELETE:
                if (!enableDelete) {
                    LOG.debug(
                            "Skip DELETE for {} because sink.enable-delete=false", event.tableId());
                    return;
                }
                recordData = event.before();
                operation = DwsSqlUtils.DELETE_OPERATION;
                break;
            default:
                LOG.warn("Unsupported operation {} for {}", event.op(), event.tableId());
                return;
        }

        if (recordData == null) {
            LOG.warn(
                    "Skip {} for {} because the record payload is null.",
                    operation,
                    event.tableId());
            return;
        }

        TableInfo tableInfo = tableInfoCache.get(event.tableId());
        if (tableInfo == null) {
            throw new IOException("Table schema cache is missing for " + event.tableId());
        }
        Preconditions.checkArgument(tableInfo.schema.getColumnCount() == recordData.getArity());

        StagingTable stagingTable = getOrCreateStagingTable(event.tableId(), tableInfo);
        try {
            PreparedStatement statement = stagingTable.insertStatement;
            statement.setString(1, operation);
            statement.setLong(2, sequence++);
            for (int i = 0; i < tableInfo.schema.getColumnCount(); i++) {
                Object fieldValue = tableInfo.fieldGetters[i].getFieldOrNull(recordData);
                statement.setObject(i + 3, fieldValue);
            }
            statement.addBatch();
            stagingTable.hasPendingBatch = true;
            stagingTable.rowCount++;
        } catch (Exception e) {
            throw new IOException("Failed to write record into DWS staging table.", e);
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
        DwsCommittable sealed = sealStagingTable(event.tableId());
        if (sealed != null) {
            sealedCommittables.add(sealed);
        }

        if (event instanceof DropTableEvent) {
            tableInfoCache.remove(event.tableId());
            return;
        }

        if (event instanceof TruncateTableEvent) {
            return;
        }

        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo currentTableInfo = tableInfoCache.get(event.tableId());
            if (currentTableInfo == null) {
                throw new IOException("Schema cache is missing for " + event.tableId());
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(currentTableInfo.schema, event);
        }
        tableInfoCache.put(event.tableId(), createTableInfo(newSchema));
    }

    private StagingTable getOrCreateStagingTable(TableId tableId, TableInfo tableInfo)
            throws IOException {
        StagingTable existing = stagingTables.get(tableId);
        if (existing != null) {
            return existing;
        }

        if (tableInfo.schema.primaryKeys().isEmpty()) {
            throw new IOException(
                    "DWS application-level two-phase commit requires primary keys for "
                            + tableId.identifier());
        }

        String targetSchema =
                DwsSqlUtils.normalizeSchemaName(tableId, defaultSchema, caseSensitive);
        String targetTable = DwsSqlUtils.normalizeTableName(tableId, caseSensitive);
        long checkpointId = lastCheckpointId + 1;
        String stagingTableName =
                DwsSqlUtils.buildStagingTableName(
                        targetSchema,
                        targetTable,
                        jobId,
                        checkpointId,
                        subtaskId,
                        stagingTableSequence++,
                        caseSensitive);
        List<String> columnNames =
                tableInfo.schema.getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        List<String> primaryKeys = new ArrayList<>(tableInfo.schema.primaryKeys());

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(
                    DwsSqlUtils.buildCreateStagingTableSql(
                            targetSchema, stagingTableName, tableInfo.schema, caseSensitive));
        } catch (SQLException e) {
            throw new IOException("Failed to create DWS staging table.", e);
        }

        try {
            PreparedStatement insertStatement =
                    getConnection()
                            .prepareStatement(
                                    DwsSqlUtils.buildInsertStagingSql(
                                            targetSchema,
                                            stagingTableName,
                                            columnNames,
                                            caseSensitive));
            StagingTable stagingTable =
                    new StagingTable(
                            targetSchema,
                            targetTable,
                            stagingTableName,
                            columnNames,
                            primaryKeys,
                            insertStatement);
            stagingTables.put(tableId, stagingTable);
            LOG.debug(
                    "Created DWS staging table {}.{} for checkpoint {}.",
                    targetSchema,
                    stagingTableName,
                    checkpointId);
            return stagingTable;
        } catch (SQLException e) {
            throw new IOException("Failed to prepare DWS staging insert statement.", e);
        }
    }

    private DwsCommittable sealStagingTable(TableId tableId) throws IOException {
        StagingTable stagingTable = stagingTables.remove(tableId);
        if (stagingTable == null) {
            return null;
        }

        try {
            flushStagingTable(stagingTable);
            if (stagingTable.rowCount == 0) {
                return null;
            }
            return new DwsCommittable(
                    jobId,
                    lastCheckpointId + 1,
                    subtaskId,
                    stagingTable.targetSchema,
                    stagingTable.targetTable,
                    stagingTable.targetSchema,
                    stagingTable.stagingTable,
                    stagingTable.columnNames,
                    stagingTable.primaryKeys);
        } finally {
            closeStagingTable(stagingTable);
        }
    }

    private void flushStagingTable(StagingTable stagingTable) throws IOException {
        if (!stagingTable.hasPendingBatch) {
            return;
        }
        try {
            stagingTable.insertStatement.executeBatch();
            stagingTable.insertStatement.clearBatch();
            stagingTable.hasPendingBatch = false;
        } catch (SQLException e) {
            throw new IOException(
                    "Failed to flush DWS staging table " + stagingTable.stagingTable, e);
        }
    }

    private void closeStagingTable(StagingTable stagingTable) throws IOException {
        try {
            stagingTable.insertStatement.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close DWS staging statement.", e);
        }
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(true);
        }
        return connection;
    }

    private TableInfo createTableInfo(Schema schema) {
        RecordData.FieldGetter[] fieldGetters = new RecordData.FieldGetter[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters[i] =
                    DwsUtils.createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId);
        }
        return new TableInfo(schema, fieldGetters);
    }

    private static class TableInfo {

        private final Schema schema;
        private final RecordData.FieldGetter[] fieldGetters;

        private TableInfo(Schema schema, RecordData.FieldGetter[] fieldGetters) {
            this.schema = schema;
            this.fieldGetters = fieldGetters;
        }
    }

    private static class StagingTable {

        private final String targetSchema;
        private final String targetTable;
        private final String stagingTable;
        private final List<String> columnNames;
        private final List<String> primaryKeys;
        private final PreparedStatement insertStatement;

        private long rowCount;
        private boolean hasPendingBatch;

        private StagingTable(
                String targetSchema,
                String targetTable,
                String stagingTable,
                List<String> columnNames,
                List<String> primaryKeys,
                PreparedStatement insertStatement) {
            this.targetSchema = targetSchema;
            this.targetTable = targetTable;
            this.stagingTable = stagingTable;
            this.columnNames = columnNames;
            this.primaryKeys = primaryKeys;
            this.insertStatement = insertStatement;
        }
    }
}
