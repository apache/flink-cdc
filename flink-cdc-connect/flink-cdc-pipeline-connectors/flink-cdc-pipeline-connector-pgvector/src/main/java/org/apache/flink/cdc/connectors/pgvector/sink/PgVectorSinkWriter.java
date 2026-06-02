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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorRecordUtils;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorSqlUtils;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorTableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** JDBC writer for pgvector sink. */
public class PgVectorSinkWriter implements SinkWriter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(PgVectorSinkWriter.class);

    private final PgVectorDataSinkConfig config;
    private final Map<TableId, Schema> schemas;
    private final Map<TableId, PreparedStatement> upsertStatements;
    private final Map<TableId, PreparedStatement> deleteStatements;
    private final List<PendingWrite> pendingWrites;
    private final PgVectorSinkMetrics metrics;
    private transient Connection connection;
    private long lastFlushTimestamp;

    public PgVectorSinkWriter(PgVectorDataSinkConfig config) throws IOException {
        this(config, PgVectorSinkMetrics.NOOP);
    }

    PgVectorSinkWriter(PgVectorDataSinkConfig config, PgVectorSinkMetrics metrics)
            throws IOException {
        this.config = config;
        this.schemas = new HashMap<>();
        this.upsertStatements = new HashMap<>();
        this.deleteStatements = new HashMap<>();
        this.pendingWrites = new ArrayList<>();
        this.metrics = metrics;
        this.lastFlushTimestamp = System.currentTimeMillis();
        try {
            openConnection();
        } catch (SQLException e) {
            throw new IOException("Failed to open PostgreSQL connection.", e);
        }
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        try {
            if (event instanceof SchemaChangeEvent) {
                flush();
                applySchemaChange((SchemaChangeEvent) event);
            } else if (event instanceof DataChangeEvent) {
                applyDataChange((DataChangeEvent) event);
            }
            flushIfNeeded();
        } catch (SQLException e) {
            throw new IOException("Failed to write event to pgvector sink: " + event, e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        try {
            flush();
        } catch (SQLException e) {
            throw new IOException("Failed to flush pgvector sink.", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            closeStatements();
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void openConnection() throws SQLException {
        connection =
                DriverManager.getConnection(
                        config.getJdbcUrl(), config.getUsername(), config.getPassword());
        connection.setAutoCommit(false);
    }

    private void applySchemaChange(SchemaChangeEvent event) throws SQLException {
        if (event instanceof CreateTableEvent) {
            schemas.put(event.tableId(), ((CreateTableEvent) event).getSchema());
        } else if (event instanceof DropTableEvent) {
            schemas.remove(event.tableId());
        } else {
            Schema oldSchema = schemas.get(event.tableId());
            if (oldSchema == null) {
                throw new IllegalStateException(
                        "Cannot apply schema change "
                                + event.getType()
                                + " for table "
                                + event.tableId()
                                + " because cached schema is missing. "
                                + "Ensure CREATE_TABLE is processed before other schema changes.");
            }
            schemas.put(event.tableId(), SchemaUtils.applySchemaChangeEvent(oldSchema, event));
        }
        resetStatements(event.tableId());
    }

    private void applyDataChange(DataChangeEvent event) throws SQLException {
        Schema schema = schemas.get(event.tableId());
        if (schema == null) {
            throw new IllegalStateException(
                    "Schema for table " + event.tableId() + " is missing in pgvector sink writer.");
        }
        if (schema.primaryKeys().isEmpty()) {
            if (!config.isAllowNoPrimaryKey()) {
                throw new IllegalStateException(
                        "pgvector sink requires primary keys for table " + event.tableId() + ".");
            }
            if (event.op() == OperationType.UPDATE || event.op() == OperationType.DELETE) {
                throw new IllegalStateException(
                        "pgvector sink only supports append-only INSERT/REPLACE events for "
                                + "tables without primary keys. Table: "
                                + event.tableId()
                                + ".");
            }
        }

        switch (event.op()) {
            case INSERT:
            case REPLACE:
                if (event.after() == null) {
                    throw new IllegalStateException(
                            "INSERT/REPLACE event must contain after record for table "
                                    + event.tableId()
                                    + ".");
                }
                addUpsert(event, schema);
                break;
            case UPDATE:
                if (event.before() == null || event.after() == null) {
                    throw new IllegalStateException(
                            "UPDATE event must contain before and after records for table "
                                    + event.tableId()
                                    + ".");
                }
                if (!PgVectorRecordUtils.primaryKeysEqual(schema, event.before(), event.after())) {
                    addDelete(event.tableId(), event.before(), schema);
                }
                addUpsert(event, schema);
                break;
            case DELETE:
                if (config.isDeleteEnabled()) {
                    if (event.before() == null) {
                        throw new IllegalStateException(
                                "DELETE event must contain before record for table "
                                        + event.tableId()
                                        + ".");
                    }
                    addDelete(event.tableId(), event.before(), schema);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + event.op());
        }
    }

    private void addUpsert(DataChangeEvent event, Schema schema) throws SQLException {
        pendingWrites.add(PendingWrite.upsert(event.tableId(), event.after()));
        metrics.setPendingRows(pendingWrites.size());
    }

    private void addDelete(TableId tableId, RecordData record, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalStateException("pgvector sink requires primary keys for DELETE.");
        }
        pendingWrites.add(PendingWrite.delete(tableId, record));
        metrics.setPendingRows(pendingWrites.size());
    }

    private void flushIfNeeded() throws SQLException {
        long now = System.currentTimeMillis();
        if (pendingWrites.size() >= config.getFlushMaxRows()
                || now - lastFlushTimestamp >= config.getFlushInterval().toMillis()) {
            flush();
        }
    }

    private void flush() throws SQLException {
        if (pendingWrites.isEmpty()) {
            return;
        }
        int rows = pendingWrites.size();
        long startMillis = System.currentTimeMillis();
        SQLException failure = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                ensureConnection();
                executePendingWritesAsOrderedBatches();
                connection.commit();
                pendingWrites.clear();
                lastFlushTimestamp = System.currentTimeMillis();
                metrics.recordSuccessfulFlush(rows, lastFlushTimestamp - startMillis);
                return;
            } catch (SQLException e) {
                failure = e;
                metrics.recordFlushFailure();
                safeRollback();
                clearBatchesQuietly();
                if (attempt < config.getMaxRetries()) {
                    metrics.recordRetry();
                    LOG.warn(
                            "Failed to flush pgvector batch. Retry {}/{}.",
                            attempt + 1,
                            config.getMaxRetries(),
                            e);
                    reconnectIfNeeded(e);
                    try {
                        sleepBeforeRetry();
                    } catch (SQLException retryFailure) {
                        metrics.recordRecordsOutErrors(rows);
                        throw retryFailure;
                    }
                }
            }
        }
        metrics.recordRecordsOutErrors(rows);
        throw failure;
    }

    private void ensureConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            reconnect();
            return;
        }
        if (!connection.isValid(2)) {
            reconnect();
        }
    }

    private void reconnectIfNeeded(SQLException failure) throws SQLException {
        if (shouldReconnect(failure) || connection == null || connection.isClosed()) {
            LOG.warn("Reconnecting to PostgreSQL after failed flush.", failure);
            reconnect();
        }
    }

    private void reconnect() throws SQLException {
        closeStatementsQuietly();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.debug("Failed to close broken PostgreSQL connection.", e);
            }
        }
        openConnection();
        metrics.recordReconnect();
        upsertStatements.clear();
        deleteStatements.clear();
    }

    private void sleepBeforeRetry() throws SQLException {
        long backoffMillis = config.getRetryBackoff().toMillis();
        if (backoffMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting to retry pgvector batch flush.", e);
        }
    }

    private boolean shouldReconnect(SQLException failure) {
        for (SQLException current = failure;
                current != null;
                current = current.getNextException()) {
            String sqlState = current.getSQLState();
            if (sqlState == null) {
                continue;
            }
            if (sqlState.startsWith("08")
                    || "57P01".equals(sqlState)
                    || "57P02".equals(sqlState)
                    || "57P03".equals(sqlState)) {
                return true;
            }
        }
        return false;
    }

    private void safeRollback() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            LOG.warn("Failed to rollback pgvector transaction.", e);
        }
    }

    private void executePendingWritesAsOrderedBatches() throws SQLException {
        PreparedStatement currentStatement = null;
        PendingOperation currentOperation = null;
        TableId currentTableId = null;

        for (PendingWrite pendingWrite : pendingWrites) {
            Schema schema = schemas.get(pendingWrite.tableId);
            if (schema == null) {
                throw new IllegalStateException(
                        "Schema for table "
                                + pendingWrite.tableId
                                + " is missing when flushing pgvector batch.");
            }
            if (currentStatement != null
                    && (currentOperation != pendingWrite.operation
                            || !currentTableId.equals(pendingWrite.tableId))) {
                currentStatement.executeBatch();
                currentStatement.clearBatch();
                currentStatement = null;
            }

            currentStatement = addPendingWriteToBatch(pendingWrite, schema);
            currentOperation = pendingWrite.operation;
            currentTableId = pendingWrite.tableId;
        }

        if (currentStatement != null) {
            currentStatement.executeBatch();
            currentStatement.clearBatch();
        }
    }

    private PreparedStatement addPendingWriteToBatch(PendingWrite pendingWrite, Schema schema)
            throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(pendingWrite.tableId, config.getDefaultSchema());
        if (pendingWrite.operation == PendingOperation.UPSERT) {
            PreparedStatement statement = getUpsertStatement(pendingWrite.tableId, schema);
            PgVectorRecordUtils.bindRecord(
                    connection,
                    statement,
                    tableInfo,
                    schema,
                    pendingWrite.record,
                    config.getVectorColumns());
            statement.addBatch();
            return statement;
        }

        PreparedStatement statement = getDeleteStatement(pendingWrite.tableId, schema);
        PgVectorRecordUtils.bindPrimaryKeys(
                connection,
                statement,
                tableInfo,
                schema,
                pendingWrite.record,
                config.getVectorColumns());
        statement.addBatch();
        return statement;
    }

    private void clearBatches() throws SQLException {
        for (PreparedStatement statement : upsertStatements.values()) {
            statement.clearBatch();
        }
        for (PreparedStatement statement : deleteStatements.values()) {
            statement.clearBatch();
        }
    }

    private void clearBatchesQuietly() {
        try {
            clearBatches();
        } catch (SQLException e) {
            LOG.warn("Failed to clear pgvector JDBC batches.", e);
        }
    }

    private PreparedStatement getUpsertStatement(TableId tableId, Schema schema)
            throws SQLException {
        PreparedStatement statement = upsertStatements.get(tableId);
        if (statement == null) {
            PgVectorTableInfo tableInfo =
                    PgVectorSqlUtils.resolveTableInfo(tableId, config.getDefaultSchema());
            statement = connection.prepareStatement(PgVectorSqlUtils.upsertSql(tableInfo, schema));
            upsertStatements.put(tableId, statement);
        }
        return statement;
    }

    private PreparedStatement getDeleteStatement(TableId tableId, Schema schema)
            throws SQLException {
        PreparedStatement statement = deleteStatements.get(tableId);
        if (statement == null) {
            PgVectorTableInfo tableInfo =
                    PgVectorSqlUtils.resolveTableInfo(tableId, config.getDefaultSchema());
            statement = connection.prepareStatement(PgVectorSqlUtils.deleteSql(tableInfo, schema));
            deleteStatements.put(tableId, statement);
        }
        return statement;
    }

    private void resetStatements(TableId tableId) throws SQLException {
        PreparedStatement upsert = upsertStatements.remove(tableId);
        if (upsert != null) {
            upsert.close();
        }
        PreparedStatement delete = deleteStatements.remove(tableId);
        if (delete != null) {
            delete.close();
        }
    }

    private void closeStatements() throws SQLException {
        for (PreparedStatement statement : upsertStatements.values()) {
            statement.close();
        }
        for (PreparedStatement statement : deleteStatements.values()) {
            statement.close();
        }
        upsertStatements.clear();
        deleteStatements.clear();
    }

    private void closeStatementsQuietly() {
        try {
            closeStatements();
        } catch (SQLException e) {
            LOG.warn("Failed to close pgvector prepared statements.", e);
        }
    }

    private enum PendingOperation {
        UPSERT,
        DELETE
    }

    private static class PendingWrite {

        private final PendingOperation operation;
        private final TableId tableId;
        private final RecordData record;

        private PendingWrite(PendingOperation operation, TableId tableId, RecordData record) {
            this.operation = operation;
            this.tableId = tableId;
            this.record = record;
        }

        private static PendingWrite upsert(TableId tableId, RecordData record) {
            return new PendingWrite(PendingOperation.UPSERT, tableId, record);
        }

        private static PendingWrite delete(TableId tableId, RecordData record) {
            return new PendingWrite(PendingOperation.DELETE, tableId, record);
        }
    }
}
