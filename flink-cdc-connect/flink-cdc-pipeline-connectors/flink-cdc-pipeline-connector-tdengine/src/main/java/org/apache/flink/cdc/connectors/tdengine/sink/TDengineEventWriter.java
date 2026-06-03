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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.tdengine.serde.TDengineRowConverter;
import org.apache.flink.cdc.connectors.tdengine.serde.TDengineRowData;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineRetryUtils;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineSqlUtils;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** JDBC writer for TDengine sink. */
public class TDengineEventWriter implements SinkWriter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(TDengineEventWriter.class);

    private final TDengineDataSinkConfig config;
    private final Map<TableId, Schema> schemas;
    private final Map<TableId, TDengineRowConverter> converters;
    private final List<TDengineRowData> pendingRows;
    private final TDengineSinkMetrics metrics;
    private final TDengineClientFactory clientFactory;
    private transient TDengineClientWrapper client;
    private long lastFlushTimestamp;

    public TDengineEventWriter(TDengineDataSinkConfig config) throws IOException {
        this(config, TDengineSinkMetrics.NOOP);
    }

    TDengineEventWriter(TDengineDataSinkConfig config, TDengineSinkMetrics metrics)
            throws IOException {
        this(config, metrics, DefaultTDengineClientWrapper::new);
    }

    TDengineEventWriter(
            TDengineDataSinkConfig config,
            TDengineSinkMetrics metrics,
            TDengineClientFactory clientFactory)
            throws IOException {
        this.config = config;
        this.schemas = new HashMap<>();
        this.converters = new HashMap<>();
        this.pendingRows = new ArrayList<>();
        this.metrics = metrics;
        this.clientFactory = clientFactory;
        this.lastFlushTimestamp = System.currentTimeMillis();
        try {
            reconnect();
        } catch (SQLException e) {
            throw new IOException("Failed to open TDengine connection.", e);
        }
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        try {
            if (event instanceof SchemaChangeEvent) {
                flushInternal();
                applySchemaChange((SchemaChangeEvent) event);
            } else if (event instanceof DataChangeEvent) {
                applyDataChange((DataChangeEvent) event);
            }
            flushIfNeeded();
        } catch (SQLException | RuntimeException e) {
            throw new IOException("Failed to write event to TDengine sink: " + event, e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        try {
            flushInternal();
        } catch (SQLException e) {
            throw new IOException("Failed to flush TDengine sink.", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flushInternal();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            schemas.put(event.tableId(), ((CreateTableEvent) event).getSchema());
        } else {
            Schema oldSchema = schemas.get(event.tableId());
            if (oldSchema == null) {
                throw new IllegalStateException(
                        "Cannot apply schema change "
                                + event.getType()
                                + " for table "
                                + event.tableId()
                                + " because cached schema is missing.");
            }
            schemas.put(event.tableId(), SchemaUtils.applySchemaChangeEvent(oldSchema, event));
        }
        converters.remove(event.tableId());
    }

    private void applyDataChange(DataChangeEvent event) {
        Schema schema = schemas.get(event.tableId());
        if (schema == null) {
            throw new IllegalStateException(
                    "Schema for table " + event.tableId() + " is missing in TDengine sink writer.");
        }
        TDengineRowConverter converter = converter(event.tableId(), schema);
        switch (event.op()) {
            case INSERT:
            case REPLACE:
                requireAfter(event);
                addInsert(converter.convert(event.after()));
                break;
            case UPDATE:
                applyUpdate(event, converter);
                break;
            case DELETE:
                applyDelete(event);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + event.op());
        }
    }

    private void applyUpdate(DataChangeEvent event, TDengineRowConverter converter) {
        if ("reject".equals(config.getUpdateMode())) {
            throw new IllegalStateException(
                    "TDengine sink rejects UPDATE events by configuration.");
        }
        requireBeforeAndAfter(event);
        if (!converter.timestampEquals(event.before(), event.after())
                && "reject".equals(config.getTimestampChangeMode())) {
            throw new IllegalStateException(
                    "TDengine sink rejects UPDATE events that change timestamp.field by default.");
        }
        if (!converter.subtableEquals(event.before(), event.after())
                && "reject".equals(config.getSubtableChangeMode())) {
            throw new IllegalStateException(
                    "TDengine sink rejects UPDATE events that change subtable.field by default.");
        }
        addInsert(converter.convert(event.after()));
    }

    private void applyDelete(DataChangeEvent event) {
        if ("ignore".equals(config.getDeleteMode())) {
            return;
        }
        throw new IllegalStateException(
                "TDengine sink rejects DELETE events by default because TDengine does not support single-row DELETE.");
    }

    private void addInsert(TDengineRowData rowData) {
        pendingRows.add(rowData);
        metrics.setPendingRows(pendingRows.size());
    }

    private void flushIfNeeded() throws SQLException {
        long now = System.currentTimeMillis();
        if (pendingRows.size() >= config.getFlushMaxRows()
                || now - lastFlushTimestamp >= config.getFlushInterval().toMillis()) {
            flushInternal();
        }
    }

    private void flushInternal() throws SQLException {
        if (pendingRows.isEmpty()) {
            return;
        }
        int originalRows = pendingRows.size();
        long startMillis = System.currentTimeMillis();
        SQLException failure = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                ensureClient();
                executePendingRows();
                lastFlushTimestamp = System.currentTimeMillis();
                metrics.recordSuccessfulFlush(originalRows, lastFlushTimestamp - startMillis);
                return;
            } catch (SQLException e) {
                failure = e;
                metrics.recordFlushFailure();
                if (attempt < config.getMaxRetries() && TDengineRetryUtils.isTransient(e)) {
                    metrics.recordRetry();
                    LOG.warn(
                            "Failed to flush TDengine batch. Retry {}/{}.",
                            attempt + 1,
                            config.getMaxRetries(),
                            e);
                    reconnectQuietly();
                    sleepBeforeRetry();
                } else {
                    break;
                }
            }
        }
        metrics.recordRecordsOutErrors(pendingRows.size());
        throw failure;
    }

    private void executePendingRows() throws SQLException {
        while (!pendingRows.isEmpty()) {
            TDengineRowData firstRow = pendingRows.get(0);
            TDengineTableInfo currentTarget = firstRow.getTableInfo();
            int targetRows = 1;
            while (targetRows < pendingRows.size()
                    && TDengineSqlUtils.sameTarget(firstRow, pendingRows.get(targetRows))) {
                targetRows++;
            }
            executeTargetRows(currentTarget, targetRows);
        }
    }

    private void executeTargetRows(TDengineTableInfo target, int targetRows) throws SQLException {
        int remainingRows = targetRows;
        while (remainingRows > 0) {
            List<TDengineRowData> splitRows = new ArrayList<>();
            int splitSqlBytes = 0;
            for (int index = 0; index < remainingRows; index++) {
                TDengineRowData row = pendingRows.get(index);
                int rowSqlBytes = TDengineSqlUtils.estimateInsertSqlBytes(target, row);
                if (!splitRows.isEmpty() && splitSqlBytes + rowSqlBytes > config.getMaxSqlBytes()) {
                    break;
                }
                splitRows.add(row);
                splitSqlBytes += rowSqlBytes;
            }
            client.execute(TDengineSqlUtils.insertSql(target, splitRows));
            removeExecutedRows(splitRows.size());
            remainingRows -= splitRows.size();
        }
    }

    private void removeExecutedRows(int executedRows) {
        pendingRows.subList(0, executedRows).clear();
        metrics.setPendingRows(pendingRows.size());
    }

    private void ensureClient() throws SQLException {
        if (client == null || !client.isValid()) {
            reconnect();
        }
    }

    private void reconnect() throws SQLException {
        if (client != null) {
            try {
                client.close();
            } catch (SQLException e) {
                LOG.debug("Failed to close broken TDengine connection.", e);
            }
        }
        client = clientFactory.create(config);
        metrics.recordReconnect();
    }

    private void reconnectQuietly() {
        try {
            reconnect();
        } catch (SQLException e) {
            LOG.warn("Failed to reconnect TDengine client before retry.", e);
        }
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
            throw new SQLException("Interrupted while waiting to retry TDengine batch flush.", e);
        }
    }

    private TDengineRowConverter converter(TableId tableId, Schema schema) {
        return converters.computeIfAbsent(
                tableId, ignored -> new TDengineRowConverter(tableId, schema, config));
    }

    private void requireAfter(DataChangeEvent event) {
        if (event.after() == null) {
            throw new IllegalStateException(event.op() + " event must contain after record.");
        }
    }

    private void requireBeforeAndAfter(DataChangeEvent event) {
        if (event.before() == null || event.after() == null) {
            throw new IllegalStateException("UPDATE event must contain before and after records.");
        }
    }
}
