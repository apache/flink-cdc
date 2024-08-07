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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.connectors.jdbc.catalog.Catalog;
import org.apache.flink.cdc.connectors.jdbc.sink.utils.JsonWrapper;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation class of the {@link StatefulSink.StatefulSinkWriter} interface. */
public class JdbcWriter<IN> implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final JdbcExecutionOptions executionOptions;

    private final JdbcConnectionProvider connectionProvider;

    private final JdbcOutputSerializer<Object> outputSerializer;
    private final RecordSerializationSchema<IN> serializationSchema;

    private final JsonWrapper jsonWrapper;

    // Catalog is unSerializable.
    private final Catalog catalog;
    private final Map<
                    TableId,
                    JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>>
            jdbcUpsertOutputs;

    private final Map<
                    TableId,
                    JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>>
            jdbcDeleteOutputs;

    private JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> jdbcOutput;

    public JdbcWriter(
            Sink.InitContext initContext,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionProvider connectionProvider,
            JdbcOutputSerializer<Object> outputSerializer,
            RecordSerializationSchema<IN> serializationSchema,
            Catalog catalog)
            throws IOException {

        checkNotNull(initContext, "initContext must be defined");
        checkNotNull(executionOptions, "executionOptions must be defined");
        checkNotNull(connectionProvider, "connectionProvider must be defined");
        checkNotNull(outputSerializer, "outputSerializer must be defined");
        checkNotNull(serializationSchema, "serializationSchema must be defined");

        this.jsonWrapper = new JsonWrapper();
        this.executionOptions = executionOptions;
        this.connectionProvider = connectionProvider;
        this.outputSerializer = outputSerializer;
        this.serializationSchema = serializationSchema;
        this.catalog = catalog;
        this.jdbcUpsertOutputs = new ConcurrentHashMap<>();
        this.jdbcDeleteOutputs = new ConcurrentHashMap<>();
    }

    @Override
    public List<JdbcWriterState> snapshotState(long l) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void write(IN event, Context context) throws IOException {
        JdbcRowData rowData = serializationSchema.serialize(event);
        if (rowData == null) {
            return;
        }

        if (RowKind.SCHEMA_CHANGE.is(rowData.getRowKind())) {
            TableId tableId = rowData.getTableId();

            // Close and remove from jdbcUpsertOutputs
            JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                    upsertOutput = jdbcUpsertOutputs.get(tableId);
            if (upsertOutput != null) {
                upsertOutput.close();
                jdbcUpsertOutputs.remove(tableId);
            }

            // Close and remove from jdbcDeleteOutputs
            JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                    deleteOutput = jdbcDeleteOutputs.get(tableId);
            if (deleteOutput != null) {
                deleteOutput.close();
                jdbcDeleteOutputs.remove(tableId);
            }

            return;
        }

        // insert event
        if (RowKind.INSERT.is(rowData.getRowKind())) {
            String upsertSmt =
                    catalog.getUpsertStatement(rowData.getTableId(), rowData.getSchema());
            JdbcStatementBuilder<JdbcRowData> upsertSmtBuilder =
                    getUpsertStatementBuilder(rowData.getSchema().getColumns());

            JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                    outputFormat =
                            jdbcUpsertOutputs.computeIfAbsent(
                                    rowData.getTableId(),
                                    id -> getJdbcOutputFormat(upsertSmt, upsertSmtBuilder));

            outputFormat.writeRecord(rowData);
            outputFormat.flush();
            LOG.info(
                    "Success to upsert column table {}.{}, sql: {}",
                    rowData.getTableId().getSchemaName(),
                    rowData.getTableId().getTableName(),
                    upsertSmt);
        }

        // delete event
        if (RowKind.DELETE.is(rowData.getRowKind())) {
            String deleteSmt =
                    catalog.getDeleteStatement(
                            rowData.getTableId(), rowData.getSchema().primaryKeys());
            JdbcStatementBuilder<JdbcRowData> delSmtBuilder =
                    getDeleteStatementBuilder(rowData.getSchema().primaryKeys());

            JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                    outputFormat =
                            jdbcDeleteOutputs.computeIfAbsent(
                                    rowData.getTableId(),
                                    id -> getJdbcOutputFormat(deleteSmt, delSmtBuilder));

            outputFormat.writeRecord(rowData);
            outputFormat.flush();
            LOG.info(
                    "Success to delete column table {}.{}, sql: {}",
                    rowData.getTableId().getSchemaName(),
                    rowData.getTableId().getTableName(),
                    deleteSmt);
        }
    }

    private JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
            getJdbcOutputFormat(String sql, JdbcStatementBuilder<JdbcRowData> builder) {
        JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                jdbcOutputFormat =
                        new JdbcOutputFormat<>(
                                connectionProvider,
                                executionOptions,
                                () -> JdbcBatchStatementExecutor.simple(sql, builder));
        try {
            jdbcOutputFormat.open(outputSerializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jdbcOutputFormat;
    }

    @Override
    public void flush(boolean b) throws IOException, RuntimeException {
        // To ensure the sequential execution of insert and delete,
        // PrepareStatements cannot perform insert and delete batch operations at the same time,
        // so flush operations are not used separately.
    }

    @Override
    public void close() throws Exception {
        jdbcUpsertOutputs.forEach(
                (key, value) -> {
                    value.close();
                    jdbcUpsertOutputs.remove(key);
                });

        jdbcDeleteOutputs.forEach(
                (key, value) -> {
                    value.close();
                    jdbcUpsertOutputs.remove(key);
                });
    }

    private JdbcStatementBuilder<JdbcRowData> getUpsertStatementBuilder(List<Column> columns) {
        return (ps, rowData) -> {
            Map<String, Object> recordMap = parserRowData(rowData.getRows());

            if (!recordMap.isEmpty()) {
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    ps.setObject(i + 1, recordMap.get(column.getName()));
                }
            }
        };
    }

    private JdbcStatementBuilder<JdbcRowData> getDeleteStatementBuilder(List<String> primaryKeys) {
        return (ps, rowData) -> {
            Map<String, Object> recordMap = parserRowData(rowData.getRows());

            if (!recordMap.isEmpty()) {
                for (int i = 0; i < primaryKeys.size(); i++) {
                    String pk = primaryKeys.get(i);
                    ps.setObject(i + 1, recordMap.get(pk));
                }
            }
        };
    }

    private Map<String, Object> parserRowData(byte[] rowData) {
        try {
            return jsonWrapper.parseObject(rowData, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
