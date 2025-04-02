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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialect;
import org.apache.flink.cdc.connectors.jdbc.sink.utils.JsonWrapper;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.jdbc.sink.v2.RowKind.PK_ROW;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation class of the {@link StatefulSink.StatefulSinkWriter} interface. */
public class JdbcWriter<IN> implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final JdbcExecutionOptions executionOptions;
    private final JdbcConnectionProvider connectionProvider;
    private final JdbcOutputSerializer<Object> outputSerializer;
    private final RecordSerializationSchema<IN> serializationSchema;
    private final JsonWrapper jsonWrapper;

    private final JdbcSinkDialect dialect;
    private final Map<TableId, RichJdbcOutputFormat> outputHandlers;

    public JdbcWriter(
            Sink.InitContext initContext,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionProvider connectionProvider,
            JdbcOutputSerializer<Object> outputSerializer,
            RecordSerializationSchema<IN> serializationSchema,
            JdbcSinkDialect dialect,
            JdbcSinkConfig sinkConfig) {

        checkNotNull(initContext, "initContext must be defined");
        checkNotNull(executionOptions, "executionOptions must be defined");
        checkNotNull(connectionProvider, "connectionProvider must be defined");
        checkNotNull(outputSerializer, "outputSerializer must be defined");
        checkNotNull(serializationSchema, "serializationSchema must be defined");
        checkNotNull(sinkConfig, "sinkConfig must be defined");

        this.jsonWrapper = new JsonWrapper();
        this.executionOptions = executionOptions;
        this.connectionProvider = connectionProvider;
        this.outputSerializer = outputSerializer;
        this.serializationSchema = serializationSchema;
        this.dialect = dialect;
        this.outputHandlers = new ConcurrentHashMap<>();
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) {
        // Jdbc sink supports at-least-once semantic only. No state snapshotting & restoring
        // required.
        return Collections.emptyList();
    }

    @Override
    public void write(IN event, Context context) throws IOException {
        JdbcRowData[] rowDataElements = serializationSchema.serialize(event);
        for (JdbcRowData rowData : rowDataElements) {
            TableId tableId = rowData.getTableId();
            if (RowKind.SCHEMA_CHANGE.is(rowData.getRowKind())) {
                // All previous outputHandlers would expire after schema changes.
                flush(false);
                Optional.ofNullable(outputHandlers.remove(tableId))
                        .ifPresent(JdbcOutputFormat::close);
            } else {
                RichJdbcOutputFormat outputFormat =
                        getOrCreateHandler(tableId, rowData.getSchema());
                outputFormat.writeRecord(rowData);
                if (!rowData.hasPrimaryKey()) {
                    // For non-PK table, we must flush immediately to avoid data consistency issues.
                    outputFormat.flush();
                }
            }
        }
    }

    private RichJdbcOutputFormat getJdbcOutputFormat(
            JdbcSinkDialect dialect, TableId tableId, Schema schema) {
        RichJdbcOutputFormat jdbcOutputFormat =
                new RichJdbcOutputFormat(
                        connectionProvider,
                        executionOptions,
                        () -> createBatchedStatementExecutor(dialect, tableId, schema));
        try {
            jdbcOutputFormat.open(outputSerializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jdbcOutputFormat;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        for (RichJdbcOutputFormat handler : outputHandlers.values()) {
            handler.flush();
            if (endOfInput) {
                handler.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        flush(true);
    }

    private RichJdbcOutputFormat getOrCreateHandler(TableId tableId, Schema schema) {
        if (outputHandlers.containsKey(tableId)) {
            return outputHandlers.get(tableId);
        }
        RichJdbcOutputFormat outputFormat = getJdbcOutputFormat(dialect, tableId, schema);
        outputHandlers.put(tableId, outputFormat);
        return outputFormat;
    }

    private JdbcStatementBuilder<JdbcRowData> getStatementBuilder(List<String> primaryKeys) {
        return (ps, rowData) -> {
            Map<String, Object> recordMap = parseRowData(rowData.getRows());

            if (!recordMap.isEmpty()) {
                for (int i = 0; i < primaryKeys.size(); i++) {
                    String pk = primaryKeys.get(i);
                    ps.setObject(i + 1, recordMap.get(pk));
                }
            }
        };
    }

    private Map<String, Object> parseRowData(byte[] rowData) {
        try {
            return jsonWrapper.parseObject(rowData, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BatchedStatementExecutor createBatchedStatementExecutor(
            JdbcSinkDialect dialect, TableId tableId, Schema schema) {
        JdbcBatchStatementExecutor<JdbcRowData> upsertExecutor =
                JdbcBatchStatementExecutor.simple(
                        dialect.getUpsertStatement(tableId, schema),
                        getStatementBuilder(schema.getColumnNames()));
        JdbcBatchStatementExecutor<JdbcRowData> deleteExecutor =
                JdbcBatchStatementExecutor.simple(
                        dialect.getDeleteStatement(tableId, schema),
                        getStatementBuilder(
                                schema.primaryKeys().isEmpty()
                                        ? schema.getColumnNames()
                                        : schema.primaryKeys()));
        Function<JdbcRowData, JdbcRowData> keyExtractor = createRowKeyExtractor(tableId, schema);
        return new BatchedStatementExecutor(upsertExecutor, deleteExecutor, keyExtractor);
    }

    private Function<JdbcRowData, JdbcRowData> createRowKeyExtractor(
            TableId tableId, Schema schema) {

        List<String> primaryKeys = schema.primaryKeys();

        // Return full-line for non-PK tables
        if (primaryKeys.isEmpty()) {
            return row ->
                    new RichJdbcRowData.Builder()
                            .setRowKind(PK_ROW)
                            .setTableId(row.getTableId())
                            .setSchema(row.getSchema())
                            .setRows(row.getRows())
                            .build();
        }

        Schema pkOnlySchema =
                schema.copy(
                        schema.getColumns().stream()
                                .filter(col -> primaryKeys.contains(col.getName()))
                                .collect(Collectors.toList()));

        return row -> {
            Map<String, Object> rowData = parseRowData(row.getRows());
            Map<String, Object> pkOnlyRowData = new HashMap<>();
            for (String pk : schema.primaryKeys()) {
                pkOnlyRowData.put(pk, rowData.get(pk));
            }
            try {
                byte[] rowBytes = jsonWrapper.toJSONBytes(pkOnlyRowData);
                return new RichJdbcRowData.Builder()
                        .setRowKind(PK_ROW)
                        .setTableId(tableId)
                        .setSchema(pkOnlySchema)
                        .setRows(rowBytes)
                        .build();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to extract primary key row from row: " + row, e);
            }
        };
    }
}
