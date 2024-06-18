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
            jdbcOutputs;

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
        this.jdbcOutputs = new ConcurrentHashMap<>();
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

        String upsertSql = catalog.getUpsertStatement(rowData.getTableId(), rowData.getSchema());
        JdbcStatementBuilder<JdbcRowData> jdbcStatementBuilder =
                getJdbcStatementBuilder(rowData.getSchema().getColumns());

        JdbcOutputFormat<Object, JdbcRowData, JdbcBatchStatementExecutor<JdbcRowData>>
                outputFormat =
                        jdbcOutputs.computeIfAbsent(
                                rowData.getTableId(),
                                id -> {
                                    JdbcOutputFormat<
                                                    Object,
                                                    JdbcRowData,
                                                    JdbcBatchStatementExecutor<JdbcRowData>>
                                            jdbcOutputFormat =
                                                    new JdbcOutputFormat<>(
                                                            connectionProvider,
                                                            executionOptions,
                                                            () ->
                                                                    JdbcBatchStatementExecutor
                                                                            .simple(
                                                                                    upsertSql,
                                                                                    jdbcStatementBuilder));
                                    try {
                                        jdbcOutputFormat.open(outputSerializer);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return jdbcOutputFormat;
                                });

        outputFormat.writeRecord(rowData);
        outputFormat.flush();
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        LOG.info("flush...");
        // TODO
        //        jdbcOutput.flush();
        //        jdbcOutput.checkFlushException();
    }

    @Override
    public void close() throws Exception {
        LOG.info("close...");
        // this.jdbcOutput.close();
        // this.jdbcOutput.close();
    }

    private JdbcStatementBuilder<JdbcRowData> getJdbcStatementBuilder(List<Column> columns) {
        return (ps, rowData) -> {
            Map<String, Object> recordMap;
            try {
                recordMap =
                        jsonWrapper.parseObject(
                                rowData.getRows(), new TypeReference<Map<String, Object>>() {});
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (!recordMap.isEmpty()) {
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    ps.setObject(i + 1, recordMap.get(column.getName()));
                }
            }
        };
    }
}
