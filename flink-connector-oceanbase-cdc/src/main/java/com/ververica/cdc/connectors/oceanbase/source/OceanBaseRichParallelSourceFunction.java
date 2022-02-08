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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.mysql.jdbc.ResultSetMetaData;
import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class OceanBaseRichParallelSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 2844054619864617340L;

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseRichParallelSourceFunction.class);

    private static final JdbcValueConverters JDBC_VALUE_CONVERTERS =
            new JdbcValueConverters(
                    JdbcValueConverters.DecimalMode.STRING,
                    TemporalPrecisionMode.ADAPTIVE,
                    ZoneOffset.UTC,
                    null,
                    null,
                    null);

    private final boolean snapshot;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String jdbcUrl;
    private final String rsList;
    private final String logProxyHost;
    private final int logProxyPort;
    private final long startTimestamp;
    private final DebeziumDeserializationSchema<T> deserializer;

    private final AtomicBoolean snapshotCompleted = new AtomicBoolean(false);
    private final List<LogMessage> logMessageBuffer = new LinkedList<>();

    private transient Map<String, TableSchema> tableSchemaMap;
    private transient volatile long resolvedTimestamp;
    private transient Connection snapshotConnection;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichParallelSourceFunction(
            boolean snapshot,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String jdbcUrl,
            String rsList,
            String logProxyHost,
            int logProxyPort,
            long startTimestamp,
            DebeziumDeserializationSchema<T> deserializer) {
        this.snapshot = snapshot;
        this.username = username;
        this.password = password;
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.jdbcUrl = jdbcUrl;
        this.rsList = rsList;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.startTimestamp = startTimestamp;
        this.deserializer = deserializer;
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
        this.tableSchemaMap = new ConcurrentHashMap<>();
        this.resolvedTimestamp = -1;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;

        LOG.info("Start readChangeEvents process");
        readChangeEvents();

        if (shouldReadSnapshot()) {
            synchronized (ctx.getCheckpointLock()) {
                readSnapshotEvents();
                LOG.info("Snapshot reading finished");
            }
        } else {
            LOG.info("Skip snapshot read");
        }

        logProxyClient.join();
    }

    protected void readSnapshotEvents() throws Exception {
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            snapshotConnection = DriverManager.getConnection(jdbcUrl, username, password);
            stmt = snapshotConnection.createStatement();
            // TODO support reading from multiple tables
            readSnapshotFromTable(stmt, databaseName, tableName);
        } catch (SQLException e) {
            LOG.error(
                    "Failed to read snapshot from database: {}, table: {}",
                    databaseName,
                    tableName,
                    e);
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (snapshotConnection != null) {
                snapshotConnection.close();
            }
        }

        snapshotCompleted.set(true);
    }

    private void readSnapshotFromTable(Statement stmt, String databaseName, String tableName)
            throws Exception {
        // TODO make topic name configurable
        String topicName = getDefaultTopicName(tenantName, databaseName, tableName);
        Map<String, String> partition = getSourcePartition(tenantName, databaseName, tableName);
        // the offset here is useless
        Map<String, Object> offset = getSourceOffset(resolvedTimestamp);

        String selectSql = String.format("SELECT * FROM `%s`.`%s`", databaseName, tableName);
        ResultSet rs = stmt.executeQuery(selectSql);
        ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();

        // build table schema from metadata
        TableSchemaGenerator tableSchemaGenerator =
                () -> {
                    TableEditor tableEditor =
                            Table.editor().tableId(tableId(databaseName, tableName));
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        ColumnEditor columnEditor =
                                Column.editor()
                                        .name(metaData.getColumnName(i))
                                        .position(i)
                                        .type(metaData.getColumnTypeName(i))
                                        .jdbcType(metaData.getColumnType(i))
                                        .length(metaData.getPrecision(i))
                                        .scale(metaData.getScale(i))
                                        .optional(
                                                metaData.isNullable(i)
                                                        == java.sql.ResultSetMetaData
                                                                .columnNullable);
                        tableEditor.addColumn(columnEditor.create());
                    }
                    TableSchemaBuilder tableSchemaBuilder =
                            new TableSchemaBuilder(
                                    JDBC_VALUE_CONVERTERS,
                                    SchemaNameAdjuster.create(),
                                    new CustomConverterRegistry(null),
                                    OceanBaseSourceInfo.schema(),
                                    false);
                    // TODO add column filter and mapper
                    return tableSchemaBuilder.create(
                            null,
                            Envelope.schemaName(topicName),
                            tableEditor.create(),
                            null,
                            null,
                            null);
                };
        TableSchema tableSchema = getTableSchema(topicName, tableSchemaGenerator);

        Struct source = OceanBaseSourceInfo.struct(tenantName, databaseName, tableName, null, null);

        while (rs.next()) {
            Struct value = new Struct(tableSchema.valueSchema());
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (metaData.getColumnType(i) == Types.DECIMAL) {
                    BigDecimal d = rs.getBigDecimal(i);
                    value.put(metaData.getColumnName(i), d == null ? null : d.toString());
                } else {
                    value.put(metaData.getColumnName(i), rs.getObject(i));
                }
            }
            Struct struct = tableSchema.getEnvelopeSchema().create(value, source, null);
            deserializer.deserialize(
                    new SourceRecord(
                            partition,
                            offset,
                            topicName,
                            null,
                            null,
                            null,
                            struct.schema(),
                            struct),
                    outputCollector);
        }
    }

    protected void readChangeEvents() throws InterruptedException {
        ObReaderConfig obReaderConfig = new ObReaderConfig();
        obReaderConfig.setRsList(rsList);
        obReaderConfig.setUsername(username);
        obReaderConfig.setPassword(password);
        obReaderConfig.setTableWhiteList(
                String.format("%s.%s.%s", tenantName, databaseName, tableName));

        if (resolvedTimestamp > 0) {
            obReaderConfig.setStartTimestamp(resolvedTimestamp);
            LOG.info("Read change events from resolvedTimestamp: {}", resolvedTimestamp);
        } else {
            obReaderConfig.setStartTimestamp(startTimestamp);
            LOG.info("Read change events from startTimestamp: {}", startTimestamp);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        logProxyClient = new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig);

        logProxyClient.addListener(
                new RecordListener() {

                    boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        switch (message.getOpt()) {
                            case HEARTBEAT:
                            case BEGIN:
                                if (!started) {
                                    started = true;
                                    latch.countDown();
                                }
                                break;
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                if (!started) {
                                    break;
                                }
                                logMessageBuffer.add(message);
                                break;
                            case COMMIT:
                                // flush buffer after snapshot completed
                                if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                    logMessageBuffer.forEach(
                                            msg -> {
                                                try {
                                                    deserializer.deserialize(
                                                            toRecord(msg), outputCollector);
                                                } catch (Exception e) {
                                                    throw new FlinkRuntimeException(e);
                                                }
                                            });
                                    logMessageBuffer.clear();
                                    resolvedTimestamp = Long.parseLong(message.getTimestamp());
                                }
                                break;
                            case DDL:
                                // TODO record ddl and remove expired table schema
                                LOG.trace(
                                        "Ddl: {}",
                                        message.getFieldList().get(0).getValue().toString());
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error("LogProxyClient exception", e);
                        logProxyClient.stop();
                    }
                });

        logProxyClient.start();
        LOG.info("LogProxyClient started");
        latch.await();
        LOG.info("LogProxyClient packet processing started");
    }

    private SourceRecord toRecord(LogMessage message) throws Exception {
        String databaseName = OceanBaseLogMessageUtils.getDatabase(message.getDbName(), tenantName);
        // TODO make topic name configurable
        String topicName = getDefaultTopicName(tenantName, databaseName, message.getTableName());

        TableSchemaGenerator tableSchemaGenerator =
                () -> {
                    TableEditor tableEditor =
                            Table.editor().tableId(tableId(databaseName, tableName));
                    for (DataMessage.Record.Field field : message.getFieldList()) {
                        if (message.getOpt() == DataMessage.Record.Type.UPDATE && field.isPrev()) {
                            continue;
                        }
                        // we can't get the scale of decimal, here we set it to 0 to be
                        // compatible with the logic of JdbcValueConverters#schemaBuilder
                        Column column =
                                Column.editor()
                                        .name(field.getFieldname())
                                        .jdbcType(OceanBaseLogMessageUtils.getJdbcType(field))
                                        .scale(0)
                                        .optional(true)
                                        .create();
                        tableEditor.addColumn(column);
                    }
                    TableSchemaBuilder tableSchemaBuilder =
                            new TableSchemaBuilder(
                                    JDBC_VALUE_CONVERTERS,
                                    SchemaNameAdjuster.create(),
                                    new CustomConverterRegistry(null),
                                    OceanBaseSourceInfo.schema(),
                                    false);
                    // TODO add column filter and mapper
                    return tableSchemaBuilder.create(
                            null,
                            Envelope.schemaName(topicName),
                            tableEditor.create(),
                            null,
                            null,
                            null);
                };

        TableSchema tableSchema = getTableSchema(topicName, tableSchemaGenerator);
        Envelope envelope = tableSchema.getEnvelopeSchema();
        Schema valueSchema = tableSchema.valueSchema();
        Struct source =
                OceanBaseSourceInfo.struct(
                        tenantName,
                        databaseName,
                        message.getTableName(),
                        message.getTimestamp(),
                        message.getOB10UniqueId());
        Struct struct;
        switch (message.getOpt()) {
            case INSERT:
                struct =
                        envelope.create(
                                getValueStruct(valueSchema, message.getFieldList()), source, null);
                break;
            case UPDATE:
                List<DataMessage.Record.Field> before = new ArrayList<>();
                List<DataMessage.Record.Field> after = new ArrayList<>();
                for (DataMessage.Record.Field field : message.getFieldList()) {
                    if (field.isPrev()) {
                        before.add(field);
                    } else {
                        after.add(field);
                    }
                }
                struct =
                        envelope.update(
                                getValueStruct(valueSchema, before),
                                getValueStruct(valueSchema, after),
                                source,
                                null);
                break;
            case DELETE:
                struct =
                        envelope.delete(
                                getValueStruct(valueSchema, message.getFieldList()), source, null);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported dml type: " + message.getOpt());
        }
        return new SourceRecord(
                getSourcePartition(tenantName, databaseName, message.getTableName()),
                getSourceOffset(Long.parseLong(message.getTimestamp())),
                topicName,
                null,
                null,
                null,
                struct.schema(),
                struct);
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp == -1 && snapshot;
    }

    private String getDefaultTopicName(String tenantName, String databaseName, String tableName) {
        return String.format("%s.%s.%s", tenantName, databaseName, tableName);
    }

    private Map<String, String> getSourcePartition(
            String tenantName, String databaseName, String tableName) {
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("tenant", tenantName);
        sourcePartition.put("database", databaseName);
        sourcePartition.put("table", tableName);
        return sourcePartition;
    }

    private Map<String, Object> getSourceOffset(long timestamp) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("timestamp", timestamp);
        return sourceOffset;
    }

    private TableId tableId(String databaseName, String tableName) {
        return new TableId(databaseName, null, tableName);
    }

    interface TableSchemaGenerator {
        TableSchema generate() throws Exception;
    }

    private TableSchema getTableSchema(String topicName, TableSchemaGenerator generator)
            throws Exception {
        TableSchema tableSchema = tableSchemaMap.get(topicName);
        if (tableSchema != null) {
            return tableSchema;
        }

        tableSchema = generator.generate();
        tableSchemaMap.put(topicName, tableSchema);
        return tableSchema;
    }

    private Struct getValueStruct(Schema valueSchema, List<DataMessage.Record.Field> fieldList) {
        Struct value = new Struct(valueSchema);
        for (DataMessage.Record.Field field : fieldList) {
            value.put(field.getFieldname(), OceanBaseLogMessageUtils.getObject(field));
        }
        return value;
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at resolvedTimestamp: {}",
                context.getCheckpointId(),
                resolvedTimestamp);
        offsetState.clear();
        offsetState.add(resolvedTimestamp);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTimestampState", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTimestamp = offset;
                LOG.info("Restore State from resolvedTimestamp: {}", resolvedTimestamp);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (snapshotConnection != null) {
                snapshotConnection.close();
            }
        } catch (SQLException e) {
            LOG.error("Failed to close snapshotConnection", e);
        }
        if (logProxyClient != null) {
            logProxyClient.stop();
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
