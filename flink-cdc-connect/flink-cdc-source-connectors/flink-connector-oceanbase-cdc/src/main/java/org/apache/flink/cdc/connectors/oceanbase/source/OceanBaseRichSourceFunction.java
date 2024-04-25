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

package org.apache.flink.cdc.connectors.oceanbase.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseSourceInfo;
import org.apache.flink.cdc.connectors.oceanbase.source.schema.OceanBaseDatabaseSchema;
import org.apache.flink.cdc.connectors.oceanbase.source.schema.OceanBaseSchema;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.clogproxy.client.util.ClientUtil;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import io.debezium.connector.SnapshotRecord;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class OceanBaseRichSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 2844054619864617340L;

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseRichSourceFunction.class);

    private final StartupOptions startupOptions;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String tableList;
    private final String serverTimeZone;
    private final Duration connectTimeout;
    private final String hostname;
    private final Integer port;
    private final String compatibleMode;
    private final String jdbcDriver;
    private final Properties jdbcProperties;
    private final String logProxyHost;
    private final Integer logProxyPort;
    private final String logProxyClientId;
    private final ObReaderConfig obReaderConfig;
    private final Properties debeziumProperties;
    private final DebeziumDeserializationSchema<T> deserializer;

    private final List<SourceRecord> changeRecordBuffer = new LinkedList<>();

    private transient OceanBaseConnectorConfig connectorConfig;
    private transient OceanBaseSourceInfo sourceInfo;
    private transient Set<TableId> tableSet;
    private transient OceanBaseSchema obSchema;
    private transient OceanBaseDatabaseSchema databaseSchema;
    private transient volatile long resolvedTimestamp;
    private transient volatile Exception logProxyClientException;
    private transient volatile OceanBaseConnection snapshotConnection;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichSourceFunction(
            StartupOptions startupOptions,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String tableList,
            String serverTimeZone,
            Duration connectTimeout,
            String hostname,
            Integer port,
            String compatibleMode,
            String jdbcDriver,
            Properties jdbcProperties,
            String logProxyHost,
            Integer logProxyPort,
            String logProxyClientId,
            ObReaderConfig obReaderConfig,
            Properties debeziumProperties,
            DebeziumDeserializationSchema<T> deserializer) {
        this.startupOptions = checkNotNull(startupOptions);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableList = tableList;
        this.serverTimeZone = checkNotNull(serverTimeZone);
        this.connectTimeout = checkNotNull(connectTimeout);
        this.hostname = checkNotNull(hostname);
        this.port = checkNotNull(port);
        this.compatibleMode = checkNotNull(compatibleMode);
        this.jdbcDriver = checkNotNull(jdbcDriver);
        this.jdbcProperties = jdbcProperties;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.logProxyClientId = logProxyClientId;
        this.obReaderConfig = obReaderConfig;
        this.debeziumProperties = debeziumProperties;
        this.deserializer = checkNotNull(deserializer);
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
        this.connectorConfig =
                new OceanBaseConnectorConfig(compatibleMode, serverTimeZone, debeziumProperties);
        this.sourceInfo = new OceanBaseSourceInfo(connectorConfig, tenantName);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;
        try {
            LOG.info("Start to initial table whitelist");
            initTableWhiteList();

            if (resolvedTimestamp <= 0 && !startupOptions.isStreamOnly()) {
                sourceInfo.setSnapshot(SnapshotRecord.TRUE);
                long startTimestamp = getSnapshotConnection().getCurrentTimestampS();
                LOG.info("Snapshot reading started from timestamp: {}", startTimestamp);
                readSnapshotRecords();
                sourceInfo.setSnapshot(SnapshotRecord.FALSE);
                LOG.info("Snapshot reading finished");
                resolvedTimestamp = startTimestamp;
            } else {
                LOG.info("Snapshot reading skipped");
            }

            if (!startupOptions.isSnapshotOnly()) {
                sourceInfo.setSnapshot(SnapshotRecord.FALSE);
                LOG.info("Change events reading started");
                readChangeRecords();
            }
        } finally {
            cancel();
        }
    }

    private OceanBaseConnection getSnapshotConnection() {
        if (snapshotConnection == null) {
            snapshotConnection =
                    new OceanBaseConnection(
                            hostname,
                            port,
                            username,
                            password,
                            connectTimeout,
                            compatibleMode,
                            jdbcDriver,
                            jdbcProperties,
                            getClass().getClassLoader());
        }
        return snapshotConnection;
    }

    private void closeSnapshotConnection() {
        if (snapshotConnection != null) {
            try {
                snapshotConnection.close();
            } catch (SQLException e) {
                LOG.error("Failed to close snapshotConnection", e);
            }
            snapshotConnection = null;
        }
    }

    private TableId tableId(String databaseName, String tableName) {
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            return new TableId(databaseName, null, tableName);
        }
        return new TableId(null, databaseName, tableName);
    }

    private void initTableWhiteList() {
        if (tableSet != null && !tableSet.isEmpty()) {
            return;
        }

        final Set<TableId> localTableSet = new HashSet<>();

        if (StringUtils.isNotBlank(tableList)) {
            for (String s : tableList.split(",")) {
                if (StringUtils.isNotBlank(s)) {
                    String[] arr = s.split("\\.");
                    TableId tableId = tableId(arr[0].trim(), arr[1].trim());
                    localTableSet.add(tableId);
                }
            }
        }

        if (StringUtils.isNotBlank(databaseName) && StringUtils.isNotBlank(tableName)) {
            try {
                List<TableId> tableIds = getSnapshotConnection().getTables(databaseName, tableName);
                LOG.info("Pattern matched tables: {}", tableIds);
                localTableSet.addAll(tableIds);
            } catch (SQLException e) {
                LOG.error(
                        String.format(
                                "Query table list by 'databaseName' %s and 'tableName' %s failed.",
                                databaseName, tableName),
                        e);
                throw new FlinkRuntimeException(e);
            }
        }

        if (localTableSet.isEmpty()) {
            throw new FlinkRuntimeException("No valid table found");
        }

        LOG.info("Table list: {}", localTableSet);
        this.tableSet = localTableSet;
        // for some 4.x versions, it will be treated as 'tenant.*.*'
        if (this.obReaderConfig != null) {
            this.obReaderConfig.setTableWhiteList(
                    localTableSet.stream()
                            .map(tableId -> String.format("%s.%s", tenantName, tableId.toString()))
                            .collect(Collectors.joining("|")));
        }
    }

    private TableSchema getTableSchema(TableId tableId) {
        if (databaseSchema == null) {
            databaseSchema =
                    new OceanBaseDatabaseSchema(connectorConfig, t -> tableSet.contains(t), false);
        }
        TableSchema tableSchema = databaseSchema.schemaFor(tableId);
        if (tableSchema != null) {
            return tableSchema;
        }

        if (obSchema == null) {
            obSchema = new OceanBaseSchema();
        }
        TableChanges.TableChange tableChange =
                obSchema.getTableSchema(getSnapshotConnection(), tableId);
        databaseSchema.refresh(tableChange.getTable());
        return databaseSchema.schemaFor(tableId);
    }

    protected void readSnapshotRecords() {
        tableSet.forEach(this::readSnapshotRecordsByTable);
    }

    private void readSnapshotRecordsByTable(TableId tableId) {
        String fullName = getSnapshotConnection().quotedTableIdString(tableId);
        sourceInfo.tableEvent(tableId);
        try (OceanBaseConnection connection = getSnapshotConnection()) {
            LOG.info("Start to read snapshot from {}", connection.quotedTableIdString(tableId));
            connection.query(
                    "SELECT * FROM " + fullName,
                    rs -> {
                        TableSchema tableSchema = getTableSchema(tableId);
                        List<Field> fields = tableSchema.valueSchema().fields();

                        while (rs.next()) {
                            Object[] fieldValues = new Object[fields.size()];
                            for (Field field : fields) {
                                fieldValues[field.index()] = rs.getObject(field.name());
                            }
                            Struct value = tableSchema.valueFromColumnData(fieldValues);
                            Instant now = Instant.now();
                            Struct struct =
                                    tableSchema
                                            .getEnvelopeSchema()
                                            .read(value, sourceInfo.struct(), now);
                            try {
                                deserializer.deserialize(
                                        new SourceRecord(
                                                null,
                                                null,
                                                tableId.identifier(),
                                                null,
                                                null,
                                                null,
                                                struct.schema(),
                                                struct),
                                        outputCollector);
                            } catch (Exception e) {
                                LOG.error("Deserialize snapshot record failed ", e);
                                throw new FlinkRuntimeException(e);
                            }
                        }
                    });
            LOG.info("Read snapshot from {} finished", fullName);
        } catch (SQLException e) {
            LOG.error("Read snapshot from table " + fullName + " failed", e);
            throw new FlinkRuntimeException(e);
        }
    }

    protected void readChangeRecords() throws InterruptedException, TimeoutException {
        if (resolvedTimestamp > 0) {
            obReaderConfig.updateCheckpoint(Long.toString(resolvedTimestamp));
            LOG.info("Restore from timestamp: {}", resolvedTimestamp);
        }

        ClientConf clientConf =
                ClientConf.builder()
                        .clientId(
                                logProxyClientId != null
                                        ? logProxyClientId
                                        : String.format(
                                                "%s_%s_%s",
                                                ClientUtil.generateClientId(),
                                                Thread.currentThread().getId(),
                                                tenantName))
                        .maxReconnectTimes(0)
                        .connectTimeoutMs((int) connectTimeout.toMillis())
                        .build();

        logProxyClient = new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig, clientConf);

        final CountDownLatch latch = new CountDownLatch(1);

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
                                SourceRecord record = getChangeRecord(message);
                                if (record != null) {
                                    changeRecordBuffer.add(record);
                                }
                                break;
                            case COMMIT:
                                changeRecordBuffer.forEach(
                                        r -> {
                                            try {
                                                deserializer.deserialize(r, outputCollector);
                                            } catch (Exception e) {
                                                throw new FlinkRuntimeException(e);
                                            }
                                        });
                                changeRecordBuffer.clear();
                                long timestamp = Long.parseLong(message.getSafeTimestamp());
                                if (timestamp > resolvedTimestamp) {
                                    resolvedTimestamp = timestamp;
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
                        logProxyClientException = e;
                        logProxyClient.stop();
                    }
                });

        LOG.info(
                "Try to start LogProxyClient with client id: {}, config: {}",
                clientConf.getClientId(),
                obReaderConfig);
        logProxyClient.start();

        if (!latch.await(connectTimeout.getSeconds(), TimeUnit.SECONDS)) {
            throw new TimeoutException(
                    "Timeout to receive log messages in LogProxyClient.RecordListener");
        }
        LOG.info("LogProxyClient started successfully");

        logProxyClient.join();

        if (logProxyClientException != null) {
            throw new RuntimeException("LogProxyClient exception", logProxyClientException);
        }
    }

    private SourceRecord getChangeRecord(LogMessage message) {
        String databaseName = message.getDbName().replace(tenantName + ".", "");
        TableId tableId = tableId(databaseName, message.getTableName());
        if (!tableSet.contains(tableId)) {
            return null;
        }

        sourceInfo.tableEvent(tableId);
        sourceInfo.setSourceTime(Instant.ofEpochSecond(Long.parseLong(message.getTimestamp())));
        Struct source = sourceInfo.struct();

        TableSchema tableSchema = getTableSchema(tableId);
        Struct struct;
        Schema valueSchema = tableSchema.valueSchema();
        List<Field> fields = valueSchema.fields();
        Struct before, after;
        Object[] beforeFieldValues, afterFieldValues;
        Map<String, Object> beforeValueMap = new HashMap<>();
        Map<String, Object> afterValueMap = new HashMap<>();
        message.getFieldList()
                .forEach(
                        field -> {
                            if (field.isPrev()) {
                                beforeValueMap.put(field.getFieldname(), getFieldValue(field));
                            } else {
                                afterValueMap.put(field.getFieldname(), getFieldValue(field));
                            }
                        });
        switch (message.getOpt()) {
            case INSERT:
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    afterFieldValues[field.index()] = afterValueMap.get(field.name());
                }
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct = tableSchema.getEnvelopeSchema().create(after, source, Instant.now());
                break;
            case DELETE:
                beforeFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] = beforeValueMap.get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                struct = tableSchema.getEnvelopeSchema().delete(before, source, Instant.now());
                break;
            case UPDATE:
                beforeFieldValues = new Object[fields.size()];
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] = beforeValueMap.get(field.name());
                    afterFieldValues[field.index()] = afterValueMap.get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct =
                        tableSchema
                                .getEnvelopeSchema()
                                .update(before, after, source, Instant.now());
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return new SourceRecord(
                null, null, tableId.identifier(), null, null, null, struct.schema(), struct);
    }

    private Object getFieldValue(DataMessage.Record.Field field) {
        if (field.getValue() == null) {
            return null;
        }
        String encoding = field.getEncoding();
        if ("binary".equalsIgnoreCase(encoding)) {
            return field.getValue().getBytes();
        }
        return field.getValue().toString(encoding);
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
        closeSnapshotConnection();
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
