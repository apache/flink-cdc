/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

    private final boolean snapshot;
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
    private final int logProxyPort;
    private final ClientConf logProxyClientConf;
    private final ObReaderConfig obReaderConfig;
    private final Properties debeziumProperties;
    private final DebeziumDeserializationSchema<T> deserializer;

    private final List<SourceRecord> changeRecordBuffer = new LinkedList<>();

    private transient Set<String> tableSet;
    private transient OceanBaseSchema obSchema;
    private transient volatile long resolvedTimestamp;
    private transient volatile OceanBaseConnection snapshotConnection;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichSourceFunction(
            boolean snapshot,
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
            int logProxyPort,
            ClientConf logProxyClientConf,
            ObReaderConfig obReaderConfig,
            Properties debeziumProperties,
            DebeziumDeserializationSchema<T> deserializer) {
        this.snapshot = checkNotNull(snapshot);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = checkNotNull(tenantName);
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
        this.logProxyHost = checkNotNull(logProxyHost);
        this.logProxyPort = checkNotNull(logProxyPort);
        this.logProxyClientConf = checkNotNull(logProxyClientConf);
        this.obReaderConfig = checkNotNull(obReaderConfig);
        this.debeziumProperties = debeziumProperties;
        this.deserializer = checkNotNull(deserializer);
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;
        try {
            LOG.info("Start to initial table whitelist");
            initTableWhiteList();

            if (shouldReadSnapshot()) {
                long startTimestamp = getSnapshotConnection().getCurrentTimestampS();
                LOG.info("Snapshot reading started from timestamp: {}", startTimestamp);
                readSnapshotRecords();
                LOG.info("Snapshot reading finished");
                resolvedTimestamp = startTimestamp;
            } else {
                LOG.info("Snapshot reading skipped");
            }

            LOG.info("Change events reading started");
            readChangeRecords();
        } finally {
            cancel();
        }
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp <= 0 && snapshot;
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

    private void initTableWhiteList() {
        if (tableSet != null && !tableSet.isEmpty()) {
            return;
        }

        final Set<String> localTableSet = new HashSet<>();

        if (StringUtils.isNotBlank(tableList)) {
            for (String s : tableList.split(",")) {
                if (StringUtils.isNotBlank(s)) {
                    String[] schema = s.split("\\.");
                    localTableSet.add(String.format("%s.%s", schema[0].trim(), schema[1].trim()));
                }
            }
        }

        if (StringUtils.isNotBlank(databaseName) && StringUtils.isNotBlank(tableName)) {
            try {
                List<String> tables = getSnapshotConnection().getTables(databaseName, tableName);
                LOG.info("Pattern matched tables: {}", tables);
                localTableSet.addAll(tables);
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
        this.obReaderConfig.setTableWhiteList(String.format("%s.*.*", tenantName));
    }

    private TableSchema getTableSchema(TableId tableId) {
        if (obSchema == null) {
            obSchema = new OceanBaseSchema();
        }
        TableChanges.TableChange tableChange =
                obSchema.getTableSchema(getSnapshotConnection(), tableId);

        OceanBaseValueConverters valueConverters =
                debeziumProperties == null
                        ? new OceanBaseValueConverters(serverTimeZone, compatibleMode)
                        : new OceanBaseValueConverters(
                                serverTimeZone,
                                compatibleMode,
                                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(
                                                debeziumProperties.getProperty(
                                                        "decimal.handling.mode", "precise"))
                                        .asDecimalMode(),
                                TemporalPrecisionMode.parse(
                                        debeziumProperties.getProperty(
                                                "time.precision.mode",
                                                "adaptive_time_microseconds")),
                                CommonConnectorConfig.BinaryHandlingMode.parse(
                                        debeziumProperties.getProperty(
                                                "binary.handling.mode", "bytes")));
        return new TableSchemaBuilder(
                        valueConverters,
                        SchemaNameAdjuster.create(),
                        new CustomConverterRegistry(null),
                        sourceSchema(),
                        false,
                        false)
                .create(null, tableId.identifier(), tableChange.getTable(), null, null, null);
    }

    private Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field("tenant", Schema.STRING_SCHEMA)
                .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private Struct sourceStruct(TableId tableId, String ts) {
        Struct struct =
                new Struct(sourceSchema()).put("tenant", tenantName).put("table", tableId.table());
        if (tableId.catalog() != null) {
            struct.put("db", tableId.catalog());
        }
        if (tableId.schema() != null) {
            struct.put("schema", tableId.schema());
        }
        if (ts != null) {
            struct.put("timestamp", ts);
        }
        return struct;
    }

    protected void readSnapshotRecords() {
        tableSet.forEach(
                table -> {
                    String[] schema = table.split("\\.");
                    readSnapshotRecordsByTable(schema[0], schema[1]);
                });
    }

    private void readSnapshotRecordsByTable(String databaseName, String tableName) {
        String fullName, catalog, schema;
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            fullName = String.format("`%s`.`%s`", databaseName, tableName);
            catalog = databaseName;
            schema = null;
        } else {
            fullName = String.format("%s.%s", databaseName, tableName);
            catalog = null;
            schema = databaseName;
        }
        TableId tableId = new TableId(catalog, schema, tableName);
        try {
            LOG.info("Start to read snapshot from {}", fullName);
            getSnapshotConnection()
                    .query(
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
                                    Struct struct =
                                            tableSchema
                                                    .getEnvelopeSchema()
                                                    .read(
                                                            value,
                                                            sourceStruct(tableId, null),
                                                            Instant.now());
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

        logProxyClient =
                new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig, logProxyClientConf);

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
                        LOG.error("LogProxyClient exception", e);
                        logProxyClient.stop();
                    }
                });

        LOG.info(
                "Try to start LogProxyClient with client id: {}, config: {}",
                logProxyClientConf.getClientId(),
                obReaderConfig);
        logProxyClient.start();

        if (!latch.await(connectTimeout.getSeconds(), TimeUnit.SECONDS)) {
            throw new TimeoutException(
                    "Timeout to receive log messages in LogProxyClient.RecordListener");
        }
        LOG.info("LogProxyClient started successfully");

        logProxyClient.join();
    }

    private SourceRecord getChangeRecord(LogMessage message) {
        String databaseName = message.getDbName().replace(tenantName + ".", "");
        if (!tableSet.contains(String.format("%s.%s", databaseName, message.getTableName()))) {
            return null;
        }
        String catalog, schema;
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            catalog = databaseName;
            schema = null;
        } else {
            catalog = null;
            schema = databaseName;
        }
        TableId tableId = new TableId(catalog, schema, message.getTableName());
        TableSchema tableSchema = getTableSchema(tableId);
        Struct source = sourceStruct(tableId, message.getSafeTimestamp());
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
