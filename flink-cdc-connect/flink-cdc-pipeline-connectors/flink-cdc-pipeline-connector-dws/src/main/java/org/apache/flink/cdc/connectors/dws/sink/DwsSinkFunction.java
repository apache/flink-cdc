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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.dws.utils.DwsConstants;
import org.apache.flink.cdc.connectors.dws.utils.DwsUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.huaweicloud.dws.client.DwsClient;
import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.exception.DwsClientException;
import com.huaweicloud.dws.client.exception.ExceptionCode;
import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.client.op.Operate;
import com.huaweicloud.dws.connectors.flink.DwsSink;
import com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions;
import com.huaweicloud.dws.connectors.flink.function.DwsInvokeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** SinkFunction wrapper around the Huawei DWS sink implementation. */
public class DwsSinkFunction extends RichSinkFunction<Event>
        implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(DwsSinkFunction.class);
    private static final String DEFAULT_SCHEMA = "public";
    private static final int DISABLED_AUTO_FLUSH_BATCH_SIZE = Integer.MAX_VALUE;
    private static final long DISABLED_AUTO_FLUSH_MAX_INTERVAL_MS = 0L;

    private final DwsConnectionOptions options;
    private final ZoneId zoneId;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final int autoFlushBatchSize;
    private final long autoFlushMaxIntervalMs;
    private final boolean enableAutoFlush;
    private final WriteMode writeMode;
    private final Map<TableId, TableInfo> tableInfoCache = new HashMap<>();
    private final SinkFunction<Event> sinkFunction;

    public DwsSinkFunction(
            DwsConnectionOptions connectorOptions,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            int autoFlushBatchSize,
            Duration autoFlushMaxInterval,
            boolean enableAutoFlush,
            WriteMode writeMode) {
        this(
                connectorOptions,
                zoneId,
                caseSensitive,
                defaultSchema,
                autoFlushBatchSize,
                autoFlushMaxInterval,
                enableAutoFlush,
                writeMode,
                null);
    }

    DwsSinkFunction(
            DwsConnectionOptions connectorOptions,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            int autoFlushBatchSize,
            Duration autoFlushMaxInterval,
            boolean enableAutoFlush,
            WriteMode writeMode,
            SinkFunction<Event> sinkFunction) {
        this.options = connectorOptions;
        this.zoneId = zoneId;
        this.caseSensitive = caseSensitive;
        this.defaultSchema = normalizeDefaultSchema(defaultSchema);
        this.autoFlushBatchSize = autoFlushBatchSize;
        this.autoFlushMaxIntervalMs = autoFlushMaxInterval.toMillis();
        this.enableAutoFlush = enableAutoFlush;
        this.writeMode = resolveWriteMode(writeMode, caseSensitive);
        this.sinkFunction = sinkFunction == null ? createDwsSinkFunction() : sinkFunction;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        prepareRichSinkFunction();
        CheckpointedFunction checkpointedFunction = asCheckpointedFunction(sinkFunction);
        if (checkpointedFunction != null) {
            checkpointedFunction.initializeState(context);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        CheckpointedFunction checkpointedFunction = asCheckpointedFunction(sinkFunction);
        if (checkpointedFunction != null) {
            checkpointedFunction.snapshotState(context);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        CheckpointListener checkpointListener = asCheckpointListener(sinkFunction);
        if (checkpointListener != null) {
            checkpointListener.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        CheckpointListener checkpointListener = asCheckpointListener(sinkFunction);
        if (checkpointListener != null) {
            checkpointListener.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        RichSinkFunction<Event> richSinkFunction = prepareRichSinkFunction();
        if (richSinkFunction != null) {
            richSinkFunction.open(parameters);
        }
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        sinkFunction.invoke(value, context);
    }

    @Override
    public void close() throws Exception {
        RichSinkFunction<Event> richSinkFunction = asRichSinkFunction(sinkFunction);
        if (richSinkFunction != null) {
            richSinkFunction.close();
        }
    }

    DwsConfig createDwsConfig() {
        DwsConfig.Builder builder =
                options.getConfig() == null
                        ? DwsConfig.builder()
                        : new DwsConfig.Builder(options.getConfig());

        builder.withUrl(options.getUrl())
                .withUsername(options.getUsername())
                .withPassword(options.getPassword())
                .withWriteMode(writeMode)
                .withLogSwitch(options.isLogSwitch())
                .withCaseSensitive(caseSensitive);

        if (enableAutoFlush) {
            builder.withAutoFlushBatchSize(autoFlushBatchSize)
                    .withAutoFlushMaxIntervalMs(autoFlushMaxIntervalMs);
        } else {
            builder.withAutoFlushBatchSize(DISABLED_AUTO_FLUSH_BATCH_SIZE)
                    .withAutoFlushMaxIntervalMs(DISABLED_AUTO_FLUSH_MAX_INTERVAL_MS);
        }

        return builder.build();
    }

    private SinkFunction<Event> createDwsSinkFunction() {
        DwsConfig dwsConfig = createDwsConfig();

        Map<String, Object> context = new HashMap<>();
        context.put("table_info_cache", tableInfoCache);

        DwsInvokeFunction<Event> invokeFunction =
                (event, client, ignoredContext) -> {
                    validateClient(client);
                    try {
                        if (event instanceof DataChangeEvent) {
                            processDataChangeEvent((DataChangeEvent) event, client);
                        } else if (event instanceof SchemaChangeEvent) {
                            handleSchemaChangeEvent((SchemaChangeEvent) event);
                        }
                    } catch (Exception e) {
                        throw new DwsClientException(
                                ExceptionCode.INTERNAL_ERROR,
                                "Failed to process event " + event,
                                e);
                    }
                };

        LOG.info(
                "Created GaussDB DWS sink with write mode {} and case-sensitive={}",
                writeMode,
                caseSensitive);
        return DwsSink.sink(dwsConfig, context, invokeFunction);
    }

    private void processDataChangeEvent(DataChangeEvent event, DwsClient client) throws Exception {
        String tableName = buildQualifiedTableName(event.tableId());
        switch (event.op()) {
            case INSERT:
                writeRecord(event.after(), event, client.write(tableName), DwsConstants.INSERT);
                break;
            case UPDATE:
                writeRecord(event.after(), event, client.write(tableName), DwsConstants.UPSERT);
                break;
            case DELETE:
                writeRecord(event.before(), event, client.delete(tableName), "DELETE");
                break;
            default:
                LOG.warn("Unsupported operation {} for {}", event.op(), event.tableId());
        }
    }

    private void writeRecord(
            RecordData recordData, DataChangeEvent event, Operate operate, String operation)
            throws Exception {
        if (recordData == null) {
            LOG.warn(
                    "Skip {} for {} because the record payload is null.",
                    operation,
                    event.tableId());
            return;
        }

        setRecordDataToOperate(operate, recordData, event);
        try {
            operate.commit();
        } catch (Exception e) {
            LOG.error("Failed to commit {} for {}", operation, event.tableId(), e);
            throw e;
        }
    }

    private void setRecordDataToOperate(
            Operate operate, RecordData recordData, DataChangeEvent event)
            throws DwsClientException {
        TableInfo tableInfo = tableInfoCache.get(event.tableId());
        if (tableInfo == null) {
            throw new DwsClientException(
                    ExceptionCode.TABLE_NOT_FOUND,
                    "Table schema cache is missing for " + event.tableId());
        }

        List<Column> columns = tableInfo.schema.getColumns();
        Preconditions.checkArgument(columns.size() == recordData.getArity());
        for (int i = 0; i < columns.size(); i++) {
            Object fieldValue = tableInfo.fieldGetters[i].getFieldOrNull(recordData);
            operate.setObject(columns.get(i).getName(), fieldValue);
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo currentTableInfo = tableInfoCache.get(tableId);
            if (currentTableInfo == null) {
                throw new IllegalStateException("Schema cache is missing for " + tableId);
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(currentTableInfo.schema, event);
        }

        RecordData.FieldGetter[] fieldGetters =
                new RecordData.FieldGetter[newSchema.getColumnCount()];
        for (int i = 0; i < newSchema.getColumnCount(); i++) {
            fieldGetters[i] =
                    DwsUtils.createFieldGetter(newSchema.getColumns().get(i).getType(), i, zoneId);
        }
        tableInfoCache.put(tableId, new TableInfo(newSchema, fieldGetters));
    }

    String buildQualifiedTableName(TableId tableId) {
        Preconditions.checkNotNull(tableId, "TableId cannot be null.");
        String schemaName = tableId.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = defaultSchema;
        }
        return schemaName.trim() + "." + tableId.getTableName().trim();
    }

    private static WriteMode resolveWriteMode(WriteMode requestedWriteMode, boolean caseSensitive) {
        // TODO: COPY_MERGE is not supported when case-sensitive field handling is enabled,
        // because the generated SQL cannot be executed. Fall back to AUTO instead.
        return Objects.requireNonNullElse(
                requestedWriteMode, caseSensitive ? WriteMode.AUTO : WriteMode.COPY_MERGE);
    }

    private static RichSinkFunction<Event> asRichSinkFunction(SinkFunction<Event> sinkFunction) {
        if (sinkFunction instanceof RichSinkFunction) {
            return (RichSinkFunction<Event>) sinkFunction;
        }
        return null;
    }

    private static CheckpointedFunction asCheckpointedFunction(SinkFunction<Event> sinkFunction) {
        if (sinkFunction instanceof CheckpointedFunction) {
            return (CheckpointedFunction) sinkFunction;
        }
        return null;
    }

    private static CheckpointListener asCheckpointListener(SinkFunction<Event> sinkFunction) {
        if (sinkFunction instanceof CheckpointListener) {
            return (CheckpointListener) sinkFunction;
        }
        return null;
    }

    private RichSinkFunction<Event> prepareRichSinkFunction() {
        RichSinkFunction<Event> richSinkFunction = asRichSinkFunction(sinkFunction);
        if (richSinkFunction != null) {
            richSinkFunction.setRuntimeContext(getRuntimeContext());
        }
        return richSinkFunction;
    }

    private static void validateClient(DwsClient client) {
        if (Objects.isNull(client)) {
            throw new IllegalStateException("DwsClient is null. Check the DWS sink configuration.");
        }
    }

    private static String normalizeDefaultSchema(String defaultSchema) {
        if (defaultSchema == null || defaultSchema.trim().isEmpty()) {
            return DEFAULT_SCHEMA;
        }
        return defaultSchema.trim();
    }

    private static final class TableInfo {
        private final Schema schema;
        private final RecordData.FieldGetter[] fieldGetters;

        private TableInfo(Schema schema, RecordData.FieldGetter[] fieldGetters) {
            this.schema = schema;
            this.fieldGetters = fieldGetters;
        }
    }
}
