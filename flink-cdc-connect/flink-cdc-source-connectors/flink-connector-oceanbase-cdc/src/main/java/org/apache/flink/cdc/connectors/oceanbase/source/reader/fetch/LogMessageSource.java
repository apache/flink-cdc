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

package org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseSourceConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.LogMessageOffset;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseOffsetContext;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBasePartition;
import org.apache.flink.util.function.SerializableFunction;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A change event source that emits events from commit log of OceanBase. */
public class LogMessageSource
        implements StreamingChangeEventSource<OceanBasePartition, OceanBaseOffsetContext> {

    private static final Logger LOG = LoggerFactory.getLogger(LogMessageSource.class);

    private final StreamSplit split;
    private final OceanBaseConnectorConfig connectorConfig;
    private final JdbcSourceEventDispatcher<OceanBasePartition> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final OceanBaseTaskContext taskContext;
    private final SerializableFunction<LogMessage, TableId> tableIdProvider;
    private final Map<TableSchema, Map<String, Integer>> fieldIndexMap = new HashMap<>();

    private final LogProxyClient client;

    public LogMessageSource(
            OceanBaseConnectorConfig connectorConfig,
            JdbcSourceEventDispatcher<OceanBasePartition> eventDispatcher,
            ErrorHandler errorHandler,
            OceanBaseTaskContext taskContext,
            StreamSplit split) {
        this.connectorConfig = connectorConfig;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.split = split;
        this.tableIdProvider = message -> getTableId(connectorConfig, message);
        this.client = createClient(connectorConfig, split);
    }

    private static TableId getTableId(OceanBaseConnectorConfig config, LogMessage message) {
        if (StringUtils.isBlank(message.getDbName())
                || StringUtils.isBlank(message.getTableName())) {
            return null;
        }
        String dbName =
                message.getDbName().replace(config.getSourceConfig().getTenantName() + ".", "");
        if (StringUtils.isBlank(dbName)) {
            return null;
        }
        return "mysql".equalsIgnoreCase(config.getSourceConfig().getCompatibleMode())
                ? new TableId(dbName, null, message.getTableName())
                : new TableId(null, dbName, message.getTableName());
    }

    private static LogProxyClient createClient(OceanBaseConnectorConfig config, StreamSplit split) {
        OceanBaseSourceConfig sourceConfig = config.getSourceConfig();
        ObReaderConfig obReaderConfig = new ObReaderConfig();
        if (StringUtils.isNotEmpty(sourceConfig.getRsList())) {
            obReaderConfig.setRsList(sourceConfig.getRsList());
        }
        if (StringUtils.isNotEmpty(sourceConfig.getConfigUrl())) {
            obReaderConfig.setClusterUrl(sourceConfig.getConfigUrl());
        }
        if (StringUtils.isNotEmpty(sourceConfig.getWorkingMode())) {
            obReaderConfig.setWorkingMode(sourceConfig.getWorkingMode());
        }
        obReaderConfig.setUsername(sourceConfig.getUsername());
        obReaderConfig.setPassword(sourceConfig.getPassword());
        obReaderConfig.setStartTimestamp(
                Long.parseLong(((LogMessageOffset) split.getStartingOffset()).getTimestamp()));
        obReaderConfig.setTimezone(sourceConfig.getServerTimeZone());

        if (sourceConfig.getObcdcProperties() != null
                && !sourceConfig.getObcdcProperties().isEmpty()) {
            Map<String, String> extraConfigs = new HashMap<>();
            sourceConfig
                    .getObcdcProperties()
                    .forEach((k, v) -> extraConfigs.put(k.toString(), v.toString()));
            obReaderConfig.setExtraConfigs(extraConfigs);
        }

        ClientConf clientConf =
                ClientConf.builder()
                        .maxReconnectTimes(0)
                        .connectTimeoutMs((int) sourceConfig.getConnectTimeout().toMillis())
                        .build();

        return new LogProxyClient(
                sourceConfig.getLogProxyHost(),
                sourceConfig.getLogProxyPort(),
                obReaderConfig,
                clientConf);
    }

    @Override
    public void execute(
            ChangeEventSourceContext changeEventSourceContext,
            OceanBasePartition partition,
            OceanBaseOffsetContext offsetContext) {
        if (connectorConfig.getSourceConfig().getStartupOptions().isSnapshotOnly()) {
            LOG.info("Streaming is not enabled in current configuration");
            return;
        }
        this.taskContext.getSchema().assureNonEmptySchema();
        OceanBaseOffsetContext effectiveOffsetContext =
                offsetContext != null
                        ? offsetContext
                        : OceanBaseOffsetContext.initial(this.connectorConfig);

        Set<Envelope.Operation> skippedOperations = this.connectorConfig.getSkippedOperations();
        EnumMap<DataMessage.Record.Type, BlockingConsumer<LogMessage>> eventHandlers =
                new EnumMap<>(DataMessage.Record.Type.class);

        eventHandlers.put(
                DataMessage.Record.Type.HEARTBEAT,
                message -> LOG.trace("HEARTBEAT message: {}", message));
        eventHandlers.put(
                DataMessage.Record.Type.DDL, message -> LOG.trace("DDL message: {}", message));

        eventHandlers.put(
                DataMessage.Record.Type.BEGIN,
                message -> handleTransactionBegin(partition, effectiveOffsetContext, message));
        eventHandlers.put(
                DataMessage.Record.Type.COMMIT,
                message -> handleTransactionCompletion(partition, effectiveOffsetContext, message));

        if (!skippedOperations.contains(Envelope.Operation.CREATE)) {
            eventHandlers.put(
                    DataMessage.Record.Type.INSERT,
                    message ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.CREATE,
                                    message));
        }
        if (!skippedOperations.contains(Envelope.Operation.UPDATE)) {
            eventHandlers.put(
                    DataMessage.Record.Type.UPDATE,
                    message ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.UPDATE,
                                    message));
        }
        if (!skippedOperations.contains(Envelope.Operation.DELETE)) {
            eventHandlers.put(
                    DataMessage.Record.Type.DELETE,
                    message ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.DELETE,
                                    message));
        }

        CountDownLatch latch = new CountDownLatch(1);

        client.addListener(
                new RecordListener() {

                    private volatile boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        if (!changeEventSourceContext.isRunning()) {
                            client.stop();
                            return;
                        }

                        if (!started) {
                            started = true;
                            latch.countDown();
                        }

                        LogMessageOffset currentOffset =
                                new LogMessageOffset(effectiveOffsetContext.getOffset());
                        if (currentOffset.isBefore(split.getStartingOffset())) {
                            return;
                        }
                        if (!LogMessageOffset.NO_STOPPING_OFFSET.equals(split.getEndingOffset())
                                && currentOffset.isAtOrAfter(split.getEndingOffset())) {
                            try {
                                eventDispatcher.dispatchWatermarkEvent(
                                        partition.getSourcePartition(),
                                        split,
                                        currentOffset,
                                        WatermarkKind.END);
                            } catch (InterruptedException e) {
                                LOG.error("Send signal event error.", e);
                                errorHandler.setProducerThrowable(
                                        new RuntimeException(
                                                "Error processing log signal event", e));
                            }
                            ((StoppableChangeEventSourceContext) changeEventSourceContext)
                                    .stopChangeEventSource();
                            client.stop();
                            return;
                        }

                        try {
                            eventHandlers
                                    .getOrDefault(
                                            message.getOpt(),
                                            msg -> LOG.trace("Skip log message {}", msg))
                                    .accept(message);
                        } catch (Throwable throwable) {
                            errorHandler.setProducerThrowable(
                                    new RuntimeException("Error handling log message", throwable));
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error(e.getMessage());
                    }
                });

        try {
            client.start();
            client.join();
        } finally {
            client.stop();
        }
    }

    private void handleTransactionBegin(
            OceanBasePartition partition, OceanBaseOffsetContext offsetContext, LogMessage message)
            throws InterruptedException {
        LogMessageOffset offset = LogMessageOffset.from(message);
        String transactionId = message.getOB10UniqueId();
        offsetContext.beginTransaction(transactionId);

        if (!offset.getCommitVersion().equals(offsetContext.getCommitVersion())) {
            offsetContext.setCommitVersion(offset.getTimestamp(), offset.getCommitVersion());
        }
        eventDispatcher.dispatchTransactionStartedEvent(partition, transactionId, offsetContext);
    }

    private void handleTransactionCompletion(
            OceanBasePartition partition, OceanBaseOffsetContext offsetContext, LogMessage message)
            throws InterruptedException {
        offsetContext.commitTransaction();
        eventDispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
    }

    private void handleChange(
            OceanBasePartition partition,
            OceanBaseOffsetContext offsetContext,
            Envelope.Operation operation,
            LogMessage message)
            throws InterruptedException {
        final TableId tableId = tableIdProvider.apply(message);
        if (tableId == null) {
            LOG.warn("No valid tableId found, skipping log message: {}", message);
            return;
        }

        TableSchema tableSchema = taskContext.getSchema().schemaFor(tableId);
        if (tableSchema == null) {
            LOG.warn("No table schema found, skipping log message: {}", message);
            return;
        }

        Map<String, Integer> fieldIndex =
                fieldIndexMap.computeIfAbsent(
                        tableSchema,
                        schema ->
                                IntStream.range(0, schema.valueSchema().fields().size())
                                        .boxed()
                                        .collect(
                                                Collectors.toMap(
                                                        i ->
                                                                schema.valueSchema()
                                                                        .fields()
                                                                        .get(i)
                                                                        .name(),
                                                        i -> i)));

        Serializable[] before = null;
        Serializable[] after = null;
        for (DataMessage.Record.Field field : message.getFieldList()) {
            if (field.isPrev()) {
                if (before == null) {
                    before = new Serializable[fieldIndex.size()];
                }
                before[fieldIndex.get(field.getFieldname())] = (Serializable) getFieldValue(field);
            } else {
                if (after == null) {
                    after = new Serializable[fieldIndex.size()];
                }
                after[fieldIndex.get(field.getFieldname())] = (Serializable) getFieldValue(field);
            }
        }

        offsetContext.event(tableId, Instant.ofEpochSecond(Long.parseLong(message.getTimestamp())));
        eventDispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new LogMessageEmitter(
                        partition, offsetContext, Clock.SYSTEM, operation, before, after));
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
}
