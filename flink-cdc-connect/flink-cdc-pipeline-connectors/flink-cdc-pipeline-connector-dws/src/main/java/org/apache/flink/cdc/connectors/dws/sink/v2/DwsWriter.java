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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlushEventSinkWriter;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.cdc.connectors.dws.sink.DwsRecordConverter;
import org.apache.flink.cdc.connectors.dws.utils.DwsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

/** SinkV2 writer backed by one official DWS client per Flink sink writer. */
public class DwsWriter implements StatefulSinkWriter<Event, DwsWriterState>, FlushEventSinkWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DwsWriter.class);
    private static final long HEALTH_CHECK_INTERVAL_MILLIS = 1_000L;

    private final DwsDataSinkConfig settings;
    private final DwsClientFacade client;
    private final DwsWriterState stateCache;
    private final Map<TableId, TableInfo> tableInfoCache = new HashMap<>();
    private final Map<String, Long> bufferedTableBytes = new HashMap<>();
    private final AtomicReference<Throwable> firstAsyncFailure = new AtomicReference<>();
    private final MailboxExecutor mailboxExecutor;
    private final ProcessingTimeService processingTimeService;
    private final DwsWriterMetrics metrics;

    private ScheduledFuture<?> healthCheckTimer;
    private long bufferedAllBytes;
    private long bufferedRecords;
    private boolean closed;

    public DwsWriter(DwsDataSinkConfig settings, String jobId) {
        this(
                settings,
                new DwsClientFacade.Official(settings),
                jobId,
                DwsWriterMetrics.testing(),
                null,
                null);
    }

    public DwsWriter(
            DwsDataSinkConfig settings,
            String jobId,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this(
                settings,
                new DwsClientFacade.Official(settings),
                jobId,
                DwsWriterMetrics.testing(),
                mailboxExecutor,
                processingTimeService);
    }

    DwsWriter(DwsDataSinkConfig settings, DwsClientFacade client) {
        this(settings, client, "test-writer", DwsWriterMetrics.testing(), null, null);
    }

    DwsWriter(
            DwsDataSinkConfig settings,
            DwsClientFacade client,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this(
                settings,
                client,
                "test-writer",
                DwsWriterMetrics.testing(),
                mailboxExecutor,
                processingTimeService);
    }

    DwsWriter(
            DwsDataSinkConfig settings,
            DwsClientFacade client,
            DwsWriterMetrics metrics,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this(settings, client, "test-writer", metrics, mailboxExecutor, processingTimeService);
    }

    DwsWriter(
            DwsDataSinkConfig settings,
            DwsClientFacade client,
            String jobId,
            DwsWriterMetrics metrics,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.client = Objects.requireNonNull(client, "client");
        this.stateCache = DwsWriterState.nativeClientMarker();
        this.mailboxExecutor = mailboxExecutor;
        this.processingTimeService = processingTimeService;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        LOG.info("Initialized DWS writer: {}", safeConfigurationSummary());
        scheduleNextHealthCheck();
    }

    /** Compatibility constructor retained for callers of the former staging writer API. */
    public DwsWriter(
            String jdbcUrl,
            String username,
            String password,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            boolean enableDelete,
            String jobId,
            int subtaskId,
            long lastCheckpointId) {
        this(
                DwsDataSinkConfig.builder()
                        .withUrl(jdbcUrl)
                        .withUsername(username)
                        .withPassword(password)
                        .withZoneId(zoneId)
                        .withCaseSensitive(caseSensitive)
                        .withDefaultSchema(defaultSchema)
                        .withEnableDelete(enableDelete)
                        .build(),
                jobId);
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        checkAsyncFailure();
        if (event instanceof DataChangeEvent) {
            try {
                processDataChangeEvent((DataChangeEvent) event);
            } catch (IOException | RuntimeException failure) {
                metrics.recordDefiniteFailure();
                throw failure;
            }
        } else if (event instanceof SchemaChangeEvent) {
            handleSchemaChangeEvent((SchemaChangeEvent) event);
        }
        checkAsyncFailure();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushClientAndResetCounters();
    }

    @Override
    public void flush(FlushEvent event) throws IOException {
        flushClientAndResetCounters();
    }

    @Override
    public List<DwsWriterState> snapshotState(long checkpointId) throws IOException {
        flush(false);
        return Collections.singletonList(stateCache);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (healthCheckTimer != null) {
            healthCheckTimer.cancel(false);
        }

        IOException failure = currentAsyncFailure();
        try {
            client.close();
        } catch (IOException closeFailure) {
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            } else {
                failure = closeFailure;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    void recordAsyncFailure(Throwable failure) {
        if (firstAsyncFailure.compareAndSet(null, Objects.requireNonNull(failure, "failure"))) {
            metrics.recordFirstAsyncFailure();
        }
    }

    private void checkAsyncFailure() throws IOException {
        IOException failure = currentAsyncFailure();
        if (failure != null) {
            throw failure;
        }
    }

    private IOException currentAsyncFailure() {
        Throwable failure = firstAsyncFailure.get();
        if (failure == null) {
            failure = client.asyncFailure();
            if (failure != null) {
                if (firstAsyncFailure.compareAndSet(null, failure)) {
                    metrics.recordFirstAsyncFailure();
                }
                failure = firstAsyncFailure.get();
            }
        }
        return failure == null
                ? null
                : new IOException("Asynchronous DWS client failure.", failure);
    }

    private void scheduleNextHealthCheck() {
        if (processingTimeService == null || mailboxExecutor == null || closed) {
            return;
        }
        long timestamp =
                processingTimeService.getCurrentProcessingTime() + HEALTH_CHECK_INTERVAL_MILLIS;
        healthCheckTimer =
                processingTimeService.registerTimer(
                        timestamp,
                        ignoredTimestamp -> {
                            if (closed) {
                                return;
                            }
                            if (currentAsyncFailure() != null) {
                                mailboxExecutor.execute(
                                        this::checkAsyncFailure,
                                        "Propagate asynchronous DWS client failure");
                            } else {
                                scheduleNextHealthCheck();
                            }
                        });
    }

    private void processDataChangeEvent(DataChangeEvent event) throws IOException {
        TableInfo tableInfo = getRequiredTableInfo(event.tableId());
        String tableName = resolveTableName(event.tableId());
        switch (event.op()) {
            case INSERT:
            case REPLACE:
                submitWrite(tableName, tableInfo.converter.convertWrite(event.after()));
                break;
            case UPDATE:
                ensurePrimaryKeyUnchanged(event, tableInfo);
                submitWrite(tableName, tableInfo.converter.convertWrite(event.after()));
                break;
            case UPDATE_BEFORE:
                submitDelete(tableName, tableInfo.converter.convertDelete(event.before()));
                break;
            case DELETE:
                if (settings.isEnableDelete()) {
                    submitDelete(tableName, tableInfo.converter.convertDelete(event.before()));
                }
                break;
            default:
                throw new IOException("Unsupported DWS data operation: " + event.op());
        }
    }

    private void submitWrite(String tableName, Map<String, Object> values) throws IOException {
        long estimatedBytes = prepareBuffer(tableName, values);
        client.write(tableName, values);
        recordAcceptedBytes(tableName, estimatedBytes);
    }

    private void submitDelete(String tableName, Map<String, Object> values) throws IOException {
        long estimatedBytes = prepareBuffer(tableName, values);
        client.delete(tableName, values);
        recordAcceptedBytes(tableName, estimatedBytes);
    }

    private long prepareBuffer(String tableName, Map<String, Object> values) throws IOException {
        long estimatedBytes = estimateRecordBytes(tableName, values);
        long perTableLimit =
                Math.min(settings.getBufferTableMaxBytes(), settings.getBufferPartitionMaxBytes());
        if (estimatedBytes > perTableLimit || estimatedBytes > settings.getBufferAllMaxBytes()) {
            throw new IOException(
                    String.format(
                            "Estimated single DWS record size %d bytes exceeds the writer buffer budget for table %s.",
                            estimatedBytes, tableName));
        }

        long tableBytes = bufferedTableBytes.getOrDefault(tableName, 0L);
        if (wouldExceed(bufferedAllBytes, estimatedBytes, settings.getBufferAllMaxBytes())
                || wouldExceed(tableBytes, estimatedBytes, perTableLimit)) {
            flushClientAndResetCounters();
        }
        return estimatedBytes;
    }

    private void recordAcceptedBytes(String tableName, long estimatedBytes) {
        bufferedAllBytes += estimatedBytes;
        bufferedRecords++;
        bufferedTableBytes.merge(tableName, estimatedBytes, Long::sum);
        metrics.recordAccepted(bufferedAllBytes);
    }

    private void flushClientAndResetCounters() throws IOException {
        checkAsyncFailure();
        long startedNanos = System.nanoTime();
        client.flush();
        checkAsyncFailure();
        long durationMillis =
                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - startedNanos);
        metrics.recordSuccessfulFlush(bufferedRecords, bufferedAllBytes, durationMillis);
        bufferedAllBytes = 0L;
        bufferedRecords = 0L;
        bufferedTableBytes.clear();
    }

    String safeConfigurationSummary() {
        return String.format(
                "client=official, writeMode=%s, autoFlush=%s, retryMaxTimes=%d, retryBase=%s, retryJitter=%s, taskTimeout=%s, statementTimeout=%s, allBufferBytes=%d, tableBufferBytes=%d, partitionBufferBytes=%d, bufferAccounting=conservative-estimate, nativeBufferMetrics=unavailable",
                settings.getWriteMode(),
                settings.isEnableAutoFlush(),
                settings.getRetryMaxTimes(),
                settings.getRetrySleepBaseTime(),
                settings.getRetrySleepRandomTime(),
                settings.getTaskTimeout(),
                settings.getStatementTimeout(),
                settings.getBufferAllMaxBytes(),
                settings.getBufferTableMaxBytes(),
                settings.getBufferPartitionMaxBytes());
    }

    private static boolean wouldExceed(long current, long additional, long limit) {
        return additional > limit - current;
    }

    private static long estimateRecordBytes(String tableName, Map<String, Object> values) {
        long bytes = 64L + utf8Length(tableName);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            bytes += 32L + utf8Length(entry.getKey()) + estimateValueBytes(entry.getValue());
        }
        return bytes;
    }

    private static long estimateValueBytes(Object value) {
        if (value == null) {
            return 8L;
        }
        if (value instanceof byte[]) {
            return 16L + ((byte[]) value).length;
        }
        if (value instanceof CharSequence) {
            return 16L + utf8Length(value.toString());
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return 16L;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            long bytes = 16L;
            for (int i = 0; i < length; i++) {
                bytes += estimateValueBytes(Array.get(value, i));
            }
            return bytes;
        }
        return 32L + utf8Length(String.valueOf(value));
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private void ensurePrimaryKeyUnchanged(DataChangeEvent event, TableInfo tableInfo)
            throws IOException {
        Map<String, Object> beforeKeys = tableInfo.converter.convertDelete(event.before());
        Map<String, Object> afterKeys = tableInfo.converter.convertDelete(event.after());
        for (String primaryKey : beforeKeys.keySet()) {
            if (!Objects.deepEquals(beforeKeys.get(primaryKey), afterKeys.get(primaryKey))) {
                throw new IOException(
                        "Primary-key-changing UPDATE must be split before reaching DWS writer.");
            }
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
        String tableName = resolveTableName(event.tableId());
        if (event instanceof DropTableEvent) {
            client.removeTableSchema(tableName);
            tableInfoCache.remove(event.tableId());
            return;
        }
        if (event instanceof TruncateTableEvent) {
            return;
        }

        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo current = getRequiredTableInfo(event.tableId());
            newSchema = SchemaUtils.applySchemaChangeEvent(current.schema, event);
        }
        TableInfo newTableInfo =
                new TableInfo(newSchema, new DwsRecordConverter(newSchema, settings.getZoneId()));
        client.refreshTableSchema(tableName);
        tableInfoCache.put(event.tableId(), newTableInfo);
    }

    private TableInfo getRequiredTableInfo(TableId tableId) throws IOException {
        TableInfo tableInfo = tableInfoCache.get(tableId);
        if (tableInfo == null) {
            throw new IOException("Table schema cache is missing for " + tableId);
        }
        return tableInfo;
    }

    private String resolveTableName(TableId tableId) {
        return DwsUtils.formatNativeTableName(
                tableId, settings.getDefaultSchema(), settings.isCaseSensitive());
    }

    private static final class TableInfo {
        private final Schema schema;
        private final DwsRecordConverter converter;

        private TableInfo(Schema schema, DwsRecordConverter converter) {
            this.schema = schema;
            this.converter = converter;
        }
    }
}
