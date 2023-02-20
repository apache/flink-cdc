/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.formatMessageTimestamp;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getSplitKey;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isLowWatermarkEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.splitKeyRangeContains;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.upsertBinlog;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A snapshot reader that reads data from Table in split level, the split is assigned by primary key
 * range.
 */
public class SnapshotSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executorService;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private MySqlSnapshotSplitReadTask splitSnapshotReadTask;
    private MySqlSnapshotSplit currentSnapshotSplit;
    private SchemaNameAdjuster nameAdjuster;
    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public SnapshotSplitReader(StatefulTaskContext statefulTaskContext, int subtaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("snapshot-reader-" + subtaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = false;
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        this.currentSnapshotSplit = mySqlSplit.asSnapshotSplit();
        statefulTaskContext.configure(currentSnapshotSplit);
        this.queue = statefulTaskContext.getQueue();
        this.nameAdjuster = statefulTaskContext.getSchemaNameAdjuster();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        this.splitSnapshotReadTask =
                new MySqlSnapshotSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getSnapshotChangeEventSourceMetrics(),
                        statefulTaskContext.getDatabaseSchema(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getTopicSelector(),
                        statefulTaskContext.getSnapshotReceiver(),
                        StatefulTaskContext.getClock(),
                        currentSnapshotSplit);
        executorService.submit(
                () -> {
                    try {
                        currentTaskRunning = true;
                        // execute snapshot read task
                        final SnapshotSplitChangeEventSourceContextImpl sourceContext =
                                new SnapshotSplitChangeEventSourceContextImpl();
                        SnapshotResult snapshotResult =
                                splitSnapshotReadTask.execute(
                                        sourceContext, statefulTaskContext.getOffsetContext());

                        final MySqlBinlogSplit backfillBinlogSplit =
                                createBackfillBinlogSplit(sourceContext);
                        // optimization that skip the binlog read when the low watermark equals high
                        // watermark
                        final boolean binlogBackfillRequired =
                                backfillBinlogSplit
                                        .getEndingOffset()
                                        .isAfter(backfillBinlogSplit.getStartingOffset());
                        if (!binlogBackfillRequired) {
                            dispatchBinlogEndEvent(backfillBinlogSplit);
                            currentTaskRunning = false;
                            return;
                        }

                        // execute binlog read task
                        if (snapshotResult.isCompletedOrSkipped()) {
                            final MySqlBinlogSplitReadTask backfillBinlogReadTask =
                                    createBackfillBinlogReadTask(backfillBinlogSplit);
                            final MySqlOffsetContext.Loader loader =
                                    new MySqlOffsetContext.Loader(
                                            statefulTaskContext.getConnectorConfig());
                            final MySqlOffsetContext mySqlOffsetContext =
                                    loader.load(
                                            backfillBinlogSplit.getStartingOffset().getOffset());

                            backfillBinlogReadTask.execute(
                                    new SnapshotBinlogSplitChangeEventSourceContextImpl(),
                                    mySqlOffsetContext);
                        } else {
                            readException =
                                    new IllegalStateException(
                                            String.format(
                                                    "Read snapshot for mysql split %s fail",
                                                    currentSnapshotSplit));
                        }
                    } catch (Exception e) {
                        currentTaskRunning = false;
                        LOG.error(
                                String.format(
                                        "Execute snapshot read task for mysql split %s fail",
                                        currentSnapshotSplit),
                                e);
                        readException = e;
                    }
                });
    }

    private MySqlBinlogSplit createBackfillBinlogSplit(
            SnapshotSplitChangeEventSourceContextImpl sourceContext) {
        return new MySqlBinlogSplit(
                currentSnapshotSplit.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                currentSnapshotSplit.getTableSchemas(),
                0);
    }

    private MySqlBinlogSplitReadTask createBackfillBinlogReadTask(
            MySqlBinlogSplit backfillBinlogSplit) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                statefulTaskContext
                        .getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with("table.include.list", currentSnapshotSplit.getTableId().toString())
                        // Disable heartbeat event in snapshot split reader
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new MySqlBinlogSplitReadTask(
                new MySqlConnectorConfig(dezConf),
                statefulTaskContext.getConnection(),
                statefulTaskContext.getDispatcher(),
                statefulTaskContext.getSignalEventDispatcher(),
                statefulTaskContext.getErrorHandler(),
                StatefulTaskContext.getClock(),
                statefulTaskContext.getTaskContext(),
                (MySqlStreamingChangeEventSourceMetrics)
                        statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                backfillBinlogSplit,
                event -> true);
    }

    private void dispatchBinlogEndEvent(MySqlBinlogSplit backFillBinlogSplit)
            throws InterruptedException {
        final SignalEventDispatcher signalEventDispatcher =
                new SignalEventDispatcher(
                        statefulTaskContext.getOffsetContext().getPartition(),
                        statefulTaskContext.getTopicSelector().getPrimaryTopic(),
                        statefulTaskContext.getDispatcher().getQueue());
        signalEventDispatcher.dispatchWatermarkEvent(
                backFillBinlogSplit,
                backFillBinlogSplit.getEndingOffset(),
                SignalEventDispatcher.WatermarkKind.BINLOG_END);
    }

    @Override
    public boolean isFinished() {
        return currentSnapshotSplit == null
                || (!currentTaskRunning && !hasNextElement.get() && reachEnd.get());
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();

        if (hasNextElement.get()) {
            // data input: [low watermark event][snapshot events][high watermark event][binlog
            // events][binlog-end event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachBinlogStart = false;
            boolean reachBinlogEnd = false;
            SourceRecord lowWatermark = null;
            SourceRecord highWatermark = null;
            Map<Struct, SourceRecord> snapshotRecords = new LinkedHashMap<>();
            while (!reachBinlogEnd) {
                checkReadException();
                List<DataChangeEvent> batch = queue.poll();
                for (DataChangeEvent event : batch) {
                    SourceRecord record = event.getRecord();
                    if (lowWatermark == null) {
                        lowWatermark = record;
                        assertLowWatermark(lowWatermark);
                        continue;
                    }

                    if (highWatermark == null && isHighWatermarkEvent(record)) {
                        highWatermark = record;
                        // snapshot events capture end and begin to capture binlog events
                        reachBinlogStart = true;
                        continue;
                    }

                    if (reachBinlogStart && RecordUtils.isEndWatermarkEvent(record)) {
                        // capture to end watermark events, stop the loop
                        reachBinlogEnd = true;
                        break;
                    }

                    if (!reachBinlogStart) {
                        snapshotRecords.put((Struct) record.key(), record);
                    } else {
                        if (isRequiredBinlogRecord(record)) {
                            // upsert binlog events through the record key
                            upsertBinlog(snapshotRecords, record);
                        }
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);

            final List<SourceRecord> normalizedRecords = new ArrayList<>();
            normalizedRecords.add(lowWatermark);
            normalizedRecords.addAll(formatMessageTimestamp(snapshotRecords.values()));
            normalizedRecords.add(highWatermark);

            final List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(normalizedRecords));
            return sourceRecordsSet.iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentSnapshotSplit, readException.getMessage()),
                    readException);
        }
    }

    private void assertLowWatermark(SourceRecord lowWatermark) {
        checkState(
                isLowWatermarkEvent(lowWatermark),
                String.format(
                        "The first record should be low watermark signal event, but actual is %s",
                        lowWatermark));
    }

    private boolean isRequiredBinlogRecord(SourceRecord record) {
        if (isDataChangeRecord(record)) {
            Object[] key =
                    getSplitKey(currentSnapshotSplit.getSplitKeyType(), record, nameAdjuster);
            return splitKeyRangeContains(
                    key, currentSnapshotSplit.getSplitStart(), currentSnapshotSplit.getSplitEnd());
        }
        return false;
    }

    @Override
    public void close() {
        try {
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the snapshot split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
        } catch (Exception e) {
            LOG.error("Close snapshot reader error", e);
        }
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link MySqlSnapshotSplit}.
     */
    public class SnapshotSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {

        private BinlogOffset lowWatermark;
        private BinlogOffset highWatermark;

        public BinlogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(BinlogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public BinlogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(BinlogOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotBinlogSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            currentTaskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return currentTaskRunning;
        }
    }
}
