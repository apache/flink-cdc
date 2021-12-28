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

package com.ververica.cdc.connectors.base.source.reader.split;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.base.source.dialect.SnapshotEventDialect;
import com.ververica.cdc.connectors.base.source.dialect.StreamingEventDialect;
import com.ververica.cdc.connectors.base.source.offset.Offset;
import com.ververica.cdc.connectors.base.source.reader.split.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.base.source.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.utils.RecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reader to read split of table, the split is the snapshot split {@link SnapshotSplit}. */
public class SnapshotReader implements Reader<SourceRecord, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotReader.class);
    private final SnapshotEventDialect.SnapshotContext statefulTaskContext;
    private final ExecutorService executor;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private SnapshotEventDialect.Task splitSnapshotReadTask;
    private SnapshotSplit currentSnapshotSplit;
    private SchemaNameAdjuster nameAdjuster;
    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private final StreamingEventDialect streamingEventDialect;
    private final SnapshotEventDialect snapshotEventDialect;

    public SnapshotReader(
            SnapshotEventDialect.SnapshotContext statefulTaskContext,
            int subtaskId,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("debezium-snapshot-reader-" + subtaskId)
                        .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = false;
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
        this.snapshotEventDialect = snapshotEventDialect;
        this.streamingEventDialect = streamingEventDialect;
    }

    @Override
    public void submitSplit(SourceSplitBase splitToRead) {
        this.currentSnapshotSplit = splitToRead.asSnapshotSplit();
        statefulTaskContext.configure(currentSnapshotSplit);
        this.queue = statefulTaskContext.getQueue();
        //        this.nameAdjuster = statefulTaskContext.getSchemaNameAdjuster();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        this.splitSnapshotReadTask = snapshotEventDialect.createTask(statefulTaskContext);
        executor.submit(
                () -> {
                    try {
                        currentTaskRunning = true;
                        // execute snapshot read task
                        final SnapshotSplitChangeEventSourceContextImpl sourceContext =
                                new SnapshotSplitChangeEventSourceContextImpl();
                        SnapshotResult snapshotResult =
                                splitSnapshotReadTask.execute(sourceContext);

                        final StreamSplit backfillStreamSplit =
                                createBackfillStreamSplit(sourceContext);
                        // optimization that skip the binlog read when the low watermark equals high
                        // watermark
                        final boolean binlogBackfillRequired =
                                backfillStreamSplit
                                        .getEndingOffset()
                                        .isAfter(backfillStreamSplit.getStartingOffset());
                        if (!binlogBackfillRequired) {
                            dispatchHighWatermark(backfillStreamSplit);
                            currentTaskRunning = false;
                            return;
                        }

                        // execute binlog read task
                        if (snapshotResult.isCompletedOrSkipped()) {
                            final StreamingEventDialect.Task backfillBinlogReadTask =
                                    createBackfillStreamReadTask(backfillStreamSplit);
                            backfillBinlogReadTask.execute(
                                    new SnapshotStreamSplitChangeEventSourceContextImpl());
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

    private StreamSplit createBackfillStreamSplit(
            SnapshotSplitChangeEventSourceContextImpl sourceContext) {
        return new StreamSplit(
                currentSnapshotSplit.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                currentSnapshotSplit.getTableSchemas(),
                0);
    }

    private StreamingEventDialect.Task createBackfillStreamReadTask(
            StreamSplit backfillStreamSplit) {
        return streamingEventDialect.createTask(backfillStreamSplit);
    }

    private void dispatchHighWatermark(StreamSplit backFillBinlogSplit)
            throws InterruptedException {
        final SignalEventDispatcher signalEventDispatcher =
                new SignalEventDispatcher(statefulTaskContext);
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
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        checkReadException();

        if (hasNextElement.get()) {
            // data input: [low watermark event][snapshot events][high watermark event][binlog
            // events][binlog-end event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachBinlogEnd = false;
            final List<SourceRecord> sourceRecords = new ArrayList<>();
            while (!reachBinlogEnd) {
                List<DataChangeEvent> batch = queue.poll();
                for (DataChangeEvent event : batch) {
                    sourceRecords.add(event.getRecord());
                    if (RecordUtils.isEndWatermarkEvent(event.getRecord())) {
                        reachBinlogEnd = true;
                        break;
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);
            return snapshotEventDialect
                    .normalizedSplitRecords(currentSnapshotSplit, sourceRecords)
                    .iterator();
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

    @Override
    public void close() {
        try {
            snapshotEventDialect.close();
            streamingEventDialect.close();
            //            if (statefulTaskContext.getConnection() != null) {
            //                statefulTaskContext.getConnection().close();
            //            }
            //            if (statefulTaskContext.getBinaryLogClient() != null) {
            //                statefulTaskContext.getBinaryLogClient().disconnect();
            //            }
        } catch (Exception e) {
            LOG.error("Close snapshot reader error", e);
        }
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public class SnapshotSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {

        private Offset lowWatermark;
        private Offset highWatermark;

        public Offset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(Offset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public Offset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(Offset highWatermark) {
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
    public class SnapshotStreamSplitChangeEventSourceContextImpl
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
