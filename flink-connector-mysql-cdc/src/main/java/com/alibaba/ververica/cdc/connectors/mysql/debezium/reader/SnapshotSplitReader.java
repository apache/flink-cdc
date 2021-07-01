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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.MySQLBinlogSplitReadTask;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.MySQLSnapshotSplitReadTask;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitState;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
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

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.normalizedSplitRecords;

/**
 * A snapshot reader that reads data from Table in split level, the split is assigned by primary key
 * range.
 */
public class SnapshotSplitReader implements DebeziumReader<SourceRecord, MySQLSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executor;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;

    // task to read snapshot for current split
    private MySQLSnapshotSplitReadTask splitSnapshotReadTask;
    private MySQLSplit currentTableSplit;
    private AtomicBoolean hasNextElement;
    private AtomicBoolean reachEnd;

    public SnapshotSplitReader(StatefulTaskContext statefulTaskContext, int subtaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subtaskId).build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = false;
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    public void submitSplit(MySQLSplit mySQLSplit) {
        this.currentTableSplit = mySQLSplit;
        statefulTaskContext.configure(currentTableSplit);
        this.queue = statefulTaskContext.getQueue();
        this.hasNextElement.set(true);
        this.splitSnapshotReadTask =
                new MySQLSnapshotSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getOffsetContext(),
                        statefulTaskContext.getSnapshotChangeEventSourceMetrics(),
                        statefulTaskContext.getDatabaseSchema(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getTopicSelector(),
                        StatefulTaskContext.getClock(),
                        currentTableSplit);
        executor.submit(
                () -> {
                    try {
                        currentTaskRunning = true;
                        // execute snapshot read task
                        final SnapshotSplitChangeEventSourceContextImpl sourceContext =
                                new SnapshotSplitChangeEventSourceContextImpl();
                        SnapshotResult snapshotResult =
                                splitSnapshotReadTask.execute(sourceContext);

                        final MySQLSplitState mySQLSplitState =
                                new MySQLSplitState(currentTableSplit);
                        mySQLSplitState.setLowWatermarkState(sourceContext.getLowWatermark());
                        mySQLSplitState.setOffsetState(sourceContext.getLowWatermark());
                        mySQLSplitState.setHighWatermarkState(sourceContext.getHighWatermark());
                        final MySQLSplit binlogReadSplit = mySQLSplitState.toMySQLSplit();
                        final MySqlOffsetContext mySqlOffsetContext =
                                statefulTaskContext.getOffsetContext();
                        mySqlOffsetContext.setBinlogStartPoint(
                                binlogReadSplit.getLowWatermark().getFilename(),
                                binlogReadSplit.getLowWatermark().getPosition());

                        // execute binlog read task
                        if (snapshotResult.isCompletedOrSkipped()) {
                            // task to read binlog for current split
                            MySQLBinlogSplitReadTask splitBinlogReadTask =
                                    new MySQLBinlogSplitReadTask(
                                            statefulTaskContext.getConnectorConfig(),
                                            mySqlOffsetContext,
                                            statefulTaskContext.getConnection(),
                                            statefulTaskContext.getDispatcher(),
                                            statefulTaskContext.getErrorHandler(),
                                            StatefulTaskContext.getClock(),
                                            statefulTaskContext.getTaskContext(),
                                            (MySqlStreamingChangeEventSourceMetrics)
                                                    statefulTaskContext
                                                            .getStreamingChangeEventSourceMetrics(),
                                            statefulTaskContext
                                                    .getTopicSelector()
                                                    .topicNameFor(binlogReadSplit.getTableId()),
                                            binlogReadSplit);
                            splitBinlogReadTask.execute(
                                    new SnapshotBinlogSplitChangeEventSourceContextImpl());
                        } else {
                            throw new IllegalStateException(
                                    String.format(
                                            "read snapshot for mysql split %s fail",
                                            currentTableSplit));
                        }
                    } catch (Exception e) {
                        currentTaskRunning = false;
                        e.printStackTrace();
                    }
                });
    }

    @Override
    public boolean isIdle() {
        return currentTableSplit == null
                || (!currentTaskRunning && !hasNextElement.get() && reachEnd.get());
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
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
            return normalizedSplitRecords(currentTableSplit, sourceRecords).iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link MySQLSplit}.
     */
    public class SnapshotSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {

        private BinlogPosition lowWatermark;
        private BinlogPosition highWatermark;

        public BinlogPosition getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(BinlogPosition lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public BinlogPosition getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(BinlogPosition highWatermark) {
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
