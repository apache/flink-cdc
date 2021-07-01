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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.task;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader.SnapshotBinlogSplitChangeEventSourceContextImpl;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitKind;
import com.github.shyiko.mysql.binlog.event.Event;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.BINLOG_FILENAME_OFFSET_KEY;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.BINLOG_POSITION_OFFSET_KEY;

/**
 * Task to read all binlog for table and also supports read bounded (from lowWatermark to
 * highWatermark) binlog.
 */
public class MySQLBinlogSplitReadTask extends MySqlStreamingChangeEventSource {

    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogSplitReadTask.class);
    private final MySQLSplit mySQLSplit;
    private final MySqlOffsetContext offsetContext;
    private final EventDispatcherImpl<TableId> eventDispatcher;
    private final SignalEventDispatcher signalEventDispatcher;
    private final ErrorHandler errorHandler;
    private ChangeEventSourceContext context;

    public MySQLBinlogSplitReadTask(
            MySqlConnectorConfig connectorConfig,
            MySqlOffsetContext offsetContext,
            MySqlConnection connection,
            EventDispatcherImpl<TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            MySqlTaskContext taskContext,
            MySqlStreamingChangeEventSourceMetrics metrics,
            String topic,
            MySQLSplit mySQLSplit) {
        super(
                connectorConfig,
                offsetContext,
                connection,
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                metrics);
        this.mySQLSplit = mySQLSplit;
        this.eventDispatcher = dispatcher;
        this.offsetContext = offsetContext;
        this.errorHandler = errorHandler;
        this.signalEventDispatcher =
                new SignalEventDispatcher(offsetContext, topic, eventDispatcher.getQueue());
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        this.context = context;
        super.execute(context);
    }

    @Override
    protected void handleEvent(Event event) {
        super.handleEvent(event);
        // check do we need to stop for read binlog for snapshot split.
        if (isBoundedRead()) {
            final com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition
                    currentBinlogPosition =
                            new com.alibaba.ververica.cdc.connectors.mysql.debezium.offset
                                    .BinlogPosition(
                                    offsetContext
                                            .getOffset()
                                            .get(BINLOG_FILENAME_OFFSET_KEY)
                                            .toString(),
                                    Long.parseLong(
                                            offsetContext
                                                    .getOffset()
                                                    .get(BINLOG_POSITION_OFFSET_KEY)
                                                    .toString()));
            // reach the high watermark, the binlog reader should finished
            if (currentBinlogPosition.isAtOrBefore(mySQLSplit.getHighWatermark())) {
                // send binlog end event
                try {
                    signalEventDispatcher.dispatchWatermarkEvent(
                            mySQLSplit,
                            currentBinlogPosition,
                            SignalEventDispatcher.WatermarkKind.BINLOG_END);
                } catch (InterruptedException e) {
                    logger.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing binlog signal event", e));
                }
                // tell reader the binlog task finished
                ((SnapshotBinlogSplitChangeEventSourceContextImpl) context).finished();
            }
        }
    }

    private boolean isBoundedRead() {
        return mySQLSplit.getSplitKind() == MySQLSplitKind.SNAPSHOT
                && mySQLSplit.getHighWatermark() != null;
    }
}
