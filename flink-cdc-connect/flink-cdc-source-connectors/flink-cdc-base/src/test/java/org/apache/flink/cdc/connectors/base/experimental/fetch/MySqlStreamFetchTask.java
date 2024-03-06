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

package org.apache.flink.cdc.connectors.base.experimental.fetch;

import org.apache.flink.cdc.connectors.base.experimental.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import com.github.shyiko.mysql.binlog.event.Event;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** The task to work for fetching data of MySQL table stream split . */
public class MySqlStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;

    public MySqlStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;
        MySqlBinlogSplitReadTask binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split);
        BinlogSplitChangeEventSourceContext changeEventSourceContext =
                new BinlogSplitChangeEventSourceContext();
        binlogSplitReadTask.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public StreamSplit getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    /**
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class MySqlBinlogSplitReadTask extends MySqlStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitReadTask.class);
        private final StreamSplit binlogSplit;
        private final JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public MySqlBinlogSplitReadTask(
                MySqlConnectorConfig connectorConfig,
                MySqlConnection connection,
                JdbcSourceEventDispatcher<MySqlPartition> dispatcher,
                ErrorHandler errorHandler,
                MySqlTaskContext taskContext,
                MySqlStreamingChangeEventSourceMetrics metrics,
                StreamSplit binlogSplit) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    taskContext,
                    metrics);
            this.binlogSplit = binlogSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                MySqlPartition partition,
                MySqlOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, partition, offsetContext);
        }

        @Override
        protected void handleEvent(
                MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
            super.handleEvent(partition, offsetContext, event);
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                final BinlogOffset currentBinlogOffset =
                        getBinlogPosition(offsetContext.getOffset());
                // reach the high watermark, the binlog fetcher should be finished
                if (currentBinlogOffset.isAtOrAfter(binlogSplit.getEndingOffset())) {
                    // send binlog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                partition.getSourcePartition(),
                                binlogSplit,
                                currentBinlogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the binlog task finished
                    ((MySqlScanFetchTask.MysqlSnapshotSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !BinlogOffset.NO_STOPPING_OFFSET.equals(binlogSplit.getEndingOffset());
        }

        public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
            Map<String, String> offsetStrMap = new HashMap<>();
            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                offsetStrMap.put(
                        entry.getKey(),
                        entry.getValue() == null ? null : entry.getValue().toString());
            }
            return new BinlogOffset(offsetStrMap);
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for binlog split task.
     */
    private class BinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
