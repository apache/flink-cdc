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

package com.ververica.cdc.connectors.base.experimental.fetch;

import com.github.shyiko.mysql.binlog.event.Event;
import com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
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

import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.NO_STOPPING_OFFSET;

/** The task to work for fetching data of MySQL table stream split . */
public class MySqlStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;
    private MySqlBinlogSplitReadTask binlogSplitReadTask;

    public MySqlStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;
        binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split);
        BinlogSplitChangeEventSourceContext changeEventSourceContext =
                new BinlogSplitChangeEventSourceContext();
        binlogSplitReadTask.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public StreamSplit getSplit() {
        return split;
    }

    /**
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class MySqlBinlogSplitReadTask extends MySqlStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitReadTask.class);
        private final StreamSplit binlogSplit;
        private final MySqlOffsetContext offsetContext;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public MySqlBinlogSplitReadTask(
                MySqlConnectorConfig connectorConfig,
                MySqlOffsetContext offsetContext,
                MySqlConnection connection,
                JdbcSourceEventDispatcher dispatcher,
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
            this.offsetContext = offsetContext;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(ChangeEventSourceContext context, MySqlOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, offsetContext);
        }

        @Override
        protected void handleEvent(MySqlOffsetContext offsetContext, Event event) {
            super.handleEvent(offsetContext, event);
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                final BinlogOffset currentBinlogOffset =
                        getBinlogPosition(offsetContext.getOffset());
                // reach the high watermark, the binlog fetcher should be finished
                if (currentBinlogOffset.isAtOrAfter(binlogSplit.getEndingOffset())) {
                    // send binlog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                offsetContext.getPartition(),
                                binlogSplit,
                                currentBinlogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the binlog task finished
                    ((MySqlScanFetchTask.SnapshotBinlogSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(binlogSplit.getEndingOffset());
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
