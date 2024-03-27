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

package org.apache.flink.cdc.connectors.db2.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnOffset;

import io.debezium.DebeziumException;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Db2Partition;
import io.debezium.connector.db2.Db2StreamingChangeEventSource;
import io.debezium.connector.db2.Lsn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.cdc.connectors.db2.source.offset.LsnOffset.NO_STOPPING_OFFSET;

/** The task to work for fetching data of Db2 table stream split . */
public class Db2StreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;

    public Db2StreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        sourceFetchContext.getOffsetContext().preSnapshotCompletion();
        taskRunning = true;
        StreamSplitReadTask streamSplitReadTask =
                new StreamSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getMetaDataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split);
        RedoLogSplitChangeEventSourceContext changeEventSourceContext =
                new RedoLogSplitChangeEventSourceContext();
        streamSplitReadTask.execute(
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
     * A wrapped task to read all redo logs for table and also supports read bounded (from
     * lowWatermark to highWatermark) redo logs.
     */
    public static class StreamSplitReadTask extends Db2StreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(StreamSplitReadTask.class);
        private final StreamSplit lsnSplit;
        private final JdbcSourceEventDispatcher<Db2Partition> dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public StreamSplitReadTask(
                Db2ConnectorConfig connectorConfig,
                Db2Connection connection,
                Db2Connection metadataConnection,
                JdbcSourceEventDispatcher<Db2Partition> dispatcher,
                ErrorHandler errorHandler,
                Db2DatabaseSchema schema,
                StreamSplit lsnSplit) {
            super(
                    connectorConfig,
                    connection,
                    metadataConnection,
                    dispatcher,
                    errorHandler,
                    Clock.system(),
                    schema);
            this.lsnSplit = lsnSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void afterHandleLsn(Db2Partition partition, Lsn toLsn) {
            // check do we need to stop for fetch redo logs for snapshot split.
            if (isBoundedRead()) {
                LsnOffset currentLsnOffset = new LsnOffset(null, toLsn, null);
                Offset endingOffset = lsnSplit.getEndingOffset();
                if (currentLsnOffset.isAtOrAfter(endingOffset)) {
                    // send streaming end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                partition.getSourcePartition(),
                                lsnSplit,
                                currentLsnOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException(
                                        "Error processing redo logs signal event", e));
                    }
                    // tell fetcher the streaming task finished
                    ((Db2ScanFetchTask.Db2SnapshotSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(lsnSplit.getEndingOffset());
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                Db2Partition partition,
                Db2OffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, partition, offsetContext);
        }
    }

    /** The {@link ChangeEventSourceContext} implementation for redo logs split task. */
    private class RedoLogSplitChangeEventSourceContext implements ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
