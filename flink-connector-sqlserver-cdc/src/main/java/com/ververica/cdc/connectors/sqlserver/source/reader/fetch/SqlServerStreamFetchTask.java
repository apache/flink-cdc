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

package com.ververica.cdc.connectors.sqlserver.source.reader.fetch;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.sqlserver.source.offset.LsnOffset;
import io.debezium.DebeziumException;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ververica.cdc.connectors.sqlserver.source.offset.LsnOffset.NO_STOPPING_OFFSET;
import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerUtils.getLsnPosition;

/** The task to work for fetching data of SqlServer table stream split . */
public class SqlServerStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;
    private LsnSplitReadTask redoLogSplitReadTask;

    public SqlServerStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        taskRunning = true;
        redoLogSplitReadTask =
                new LsnSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getMetaDataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split);
        RedoLogSplitChangeEventSourceContext changeEventSourceContext =
                new RedoLogSplitChangeEventSourceContext();
        redoLogSplitReadTask.execute(
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
    public static class LsnSplitReadTask extends SqlServerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(LsnSplitReadTask.class);
        private final StreamSplit lsnSplit;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public LsnSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerConnection connection,
                SqlServerConnection metadataConnection,
                JdbcSourceEventDispatcher dispatcher,
                ErrorHandler errorHandler,
                SqlServerDatabaseSchema schema,
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
        public void afterHandleLsn(SqlServerOffsetContext offsetContext) {
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                final LsnOffset currentRedoLogOffset = getLsnPosition(offsetContext.getOffset());
                // reach the high watermark, the binlog fetcher should be finished
                if (currentRedoLogOffset.isAtOrAfter(lsnSplit.getEndingOffset())) {
                    // send binlog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                offsetContext.getPartition(),
                                lsnSplit,
                                currentRedoLogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the binlog task finished
                    ((SqlServerScanFetchTask.SnapshotBinlogSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(lsnSplit.getEndingOffset());
        }

        @Override
        public void execute(ChangeEventSourceContext context, SqlServerOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, offsetContext);
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for binlog split task.
     */
    private class RedoLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
