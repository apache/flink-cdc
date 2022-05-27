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

package com.ververica.cdc.connectors.sqlserver.experimental.fetch;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.connector.sqlserver.SqlServerTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ververica.cdc.connectors.sqlserver.experimental.offset.TransactionLogOffset.NO_STOPPING_OFFSET;

/** The task to work for fetching data of Sql Server table stream split . */
public class SqlServerStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;
    private SqlServeTransactionLogSplitReadTask transactionLogSplitReadTask;

    public SqlServerStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        // TODO: 2022/5/27 stream task
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        taskRunning = true;
        /* transactionLogSplitReadTask =
        new SqlServeTransactionLogSplitReadTask(
                sourceFetchContext.getDbzConnectorConfig(),
                sourceFetchContext.getOffsetContext(),
                sourceFetchContext.getDataConnection(),
                sourceFetchContext.getDispatcher(),
                sourceFetchContext.getErrorHandler(),
                sourceFetchContext.getTaskContext(),
                sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                split);*/
        BinlogSplitChangeEventSourceContext changeEventSourceContext =
                new BinlogSplitChangeEventSourceContext();
        transactionLogSplitReadTask.execute(
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
     * A wrapped task to read all transaction log for table and also supports read bounded (from
     * lowWatermark to highWatermark) transaction log.
     */
    public static class SqlServeTransactionLogSplitReadTask
            extends SqlServerStreamingChangeEventSource {

        private static final Logger LOG =
                LoggerFactory.getLogger(SqlServeTransactionLogSplitReadTask.class);
        private final StreamSplit binlogSplit;
        private final SqlServerOffsetContext offsetContext;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        private final SqlServerDatabaseSchema schema;

        public SqlServeTransactionLogSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerOffsetContext offsetContext,
                SqlServerConnection dataConnection,
                SqlServerConnection metadataConnection,
                JdbcSourceEventDispatcher dispatcher,
                ErrorHandler errorHandler,
                SqlServerTaskContext taskContext,
                StreamSplit binlogSplit,
                SqlServerDatabaseSchema schema) {
            super(
                    connectorConfig,
                    dataConnection,
                    metadataConnection,
                    dispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema);
            this.binlogSplit = binlogSplit;
            this.dispatcher = dispatcher;
            this.offsetContext = offsetContext;
            this.errorHandler = errorHandler;
            this.schema = schema;
        }

        @Override
        public void execute(ChangeEventSourceContext context, SqlServerOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, offsetContext);
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(binlogSplit.getEndingOffset());
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
