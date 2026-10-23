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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task to work for fetching data of Oracle table stream split. */
@Internal
public class OracleStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;

    public OracleStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        RedoLogSplitReadTask redoLogSplitReadTask =
                new RedoLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        sourceFetchContext.getWaterMarkDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split);
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        redoLogSplitReadTask.execute(
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
     * A wrapped task to read all redo log for table and also supports read bounded (from
     * lowWatermark to highWatermark) redo log.
     *
     * <p>Overrides {@link LogMinerStreamingChangeEventSource#handleRowPreProcessing} to intercept
     * each LogMiner event row and check whether the bounded split's high-watermark has been
     * reached.
     */
    public static class RedoLogSplitReadTask extends LogMinerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(RedoLogSplitReadTask.class);
        private final StreamSplit redoLogSplit;
        EventDispatcher<OraclePartition, TableId> eventDispatcher;
        private final WatermarkDispatcher watermarkDispatcher;
        private final ErrorHandler errorHandler;

        public RedoLogSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleConnection connection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                ErrorHandler errorHandler,
                OracleDatabaseSchema schema,
                Configuration jdbcConfig,
                AbstractOracleStreamingChangeEventSourceMetrics metrics,
                StreamSplit redoLogSplit) {
            super(
                    connectorConfig,
                    connection,
                    eventDispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    jdbcConfig,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.eventDispatcher = eventDispatcher;
            this.watermarkDispatcher = watermarkDispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext offsetContext)
                throws InterruptedException {
            super.execute(context, partition, offsetContext);
        }

        /**
         * Intercepts each LogMiner event row before normal processing to detect the end of a
         * bounded stream split. Returns {@code true} (skip + stop) when the row's SCN has reached
         * or passed the split's high-watermark.
         */
        @Override
        protected boolean handleRowPreProcessing(LogMinerEventRow row) throws InterruptedException {
            return EventProcessorFactory.reachEndingOffset(
                    getPartition(),
                    row,
                    redoLogSplit,
                    errorHandler,
                    watermarkDispatcher,
                    getContext());
        }
    }
}
