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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** The task to work for fetching data of Oracle table stream split. */
@Internal
public class OracleStreamFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(OracleStreamFetchTask.class);
    private static final String XSTREAM_COMMIT_PROCESSED_LOW_WATERMARK_ENABLED =
            "xstream.commit.processed.low.watermark.enabled";

    private final StreamSplit split;
    private volatile boolean taskRunning = false;
    private volatile RunningChangeEventSource runningChangeEventSource;
    @Nullable private volatile Offset lastCommittedOffset;

    public OracleStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        final OracleConnectorConfig connectorConfig = sourceFetchContext.getDbzConnectorConfig();
        validateAdapterSupportsSplit(connectorConfig, split);

        if (OracleSourceFetchTaskContext.isLogMinerAdapter(connectorConfig)) {
            executeLogMinerTask(sourceFetchContext, connectorConfig);
            return;
        }
        if (OracleSourceFetchTaskContext.isXStreamAdapter(connectorConfig)) {
            executeXStreamTask(sourceFetchContext, connectorConfig);
            return;
        }

        executeWithAdapter(sourceFetchContext, connectorConfig);
    }

    private void executeLogMinerTask(
            OracleSourceFetchTaskContext sourceFetchContext,
            OracleConnectorConfig connectorConfig) {
        RedoLogSplitReadTask redoLogSplitReadTask =
                new RedoLogSplitReadTask(
                        connectorConfig,
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        sourceFetchContext.getWaterMarkDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split,
                        sourceFetchContext.getStartupTimestampMillis());
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        RunningChangeEventSource runningChangeEventSource =
                registerRunningChangeEventSource(changeEventSourceContext, null);
        try {
            redoLogSplitReadTask.execute(
                    changeEventSourceContext,
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.getOffsetContext());
        } finally {
            clearRunningChangeEventSource(runningChangeEventSource);
        }
    }

    private void executeWithAdapter(
            OracleSourceFetchTaskContext sourceFetchContext, OracleConnectorConfig connectorConfig)
            throws InterruptedException {
        StreamingChangeEventSource<OraclePartition, OracleOffsetContext> streamingSource =
                connectorConfig
                        .getAdapter()
                        .getSource(
                                sourceFetchContext.getConnection(),
                                sourceFetchContext.getEventDispatcher(),
                                sourceFetchContext.getErrorHandler(),
                                Clock.SYSTEM,
                                sourceFetchContext.getDatabaseSchema(),
                                sourceFetchContext.getTaskContext(),
                                sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                                sourceFetchContext.getStreamingChangeEventSourceMetrics());

        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        XstreamStreamingChangeEventSource xstreamStreamingSource =
                streamingSource instanceof XstreamStreamingChangeEventSource
                        ? (XstreamStreamingChangeEventSource) streamingSource
                        : null;
        RunningChangeEventSource runningChangeEventSource =
                registerRunningChangeEventSource(
                        changeEventSourceContext,
                        xstreamStreamingSource,
                        isXStreamCommitProcessedLowWatermarkEnabled(sourceFetchContext));
        try {
            streamingSource.execute(
                    changeEventSourceContext,
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.getOffsetContext());
        } finally {
            clearRunningChangeEventSource(runningChangeEventSource);
        }
    }

    private void executeXStreamTask(
            OracleSourceFetchTaskContext sourceFetchContext, OracleConnectorConfig connectorConfig)
            throws InterruptedException {
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        XStreamSplitReadTask xstreamFetchTask =
                XStreamSplitReadTask.createTask(
                        connectorConfig,
                        sourceFetchContext,
                        split,
                        sourceFetchContext.getStartupTimestampMillis());
        RunningChangeEventSource runningChangeEventSource =
                registerRunningChangeEventSource(
                        changeEventSourceContext,
                        xstreamFetchTask,
                        isXStreamCommitProcessedLowWatermarkEnabled(sourceFetchContext));
        try {
            xstreamFetchTask.execute(
                    changeEventSourceContext,
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.getOffsetContext());
        } finally {
            clearRunningChangeEventSource(runningChangeEventSource);
        }
    }

    static void validateAdapterSupportsSplit(
            OracleConnectorConfig connectorConfig, StreamSplit streamSplit) {
        if (RedoLogOffset.NO_STOPPING_OFFSET.equals(streamSplit.getEndingOffset())) {
            return;
        }
        if (OracleSourceFetchTaskContext.isLogMinerAdapter(connectorConfig)) {
            return;
        }
        throw new DebeziumException(
                String.format(
                        "Oracle adapter '%s' does not support bounded stream split backfill yet.",
                        connectorConfig.getAdapter().getType()));
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
        RunningChangeEventSource runningChangeEventSource = this.runningChangeEventSource;
        if (runningChangeEventSource != null) {
            runningChangeEventSource.stop();
        }
    }

    RunningChangeEventSource registerRunningChangeEventSource(
            StoppableChangeEventSourceContext changeEventSourceContext,
            XstreamStreamingChangeEventSource xstreamStreamingSource) {
        return registerRunningChangeEventSource(
                changeEventSourceContext, xstreamStreamingSource, false);
    }

    RunningChangeEventSource registerRunningChangeEventSource(
            StoppableChangeEventSourceContext changeEventSourceContext,
            XstreamStreamingChangeEventSource xstreamStreamingSource,
            boolean commitProcessedLowWatermarkEnabled) {
        RunningChangeEventSource runningChangeEventSource =
                new ActiveChangeEventSource(
                        changeEventSourceContext,
                        xstreamStreamingSource,
                        commitProcessedLowWatermarkEnabled);
        this.runningChangeEventSource = runningChangeEventSource;
        return runningChangeEventSource;
    }

    void clearRunningChangeEventSource(RunningChangeEventSource runningChangeEventSource) {
        if (this.runningChangeEventSource == runningChangeEventSource) {
            this.runningChangeEventSource = null;
        }
    }

    public void commitCurrentOffset(@Nullable Offset offsetToCommit) {
        if (offsetToCommit == null) {
            return;
        }
        if (lastCommittedOffset != null && offsetToCommit.compareTo(lastCommittedOffset) <= 0) {
            return;
        }

        RunningChangeEventSource current = this.runningChangeEventSource;
        if (current != null && current.commitOffset(offsetToCommit)) {
            lastCommittedOffset = offsetToCommit;
            LOG.info("Committed Oracle stream offset {} for {}", offsetToCommit, split);
        }
    }

    private boolean isXStreamCommitProcessedLowWatermarkEnabled(
            OracleSourceFetchTaskContext sourceFetchContext) {
        return Boolean.parseBoolean(
                sourceFetchContext
                        .getSourceConfig()
                        .getOriginDbzConnectorConfig()
                        .getString(XSTREAM_COMMIT_PROCESSED_LOW_WATERMARK_ENABLED, "false"));
    }

    @VisibleForTesting
    void setRunningChangeEventSource(RunningChangeEventSource runningChangeEventSource) {
        this.runningChangeEventSource = runningChangeEventSource;
    }

    interface RunningChangeEventSource {
        void stop();

        default boolean commitOffset(Offset offset) {
            return false;
        }
    }

    private static final class ActiveChangeEventSource implements RunningChangeEventSource {

        private final StoppableChangeEventSourceContext changeEventSourceContext;
        private final XstreamStreamingChangeEventSource xstreamStreamingSource;
        private final boolean commitProcessedLowWatermarkEnabled;

        private ActiveChangeEventSource(
                StoppableChangeEventSourceContext changeEventSourceContext,
                XstreamStreamingChangeEventSource xstreamStreamingSource,
                boolean commitProcessedLowWatermarkEnabled) {
            this.changeEventSourceContext = changeEventSourceContext;
            this.xstreamStreamingSource = xstreamStreamingSource;
            this.commitProcessedLowWatermarkEnabled = commitProcessedLowWatermarkEnabled;
        }

        @Override
        public void stop() {
            changeEventSourceContext.stopChangeEventSource();
            if (xstreamStreamingSource != null) {
                xstreamStreamingSource.closeXStreamSession();
            }
        }

        @Override
        public boolean commitOffset(Offset offset) {
            if (xstreamStreamingSource == null) {
                return false;
            }
            if (!commitProcessedLowWatermarkEnabled) {
                LOG.debug(
                        "Skip committing Oracle XStream offset because {} is disabled: {}",
                        XSTREAM_COMMIT_PROCESSED_LOW_WATERMARK_ENABLED,
                        offset);
                return false;
            }
            String lcrPosition = offset.getOffset().get(RedoLogOffset.LCR_POSITION_KEY);
            if (lcrPosition == null || "null".equalsIgnoreCase(lcrPosition)) {
                LOG.debug("Skip committing Oracle XStream offset without LCR position: {}", offset);
                return false;
            }
            xstreamStreamingSource.commitOffset(offset.getOffset());
            return true;
        }
    }

    /**
     * A wrapped task to read all redo log for table and also supports read bounded (from
     * lowWatermark to highWatermark) redo log.
     */
    public static class RedoLogSplitReadTask extends LogMinerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(RedoLogSplitReadTask.class);
        private final StreamSplit redoLogSplit;
        EventDispatcher<OraclePartition, TableId> eventDispatcher;
        private final WatermarkDispatcher watermarkDispatcher;
        private final ErrorHandler errorHandler;
        private final OracleConnectorConfig connectorConfig;
        private final OracleConnection connection;

        private final OracleDatabaseSchema schema;

        private final OracleStreamingChangeEventSourceMetrics metrics;
        private final Long startupTimestampMillis;

        public RedoLogSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleConnection connection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                ErrorHandler errorHandler,
                OracleDatabaseSchema schema,
                Configuration jdbcConfig,
                OracleStreamingChangeEventSourceMetrics metrics,
                StreamSplit redoLogSplit,
                Long startupTimestampMillis) {
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
            this.connectorConfig = connectorConfig;
            this.connection = connection;
            this.metrics = metrics;
            this.schema = schema;
            this.startupTimestampMillis = startupTimestampMillis;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext offsetContext) {
            super.execute(context, partition, offsetContext);
        }

        /**
         * Delegate {@link EventProcessorFactory} to produce a LogMinerEventProcessor with enhanced
         * processRow method to distinguish whether is bounded.
         */
        @Override
        protected LogMinerEventProcessor createProcessor(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext offsetContext) {
            return EventProcessorFactory.createProcessor(
                    context,
                    connectorConfig,
                    connection,
                    eventDispatcher,
                    watermarkDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit,
                    startupTimestampMillis);
        }
    }

    /** Task to read XStream events and stop when reaching split ending watermark. */
    public static class XStreamSplitReadTask extends XstreamStreamingChangeEventSource {

        private final WatermarkDispatcher watermarkDispatcher;
        private final StreamSplit streamSplit;
        private boolean endWatermarkDispatched;

        public XStreamSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleConnection connection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                ErrorHandler errorHandler,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                WatermarkDispatcher watermarkDispatcher,
                StreamSplit streamSplit,
                Long startupTimestampMillis) {
            super(
                    connectorConfig,
                    connection,
                    eventDispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    metrics,
                    startupTimestampMillis);
            this.watermarkDispatcher = watermarkDispatcher;
            this.streamSplit = streamSplit;
        }

        @Override
        protected void onAfterReceive(
                ChangeEventSource.ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext offsetContext)
                throws InterruptedException {
            if (reachEndingOffset(context, partition, offsetContext)) {
                return;
            }
            super.onAfterReceive(context, partition, offsetContext);
        }

        @Override
        protected int getAttachBusyRetryTimes() {
            return 0;
        }

        private boolean reachEndingOffset(
                ChangeEventSource.ChangeEventSourceContext sourceContext,
                OraclePartition partition,
                OracleOffsetContext offsetContext)
                throws InterruptedException {
            if (endWatermarkDispatched
                    || RedoLogOffset.NO_STOPPING_OFFSET.equals(streamSplit.getEndingOffset())) {
                return false;
            }
            if (offsetContext.getScn() == null) {
                return false;
            }

            RedoLogOffset currentOffset = new RedoLogOffset(offsetContext.getScn().longValue());
            if (!currentOffset.isAtOrAfter(streamSplit.getEndingOffset())) {
                return false;
            }

            watermarkDispatcher.dispatchWatermarkEvent(
                    partition.getSourcePartition(), streamSplit, currentOffset, WatermarkKind.END);
            endWatermarkDispatched = true;
            if (sourceContext instanceof StoppableChangeEventSourceContext) {
                ((StoppableChangeEventSourceContext) sourceContext).stopChangeEventSource();
            }
            return true;
        }

        static XStreamSplitReadTask createTask(
                OracleConnectorConfig connectorConfig,
                OracleSourceFetchTaskContext sourceFetchContext,
                StreamSplit streamSplit,
                Long startupTimestampMillis) {
            return new XStreamSplitReadTask(
                    connectorConfig,
                    sourceFetchContext.getConnection(),
                    sourceFetchContext.getEventDispatcher(),
                    sourceFetchContext.getErrorHandler(),
                    sourceFetchContext.getDatabaseSchema(),
                    sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                    sourceFetchContext.getWaterMarkDispatcher(),
                    streamSplit,
                    startupTimestampMillis);
        }
    }
}
