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
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.EmbeddedInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.RemoteInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Factory to produce a LogMinerEventProcessor with enhanced processRow method to distinguish
 * whether is bounded.
 */
@Internal
public class EventProcessorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessorFactory.class);

    private EventProcessorFactory() {}

    public static LogMinerEventProcessor createProcessor(
            ChangeEventSource.ChangeEventSourceContext context,
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<OraclePartition, TableId> eventDispatcher,
            WatermarkDispatcher watermarkDispatcher,
            OraclePartition partition,
            OracleOffsetContext offsetContext,
            OracleDatabaseSchema schema,
            OracleStreamingChangeEventSourceMetrics metrics,
            ErrorHandler errorHandler,
            StreamSplit redoLogSplit,
            Long startupTimestampMillis) {
        final OracleConnectorConfig.LogMiningBufferType bufferType =
                connectorConfig.getLogMiningBufferType();
        if (bufferType.equals(OracleConnectorConfig.LogMiningBufferType.MEMORY)) {
            return new CDCMemoryLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    watermarkDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit,
                    startupTimestampMillis);
        } else if (bufferType.equals(
                OracleConnectorConfig.LogMiningBufferType.INFINISPAN_EMBEDDED)) {
            return new CDCEmbeddedInfinispanLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    watermarkDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit,
                    startupTimestampMillis);
        } else if (bufferType.equals(OracleConnectorConfig.LogMiningBufferType.INFINISPAN_REMOTE)) {
            return new CDCRemoteInfinispanLogMinerEventProcessor(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    watermarkDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics,
                    errorHandler,
                    redoLogSplit,
                    startupTimestampMillis);
        } else {
            throw new IllegalArgumentException(
                    "not support this type of bufferType: " + bufferType);
        }
    }

    /**
     * A {@link MemoryLogMinerEventProcessor} with enhanced processRow method to distinguish whether
     * is bounded.
     */
    public static class CDCMemoryLogMinerEventProcessor extends MemoryLogMinerEventProcessor {
        private final StreamSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final WatermarkDispatcher watermarkDispatcher;
        private final StartupTimestampFilter startupTimestampFilter;

        public CDCMemoryLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                StreamSplit redoLogSplit,
                Long startupTimestampMillis) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.watermarkDispatcher = watermarkDispatcher;
            this.startupTimestampFilter = new StartupTimestampFilter(startupTimestampMillis);
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, watermarkDispatcher, context)) {
                return;
            }
            if (startupTimestampFilter.shouldSkip(row)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    /**
     * A {@link EmbeddedInfinispanLogMinerEventProcessor} with enhanced processRow method to
     * distinguish whether is bounded.
     */
    public static class CDCEmbeddedInfinispanLogMinerEventProcessor
            extends EmbeddedInfinispanLogMinerEventProcessor {
        private final StreamSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final WatermarkDispatcher watermarkDispatcher;
        private final StartupTimestampFilter startupTimestampFilter;

        public CDCEmbeddedInfinispanLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                StreamSplit redoLogSplit,
                Long startupTimestampMillis) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.watermarkDispatcher = watermarkDispatcher;
            this.startupTimestampFilter = new StartupTimestampFilter(startupTimestampMillis);
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, watermarkDispatcher, context)) {
                return;
            }
            if (startupTimestampFilter.shouldSkip(row)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    /**
     * A {@link CDCRemoteInfinispanLogMinerEventProcessor} with enhanced processRow method to
     * distinguish whether is bounded.
     */
    public static class CDCRemoteInfinispanLogMinerEventProcessor
            extends RemoteInfinispanLogMinerEventProcessor {
        private final StreamSplit redoLogSplit;
        private final ErrorHandler errorHandler;

        private ChangeEventSource.ChangeEventSourceContext context;
        private final WatermarkDispatcher watermarkDispatcher;
        private final StartupTimestampFilter startupTimestampFilter;

        public CDCRemoteInfinispanLogMinerEventProcessor(
                ChangeEventSource.ChangeEventSourceContext context,
                OracleConnectorConfig connectorConfig,
                OracleConnection jdbcConnection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                OraclePartition partition,
                OracleOffsetContext offsetContext,
                OracleDatabaseSchema schema,
                OracleStreamingChangeEventSourceMetrics metrics,
                ErrorHandler errorHandler,
                StreamSplit redoLogSplit,
                Long startupTimestampMillis) {
            super(
                    context,
                    connectorConfig,
                    jdbcConnection,
                    eventDispatcher,
                    partition,
                    offsetContext,
                    schema,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.errorHandler = errorHandler;
            this.context = context;
            this.watermarkDispatcher = watermarkDispatcher;
            this.startupTimestampFilter = new StartupTimestampFilter(startupTimestampMillis);
        }

        @Override
        protected void processRow(OraclePartition partition, LogMinerEventRow row)
                throws SQLException, InterruptedException {
            if (reachEndingOffset(
                    partition, row, redoLogSplit, errorHandler, watermarkDispatcher, context)) {
                return;
            }
            if (startupTimestampFilter.shouldSkip(row)) {
                return;
            }
            super.processRow(partition, row);
        }
    }

    private static class StartupTimestampFilter {
        private final Long startupTimestampMillis;
        private boolean reachedTimestamp;
        private long filtered;
        private boolean seekStarted;

        private StartupTimestampFilter(Long startupTimestampMillis) {
            this.startupTimestampMillis = startupTimestampMillis;
        }

        private boolean shouldSkip(LogMinerEventRow row) {
            if (startupTimestampMillis == null
                    || reachedTimestamp
                    || !isRowMutation(row.getEventType())) {
                return false;
            }
            if (!seekStarted) {
                seekStarted = true;
                LOG.info(
                        "Begin to seek Oracle redo log to startup timestamp {}.",
                        startupTimestampMillis);
            }
            if (row.getChangeTime() != null
                    && row.getChangeTime().toEpochMilli() >= startupTimestampMillis) {
                reachedTimestamp = true;
                LOG.info(
                        "Successfully seek Oracle redo log to startup timestamp {} with filtered {} change events.",
                        startupTimestampMillis,
                        filtered);
                return false;
            }
            filtered++;
            if (filtered % 10000 == 0) {
                LOG.info(
                        "Seeking Oracle redo log to startup timestamp {} with filtered {} change events.",
                        startupTimestampMillis,
                        filtered);
            }
            return true;
        }
    }

    private static boolean isRowMutation(EventType eventType) {
        return EventType.INSERT.equals(eventType)
                || EventType.UPDATE.equals(eventType)
                || EventType.DELETE.equals(eventType)
                || EventType.SELECT_LOB_LOCATOR.equals(eventType)
                || EventType.LOB_WRITE.equals(eventType)
                || EventType.LOB_ERASE.equals(eventType)
                || EventType.LOB_TRIM.equals(eventType);
    }

    public static boolean reachEndingOffset(
            OraclePartition partition,
            LogMinerEventRow row,
            StreamSplit redoLogSplit,
            ErrorHandler errorHandler,
            WatermarkDispatcher dispatcher,
            ChangeEventSource.ChangeEventSourceContext context) {
        // check do we need to stop for fetch redo log for snapshot split.
        if (isBoundedRead(redoLogSplit)) {
            final RedoLogOffset currentRedoLogOffset = new RedoLogOffset(row.getScn().longValue());
            // reach the high watermark, the redo log fetcher should be finished
            if (currentRedoLogOffset.isAtOrAfter(redoLogSplit.getEndingOffset())) {
                // send redo log end event
                try {
                    dispatcher.dispatchWatermarkEvent(
                            partition.getSourcePartition(),
                            redoLogSplit,
                            currentRedoLogOffset,
                            WatermarkKind.END);
                } catch (InterruptedException e) {
                    LOG.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing redo log signal event", e));
                }
                // tell fetcher the redo log task finished
                ((StoppableChangeEventSourceContext) context).stopChangeEventSource();
                return true;
            }
        }
        return false;
    }

    private static boolean isBoundedRead(StreamSplit redoLogSplit) {
        return !RedoLogOffset.NO_STOPPING_OFFSET.equals(redoLogSplit.getEndingOffset());
    }
}
