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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** A {@link FetchTask} implementation for Postgres to read streaming changes. */
public class PostgresStreamFetchTask implements FetchTask<SourceSplitBase> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresStreamFetchTask.class);

    private final StreamSplit split;
    private final StoppableChangeEventSourceContext changeEventSourceContext;
    private volatile boolean taskRunning = false;
    private volatile boolean stopped = false;

    private StreamSplitReadTask streamSplitReadTask;

    private Long lastCommitLsn;

    public PostgresStreamFetchTask(StreamSplit streamSplit) {
        this.split = streamSplit;
        this.changeEventSourceContext = new StoppableChangeEventSourceContext();
    }

    @Override
    public void execute(Context context) throws Exception {
        if (stopped) {
            LOG.debug(
                    "StreamFetchTask for split: {} is already stopped and can not be executed",
                    split);
            return;
        } else {
            LOG.debug("execute StreamFetchTask for split: {}", split);
        }

        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        taskRunning = true;
        streamSplitReadTask =
                new StreamSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getSnapShotter(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        sourceFetchContext.getWaterMarkDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getTaskContext().getClock(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getReplicationConnection(),
                        split);

        streamSplitReadTask.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public void close() {
        LOG.debug("stopping StreamFetchTask for split: {}", split);
        if (streamSplitReadTask != null) {
            ((StoppableChangeEventSourceContext) (streamSplitReadTask.context))
                    .stopChangeEventSource();
        }
        stopped = true;
        taskRunning = false;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    public void commitCurrentOffset(@Nullable Offset offsetToCommit) {
        if (streamSplitReadTask != null && streamSplitReadTask.offsetContext != null) {
            PostgresOffsetContext postgresOffsetContext = streamSplitReadTask.offsetContext;

            // only extracting and storing the lsn of the last commit
            Long commitLsn =
                    (Long)
                            postgresOffsetContext
                                    .getOffset()
                                    .get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY);

            if (offsetToCommit != null) {
                // We should commit the checkpoint's LSN instead of postgresOffsetContext's LSN to
                // the slot.
                // If the checkpoint succeeds and a table UPDATE message arrives before the
                // notifyCheckpoint is called, which is represented as a BEGIN/UPDATE/COMMIT WAL
                // event sequence. The LSN of postgresOffsetContext will be updated to the LSN of
                // the COMMIT event. Committing the COMMIT LSN to the slot is incorrect because if a
                // failover occurs after the successful commission, Flink will recover from that
                // checkpoint and consume WAL starting from the slot LSN that is the LSN of COMMIT
                // event, rather than from the checkpoint's LSN. Therefore, UPDATE messages cannot
                // be consumed, resulting in data loss.
                commitLsn = ((PostgresOffset) offsetToCommit).getLsn().asLong();
            }

            if (commitLsn != null
                    && (lastCommitLsn == null
                            || Lsn.valueOf(commitLsn).compareTo(Lsn.valueOf(lastCommitLsn)) > 0)) {
                lastCommitLsn = commitLsn;

                Map<String, Object> offsets = new HashMap<>();
                offsets.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, lastCommitLsn);
                LOG.debug(
                        "Committing offset {} for {}",
                        Lsn.valueOf(lastCommitLsn),
                        streamSplitReadTask.streamSplit);
                streamSplitReadTask.commitOffset(offsets);
            }
        }
    }

    @VisibleForTesting
    StoppableChangeEventSourceContext getChangeEventSourceContext() {
        return changeEventSourceContext;
    }

    /** A {@link ChangeEventSource} implementation for Postgres to read streaming changes. */
    public static class StreamSplitReadTask extends PostgresStreamingChangeEventSource {
        private static final Logger LOG = LoggerFactory.getLogger(StreamSplitReadTask.class);
        private final StreamSplit streamSplit;
        private final WatermarkDispatcher watermarkDispatcher;
        private final ErrorHandler errorHandler;

        public ChangeEventSourceContext context;
        public PostgresOffsetContext offsetContext;

        public StreamSplitReadTask(
                PostgresConnectorConfig connectorConfig,
                Snapshotter snapshotter,
                PostgresConnection connection,
                PostgresEventDispatcher<TableId> eventDispatcher,
                WatermarkDispatcher watermarkDispatcher,
                ErrorHandler errorHandler,
                Clock clock,
                PostgresSchema schema,
                PostgresTaskContext taskContext,
                ReplicationConnection replicationConnection,
                StreamSplit streamSplit) {

            super(
                    connectorConfig,
                    snapshotter,
                    connection,
                    eventDispatcher,
                    errorHandler,
                    clock,
                    schema,
                    taskContext,
                    replicationConnection);
            this.streamSplit = streamSplit;
            this.watermarkDispatcher = watermarkDispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                PostgresPartition partition,
                PostgresOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            this.offsetContext = offsetContext;

            LOG.info("Execute StreamSplitReadTask for split: {}", streamSplit);

            offsetContext.setStreamingStoppingLsn(
                    ((PostgresOffset) streamSplit.getEndingOffset()).getLsn());
            super.execute(context, partition, offsetContext);
            if (isBoundedRead()) {
                LOG.debug("StreamSplit is bounded read: {}", streamSplit);
                final PostgresOffset currentOffset = PostgresOffset.of(offsetContext.getOffset());
                try {
                    watermarkDispatcher.dispatchWatermarkEvent(
                            partition.getSourcePartition(),
                            streamSplit,
                            currentOffset,
                            WatermarkKind.END);
                    LOG.info(
                            "StreamSplitReadTask finished for {} at {}",
                            streamSplit,
                            currentOffset);
                } catch (InterruptedException e) {
                    LOG.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new FlinkRuntimeException("Error processing WAL signal event", e));
                }

                ((StoppableChangeEventSourceContext) context).stopChangeEventSource();
            }
        }

        private boolean isBoundedRead() {
            return !PostgresOffset.NO_STOPPING_OFFSET
                    .getLsn()
                    .equals(((PostgresOffset) streamSplit.getEndingOffset()).getLsn());
        }
    }
}
