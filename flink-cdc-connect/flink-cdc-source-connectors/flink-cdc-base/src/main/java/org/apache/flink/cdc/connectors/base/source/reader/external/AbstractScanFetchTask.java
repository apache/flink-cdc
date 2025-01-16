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

package org.apache.flink.cdc.connectors.base.source.reader.external;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/** An abstract {@link FetchTask} implementation to read snapshot split. */
public abstract class AbstractScanFetchTask implements FetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractScanFetchTask.class);
    protected volatile boolean taskRunning = false;

    protected final SnapshotSplit snapshotSplit;
    private SnapshotPhaseHooks snapshotPhaseHooks = SnapshotPhaseHooks.empty();

    public AbstractScanFetchTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        LOG.info("Execute ScanFetchTask for split: {}", snapshotSplit);

        DataSourceDialect dialect = context.getDataSourceDialect();
        SourceConfig sourceConfig = context.getSourceConfig();

        taskRunning = true;

        if (snapshotPhaseHooks.getPreLowWatermarkAction() != null) {
            snapshotPhaseHooks.getPreLowWatermarkAction().accept(sourceConfig, snapshotSplit);
        }
        final Offset lowWatermark = dialect.displayCurrentOffset(sourceConfig);
        LOG.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        dispatchLowWaterMarkEvent(context, snapshotSplit, lowWatermark);
        if (snapshotPhaseHooks.getPostLowWatermarkAction() != null) {
            snapshotPhaseHooks.getPostLowWatermarkAction().accept(sourceConfig, snapshotSplit);
        }

        LOG.info("Snapshot step 2 - Snapshotting data");
        executeDataSnapshot(context);

        if (snapshotPhaseHooks.getPreHighWatermarkAction() != null) {
            snapshotPhaseHooks.getPreHighWatermarkAction().accept(sourceConfig, snapshotSplit);
        }
        // Directly set HW = LW if backfill is skipped. Stream events created during snapshot
        // phase could be processed later in stream reading phase.
        //
        // Note that this behaviour downgrades the delivery guarantee to at-least-once. We can't
        // promise that the snapshot is exactly the view of the table at low watermark moment,
        // so stream events created during snapshot might be replayed later in stream reading
        // phase.
        Offset highWatermark =
                context.getSourceConfig().isSkipSnapshotBackfill()
                        ? lowWatermark
                        : dialect.displayCurrentOffset(sourceConfig);
        LOG.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        dispatchHighWaterMarkEvent(context, snapshotSplit, highWatermark);
        if (snapshotPhaseHooks.getPostHighWatermarkAction() != null) {
            snapshotPhaseHooks.getPostHighWatermarkAction().accept(sourceConfig, snapshotSplit);
        }

        // optimization that skip the stream read when the low watermark equals high watermark
        final StreamSplit backfillStreamSplit =
                createBackfillStreamSplit(lowWatermark, highWatermark);
        final boolean streamBackfillRequired =
                backfillStreamSplit
                        .getEndingOffset()
                        .isAfter(backfillStreamSplit.getStartingOffset());

        if (!streamBackfillRequired) {
            LOG.info(
                    "Skip the backfill {} for split {}: low watermark >= high watermark",
                    backfillStreamSplit,
                    snapshotSplit);
            dispatchEndWaterMarkEvent(
                    context, backfillStreamSplit, backfillStreamSplit.getEndingOffset());
        } else {
            executeBackfillTask(context, backfillStreamSplit);
        }

        taskRunning = false;
    }

    protected StreamSplit createBackfillStreamSplit(Offset lowWatermark, Offset highWatermark) {
        return new StreamSplit(
                snapshotSplit.splitId(),
                lowWatermark,
                highWatermark,
                new ArrayList<>(),
                snapshotSplit.getTableSchemas(),
                0);
    }

    protected abstract void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception;

    protected abstract void executeDataSnapshot(Context context) throws Exception;

    /** Dispatch low watermark event, which means the beginning of snapshot data. */
    protected void dispatchLowWaterMarkEvent(
            Context context, SourceSplitBase split, Offset lowWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getWaterMarkDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            lowWatermark,
                            WatermarkKind.LOW);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    /**
     * Dispatch high watermark event, which means the end of snapshot data and also the beginning of
     * backfill data. Data change events between (low_watermark, high_watermark) are snapshot data.
     */
    protected void dispatchHighWaterMarkEvent(
            Context context, SourceSplitBase split, Offset highWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getWaterMarkDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            highWatermark,
                            WatermarkKind.HIGH);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    /**
     * Dispatch end watermark event, which means the end of backfill data. Data change events
     * between ( high_watermark, end_watermark ) are backfill data, which maybe duplicate of
     * snapshot data. Thus, only the intersection of both is exactly-once.
     */
    protected void dispatchEndWaterMarkEvent(
            Context context, SourceSplitBase split, Offset endWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getWaterMarkDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            endWatermark,
                            WatermarkKind.END);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SnapshotSplit getSplit() {
        return snapshotSplit;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    @VisibleForTesting
    public void setSnapshotPhaseHooks(SnapshotPhaseHooks snapshotPhaseHooks) {
        this.snapshotPhaseHooks = snapshotPhaseHooks;
    }
}
