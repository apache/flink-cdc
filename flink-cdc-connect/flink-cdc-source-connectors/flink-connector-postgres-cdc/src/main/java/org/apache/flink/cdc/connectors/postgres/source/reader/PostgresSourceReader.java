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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitAckEvent;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Supplier;

/**
 * The multi-parallel postgres source reader for table snapshot phase from {@link SnapshotSplit} and
 * then single-parallel source reader for table stream phase from {@link StreamSplit}.
 *
 * <p>If scan newly added table is enable, postgres reader will not commit offset until all new
 * added tables' snapshot splits are finished.
 */
@Experimental
public class PostgresSourceReader extends IncrementalSourceReaderWithCommit {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceReader.class);

    /** whether to commit offset. */
    private volatile boolean isCommitOffset = false;

    private final PriorityQueue<Long> minHeap;
    private final int lsnCommitCheckpointsDelay;

    /**
     * Kept locally because {@link IncrementalSourceReader} holds its copy privately, and {@link
     * #onSplitFinished} needs it to tell a suspended stream split apart from one that reached its
     * ending offset.
     */
    private final IncrementalSourceReaderContext incrementalSourceReaderContext;

    public PostgresSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            SourceConfig sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
        this.lsnCommitCheckpointsDelay =
                ((PostgresSourceConfig) sourceConfig).getLsnCommitCheckpointsDelay();
        this.minHeap = new PriorityQueue<>();
        this.incrementalSourceReaderContext = incrementalSourceReaderContext;
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof OffsetCommitEvent) {
            isCommitOffset = ((OffsetCommitEvent) sourceEvent).isCommitOffset();
            context.sendSourceEventToCoordinator(new OffsetCommitAckEvent());
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (recordEmitter instanceof IncrementalSourceRecordEmitter) {
            ((IncrementalSourceRecordEmitter) recordEmitter).applySplit(split);
        }
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new StreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    protected void updateStreamSplitFinishedSplitsSize(
            LatestFinishedSplitsNumberEvent sourceEvent) {
        super.updateStreamSplitFinishedSplitsSize(sourceEvent);
        isCommitOffset = true;
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);
        if (!isCommitOffset()) {
            LOG.debug("Close offset commit of checkpoint {}", checkpointId);
            lastCheckpointOffsets.remove(checkpointId);
        }

        return sourceSplitBases;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.minHeap.add(checkpointId);
        if (this.minHeap.size() <= this.lsnCommitCheckpointsDelay) {
            LOG.info("Pending checkpoints '{}'.", this.minHeap);
            return;
        }
        final long checkpointIdToCommit = this.minHeap.poll();
        LOG.info(
                "Pending checkpoints '{}', to be committed checkpoint id '{}', isCommitOffset is {}.",
                this.minHeap,
                checkpointIdToCommit,
                isCommitOffset());

        // After all snapshot splits are finished, update stream split's metadata and reset start
        // offset, which maybe smaller than before.
        // In case that new start-offset of stream split has been recycled, don't commit offset
        // during new table added phase.
        if (isCommitOffset()) {
            super.notifyCheckpointComplete(checkpointIdToCommit);
        }
    }

    @Override
    protected void onSplitFinished(Map finishedSplitIds) {
        super.onSplitFinished(finishedSplitIds);
        for (Object splitState : finishedSplitIds.values()) {
            SourceSplitBase sourceSplit = ((SourceSplitState) splitState).toSourceSplit();
            if (sourceSplit.isStreamSplit()) {
                // A stream split finishes for one of two reasons, see
                // IncrementalSourceReader#onSplitFinished: it was suspended by the enumerator so
                // that newly added tables can be snapshotted, or it reached its ending offset.
                // Only the latter means the bounded snapshot-only read is over and the slot is no
                // longer needed.
                //
                // Comparing the split's starting and ending offsets cannot answer that question.
                // For snapshot-only, HybridSplitAssigner#createStreamSplit seeds them with the
                // lowest and highest high watermark of the finished snapshot splits, and afterwards
                // only IncrementalSourceRecordEmitter#updateStreamSplitState advances the starting
                // offset, for data-change records and heartbeats. A split that reaches its ending
                // offset without emitting such a record — a captured publication with no traffic
                // emits none — therefore left the slot behind on every run.
                if (this.sourceConfig.getStartupOptions().isSnapshotOnly()
                        && !incrementalSourceReaderContext.isStreamSplitReaderSuspended()) {
                    PostgresDialect dialect = (PostgresDialect) this.dialect;
                    String slotName = dialect.getSlotName();
                    boolean removed = dialect.removeSlot(slotName);
                    if (removed) {
                        LOG.info("Removed replication slot '{}'.", slotName);
                    } else {
                        // removeSlot swallows the failure, so without this the only trace of a
                        // slot that keeps pinning WAL would be an INFO line reading "false".
                        LOG.warn(
                                "Failed to remove replication slot '{}'. It will keep retaining WAL "
                                        + "until it is dropped, e.g. with pg_drop_replication_slot('{}').",
                                slotName,
                                slotName);
                    }
                }
            }
        }
    }

    private boolean isCommitOffset() {
        // Only if scan newly added table is enable, offset commit is controlled by isCommitOffset.
        return !sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset;
    }
}
