/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.common.annotation.Experimental;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import com.ververica.cdc.connectors.postgres.source.events.OffsetCommitAckEvent;
import com.ververica.cdc.connectors.postgres.source.events.OffsetCommitEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
        // After all snapshot splits are finished, update stream split's metadata and reset start
        // offset, which maybe smaller than before.
        // In case that new start-offset of stream split has been recycled, don't commit offset
        // during new table added phase.
        if (isCommitOffset()) {
            super.notifyCheckpointComplete(checkpointId);
        }
    }

    private boolean isCommitOffset() {
        // Only if scan newly added table is enable, offset commit is controlled by isCommitOffset.
        return !sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset;
    }
}
