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

package org.apache.flink.cdc.connectors.base.source.assigner;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Assigner for stream split. */
public class StreamSplitAssigner implements SplitAssigner {

    private static final String STREAM_SPLIT_ID = "stream-split";

    private final SourceConfig sourceConfig;

    private boolean isStreamSplitAssigned;

    private final DataSourceDialect dialect;
    private final OffsetFactory offsetFactory;

    private final SplitEnumeratorContext<? extends SourceSplit> enumeratorContext;
    private SourceEnumeratorMetrics enumeratorMetrics;

    public StreamSplitAssigner(
            SourceConfig sourceConfig,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this(sourceConfig, false, dialect, offsetFactory, enumeratorContext);
    }

    public StreamSplitAssigner(
            SourceConfig sourceConfig,
            StreamPendingSplitsState checkpoint,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this(
                sourceConfig,
                checkpoint.isStreamSplitAssigned(),
                dialect,
                offsetFactory,
                enumeratorContext);
    }

    private StreamSplitAssigner(
            SourceConfig sourceConfig,
            boolean isStreamSplitAssigned,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this.sourceConfig = sourceConfig;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
        this.enumeratorContext = enumeratorContext;
    }

    @Override
    public void open() {
        this.enumeratorMetrics = new SourceEnumeratorMetrics(enumeratorContext.metricGroup());
        if (isStreamSplitAssigned) {
            enumeratorMetrics.enterStreamReading();
        } else {
            enumeratorMetrics.exitStreamReading();
        }
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (isStreamSplitAssigned) {
            return Optional.empty();
        } else {
            isStreamSplitAssigned = true;
            enumeratorMetrics.enterStreamReading();
            return Optional.of(createStreamSplit());
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return false;
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        // we don't store the split, but will re-create stream split later
        isStreamSplitAssigned = false;
        enumeratorMetrics.exitStreamReading();
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new StreamPendingSplitsState(isStreamSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
    }

    @Override
    public void startAssignNewlyAddedTables() {}

    @Override
    public void onStreamSplitUpdated() {}

    @Override
    public boolean noMoreSplits() {
        return isStreamSplitAssigned;
    }

    @Override
    public void close() throws IOException {
        dialect.close();
    }

    // ------------------------------------------------------------------------------------------

    public StreamSplit createStreamSplit() {
        StartupOptions startupOptions = sourceConfig.getStartupOptions();

        Offset startingOffset;
        switch (startupOptions.startupMode) {
            case LATEST_OFFSET:
                startingOffset = dialect.displayCurrentOffset(sourceConfig);
                break;
            case EARLIEST_OFFSET:
                startingOffset = offsetFactory.createInitialOffset();
                break;
            case TIMESTAMP:
                startingOffset =
                        offsetFactory.createTimestampOffset(startupOptions.startupTimestampMillis);
                break;
            case SPECIFIC_OFFSETS:
                startingOffset =
                        offsetFactory.newOffset(
                                startupOptions.specificOffsetFile,
                                startupOptions.specificOffsetPos.longValue());
                break;
            case COMMITTED_OFFSETS:
                startingOffset = dialect.displayCommittedOffset(sourceConfig);
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported startup mode " + startupOptions.startupMode);
        }

        return new StreamSplit(
                STREAM_SPLIT_ID,
                startingOffset,
                offsetFactory.createNoStoppingOffset(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }
}
