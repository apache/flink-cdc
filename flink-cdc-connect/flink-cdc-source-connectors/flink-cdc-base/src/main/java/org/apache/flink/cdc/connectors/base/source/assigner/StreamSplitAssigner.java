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
import org.apache.flink.cdc.common.utils.Preconditions;
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

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Assigner for stream split. */
public class StreamSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final String STREAM_SPLIT_ID = "stream-split";

    private final C sourceConfig;

    private final DataSourceDialect<C> dialect;
    private final OffsetFactory offsetFactory;

    private final SplitEnumeratorContext<? extends SourceSplit> enumeratorContext;
    private SourceEnumeratorMetrics enumeratorMetrics;
    private final int numberOfStreamSplits;

    public StreamSplitAssigner(
            C sourceConfig,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this(sourceConfig, false, dialect, offsetFactory, enumeratorContext);
    }

    public StreamSplitAssigner(
            C sourceConfig,
            StreamPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
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
            C sourceConfig,
            boolean isStreamSplitAllAssigned,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this.sourceConfig = sourceConfig;
        this.isStreamSplitAllAssigned = isStreamSplitAllAssigned;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
        this.enumeratorContext = enumeratorContext;
        this.numberOfStreamSplits = dialect.getNumberOfStreamSplits(sourceConfig);
    }

    @Override
    public void open() {
        this.enumeratorMetrics = new SourceEnumeratorMetrics(enumeratorContext.metricGroup());
        if (isStreamSplitAllAssigned) {
            enumeratorMetrics.enterStreamReading();
        } else {
            enumeratorMetrics.exitStreamReading();
        }
        pendingStreamSplits = null;
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (isStreamSplitAllAssigned) {
            return Optional.empty();
        } else {
            enumeratorMetrics.enterStreamReading();
            return getNextStreamSplit();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return false;
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return Collections.emptyList();
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        // we don't store the split, but will re-create stream split later
        isStreamSplitAllAssigned = false;
        pendingStreamSplits = null;
        enumeratorMetrics.exitStreamReading();
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new StreamPendingSplitsState(isStreamSplitAllAssigned);
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
        return isStreamSplitAllAssigned;
    }

    @Override
    public void close() throws IOException {
        dialect.close();
    }

    // ------------------------------------------------------------------------------------------
    protected boolean isStreamSplitAllAssigned;
    protected List<SourceSplitBase> pendingStreamSplits = null;

    private Optional<SourceSplitBase> getNextStreamSplit() {
        if (pendingStreamSplits == null) {
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
                            offsetFactory.createTimestampOffset(
                                    startupOptions.startupTimestampMillis);
                    break;
                case SPECIFIC_OFFSETS:
                    startingOffset =
                            offsetFactory.newOffset(
                                    startupOptions.specificOffsetFile,
                                    startupOptions.specificOffsetPos.longValue());
                    break;
                default:
                    throw new IllegalStateException(
                            "Unsupported startup mode " + startupOptions.startupMode);
            }

            pendingStreamSplits =
                    new ArrayList<>(
                            createStreamSplits(
                                    sourceConfig,
                                    startingOffset,
                                    offsetFactory.createNoStoppingOffset(),
                                    new ArrayList<>(),
                                    new HashMap<>(),
                                    0,
                                    false,
                                    true));
            Preconditions.checkArgument(
                    pendingStreamSplits.size() == numberOfStreamSplits,
                    "Inconsistent number of stream splits. Reported %s, but was %s",
                    numberOfStreamSplits,
                    pendingStreamSplits.size());
            Preconditions.checkArgument(
                    pendingStreamSplits.size() <= enumeratorContext.currentParallelism(),
                    "%s stream splits generated, which is greater than current parallelism %s. Some splits might never be assigned.",
                    pendingStreamSplits.size(),
                    enumeratorContext.currentParallelism());
        }

        if (pendingStreamSplits.isEmpty()) {
            return Optional.empty();
        } else {
            SourceSplitBase nextSplit = pendingStreamSplits.remove(0);
            if (pendingStreamSplits.isEmpty()) {
                isStreamSplitAllAssigned = true;
            }
            return Optional.of(nextSplit);
        }
    }

    protected List<SourceSplitBase> createStreamSplits(
            C sourceConfig,
            Offset minOffset,
            Offset stoppingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted) {
        return Collections.singletonList(
                new StreamSplit(
                        STREAM_SPLIT_ID,
                        minOffset,
                        stoppingOffset,
                        finishedSnapshotSplitInfos,
                        tableSchemas,
                        totalFinishedSplitSize,
                        isSuspended,
                        isSnapshotCompleted));
    }
}
