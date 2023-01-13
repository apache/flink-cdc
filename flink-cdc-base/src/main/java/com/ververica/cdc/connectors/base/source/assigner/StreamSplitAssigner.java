/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.base.source.assigner;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Assigner for stream split. */
public class StreamSplitAssigner implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(StreamSplitAssigner.class);
    private static final String STREAM_SPLIT_ID = "stream-split";

    private final SourceConfig sourceConfig;

    private boolean isStreamSplitAssigned;

    private final DataSourceDialect dialect;
    private final OffsetFactory offsetFactory;

    public StreamSplitAssigner(
            SourceConfig sourceConfig, DataSourceDialect dialect, OffsetFactory offsetFactory) {
        this(sourceConfig, false, dialect, offsetFactory);
    }

    public StreamSplitAssigner(
            SourceConfig sourceConfig,
            StreamPendingSplitsState checkpoint,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this(sourceConfig, checkpoint.isStreamSplitAssigned(), dialect, offsetFactory);
    }

    private StreamSplitAssigner(
            SourceConfig sourceConfig,
            boolean isStreamSplitAssigned,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this.sourceConfig = sourceConfig;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {}

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (isStreamSplitAssigned) {
            return Optional.empty();
        } else {
            isStreamSplitAssigned = true;
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
    public void close() {}

    // ------------------------------------------------------------------------------------------

    public StreamSplit createStreamSplit() {

        return new StreamSplit(
                STREAM_SPLIT_ID,
                dialect.displayCurrentOffset(sourceConfig),
                offsetFactory.createNoStoppingOffset(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }
}
