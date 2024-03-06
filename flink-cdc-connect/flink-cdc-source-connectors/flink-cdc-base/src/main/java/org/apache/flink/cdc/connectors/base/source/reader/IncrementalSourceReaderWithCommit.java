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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Record the LSN of checkpoint {@link StreamSplit}, which can be used to submit to the CDC source.
 */
@Experimental
public class IncrementalSourceReaderWithCommit extends IncrementalSourceReader {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalSourceReaderWithCommit.class);

    protected final TreeMap<Long, Offset> lastCheckpointOffsets;
    private long maxCompletedCheckpointId;

    public IncrementalSourceReaderWithCommit(
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
        this.lastCheckpointOffsets = new TreeMap<>();
        this.maxCompletedCheckpointId = 0;
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        final List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        stateSplits.stream()
                .filter(SourceSplitBase::isStreamSplit)
                .findAny()
                .map(SourceSplitBase::asStreamSplit)
                .ifPresent(
                        streamSplit -> {
                            lastCheckpointOffsets.put(
                                    checkpointId, streamSplit.getStartingOffset());
                            LOG.debug(
                                    "Starting offset of stream split is: {}, and checkpoint id is {}.",
                                    streamSplit.getStartingOffset(),
                                    checkpointId);
                        });

        return stateSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // checkpointId might be for a checkpoint that was triggered earlier. see
        // CheckpointListener#notifyCheckpointComplete(long).
        if (checkpointId > maxCompletedCheckpointId) {
            Offset offset = lastCheckpointOffsets.get(checkpointId);
            dialect.notifyCheckpointComplete(checkpointId, offset);
            lastCheckpointOffsets.headMap(checkpointId, true).clear();
            maxCompletedCheckpointId = checkpointId;
        }
    }
}
