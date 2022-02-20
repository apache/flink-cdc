/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplitState;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/** The source reader for MongoDB source splits. */
public class MongoDBSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord,
                T,
                SourceSplitBase<CollectionId, CollectionSchema>,
                SourceSplitState<CollectionId, CollectionSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceReader.class);

    private final MongoDBSourceConfig sourceConfig;
    private final Map<String, SnapshotSplit<CollectionId, CollectionSchema>> finishedUnackedSplits;
    private final int subtaskId;
    private final MongoDBSourceReaderContext sourceReaderContext;

    public MongoDBSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MongoDBSourceSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, SourceSplitState<CollectionId, CollectionSchema>>
                    recordEmitter,
            Configuration config,
            MongoDBSourceReaderContext context,
            MongoDBSourceConfig sourceConfig) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.sourceReaderContext = context;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(
            Map<String, SourceSplitState<CollectionId, CollectionSchema>> finishedSplitIds) {
        for (SourceSplitState<CollectionId, CollectionSchema> splitState :
                finishedSplitIds.values()) {
            SourceSplitBase<CollectionId, CollectionSchema> sourceSplit =
                    splitState.toSourceSplit();
            checkState(
                    sourceSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is stream split %s",
                            sourceSplit));
            finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<SourceSplitBase<CollectionId, CollectionSchema>> splits) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase<CollectionId, CollectionSchema>> unfinishedSplits = new ArrayList<>();
        for (SourceSplitBase<CollectionId, CollectionSchema> split : splits) {
            LOG.info("Add Split: " + split);
            if (split.isSnapshotSplit()) {
                SnapshotSplit<CollectionId, CollectionSchema> snapshotSplit =
                        split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                unfinishedSplits.add(split.asStreamSplit());
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including stream split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected SourceSplitState<CollectionId, CollectionSchema> initializedState(
            SourceSplitBase<CollectionId, CollectionSchema> split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState<>(split.asSnapshotSplit());
        } else {
            return new StreamSplitState<>(split.asStreamSplit());
        }
    }

    @Override
    protected SourceSplitBase<CollectionId, CollectionSchema> toSplitType(
            String splitId, SourceSplitState<CollectionId, CollectionSchema> splitState) {
        return splitState.toSourceSplit();
    }

    @Override
    public List<SourceSplitBase<CollectionId, CollectionSchema>> snapshotState(long checkpointId) {
        List<SourceSplitBase<CollectionId, CollectionSchema>> stateSplits =
                super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        return stateSplits;
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, Offset> finishedOffsets = new HashMap<>();
            for (SnapshotSplit<CollectionId, CollectionSchema> split :
                    finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }
}
