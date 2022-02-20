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

import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mongodb.source.events.SuspendStreamReaderAckEvent;
import com.ververica.cdc.connectors.mongodb.source.events.SuspendStreamReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.events.WakeupSnapshotReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.events.WakeupStreamReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffsetSerializer;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplitState;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplitState;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplitState;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit.toSuspendedStreamSplit;

/** The source reader for MongoDB source splits. */
public class MongoDBSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord, T, MongoDBSplit, MongoDBSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceReader.class);

    private final MongoDBSourceConfig sourceConfig;
    private final Map<String, MongoDBSnapshotSplit> finishedUnackedSplits;
    private final int subtaskId;
    private final MongoDBSourceReaderContext sourceReaderContext;
    private MongoDBStreamSplit suspendedStreamSplit;

    public MongoDBSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MongoDBSplitReader<MongoDBSplit>> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MongoDBSplitState> recordEmitter,
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
        this.suspendedStreamSplit = null;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, MongoDBSplitState> finishedSplitIds) {
        for (MongoDBSplitState mongoDBSplitState : finishedSplitIds.values()) {
            MongoDBSplit mongoDBSplit = mongoDBSplitState.toMongoDBSplit();
            if (mongoDBSplit.isStreamSplit()) {
                LOG.info(
                        "stream split reader suspended due to newly added collections",
                        mongoDBSplitState.asStreamSplitState());

                sourceReaderContext.resetStopStreamSplitReader();
                suspendedStreamSplit = toSuspendedStreamSplit(mongoDBSplit.asStreamSplit());

                SuspendStreamReaderAckEvent suspendStreamReaderAckEvent =
                        new SuspendStreamReaderAckEvent(
                                MongoDBChangeStreamOffsetSerializer.serialize(
                                        suspendedStreamSplit.getChangeStreamOffset()));
                context.sendSourceEventToCoordinator(suspendStreamReaderAckEvent);
            } else {
                finishedUnackedSplits.put(mongoDBSplit.splitId(), mongoDBSplit.asSnapshotSplit());
            }
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<MongoDBSplit> splits) {
        // restore for finishedUnackedSplits
        List<MongoDBSplit> unfinishedSplits = new ArrayList<>();
        for (MongoDBSplit split : splits) {
            LOG.info("Add Split: " + split);
            if (split.isSnapshotSplit()) {
                MongoDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                MongoDBStreamSplit streamSplit = split.asStreamSplit();
                unfinishedSplits.add(streamSplit);
                suspendedStreamSplit = null;
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including stream split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
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
        } else if (sourceEvent instanceof SuspendStreamReaderEvent) {
            sourceReaderContext.setStopStreamSplitReader();
        } else if (sourceEvent instanceof WakeupSnapshotReaderEvent) {
            context.sendSplitRequest();
        } else if (sourceEvent instanceof WakeupStreamReaderEvent) {
            context.sendSplitRequest();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected MongoDBSplitState initializedState(MongoDBSplit split) {
        if (split.isSnapshotSplit()) {
            return new MongoDBSnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new MongoDBStreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    public List<MongoDBSplit> snapshotState(long checkpointId) {
        List<MongoDBSplit> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<MongoDBSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that didn't receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add suspended StreamSplit
        if (suspendedStreamSplit != null) {
            unfinishedSplits.add(suspendedStreamSplit);
        }
        return unfinishedSplits;
    }

    @Override
    protected MongoDBSplit toSplitType(String s, MongoDBSplitState mongoDBSplitState) {
        return mongoDBSplitState.toMongoDBSplit();
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final List<String> finishedSplits = new ArrayList<>();
            for (MongoDBSnapshotSplit split : finishedUnackedSplits.values()) {
                finishedSplits.add(split.splitId());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedSplits);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug("The subtask {} reports offsets of finished snapshot splits.", subtaskId);
        }
    }
}
