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

package com.ververica.cdc.connectors.mongodb.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mongodb.source.assigners.MongoDBSplitAssigner;
import com.ververica.cdc.connectors.mongodb.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mongodb.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mongodb.source.events.SuspendStreamReaderAckEvent;
import com.ververica.cdc.connectors.mongodb.source.events.SuspendStreamReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.events.WakeupSnapshotReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.events.WakeupStreamReaderEvent;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffsetSerializer;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isAssigningFinished;
import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isSuspended;

/**
 * A MongoDB CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class MongoDBSourceEnumerator implements SplitEnumerator<MongoDBSplit, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private final SplitEnumeratorContext<MongoDBSplit> context;
    private final MongoDBSourceConfig sourceConfig;
    private final MongoDBSplitAssigner splitAssigner;

    // using TreeSet to prefer assigning stream split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;
    private boolean streamReaderIsSuspended = false;

    public MongoDBSourceEnumerator(
            SplitEnumeratorContext<MongoDBSplit> context,
            MongoDBSourceConfig sourceConfig,
            MongoDBSplitAssigner splitAssigner) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        splitAssigner.open();
        suspendStreamReaderIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<MongoDBSplit> splits, int subtaskId) {
        LOG.debug("MongoDB Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator receives finished split offsets {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            List<String> finishedSplits = reportEvent.getFinishedSplits();

            splitAssigner.onFinishedSnapshotSplits(finishedSplits);

            wakeupStreamReaderIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedSplits));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof SuspendStreamReaderAckEvent) {
            LOG.info(
                    "The enumerator receives event that the stream split reader has been suspended from subtask {}. ",
                    subtaskId);

            SuspendStreamReaderAckEvent suspendStreamReaderAckEvent =
                    (SuspendStreamReaderAckEvent) sourceEvent;

            MongoDBChangeStreamOffset changeStreamOffset =
                    MongoDBChangeStreamOffsetSerializer.deserialize(
                            suspendStreamReaderAckEvent.getOffsetSerialized());

            splitAssigner.onFinishedStreamSplit(changeStreamOffset);

            splitAssigner.wakeup();

            wakeupSnapshotReader();
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // stream split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<MongoDBSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final MongoDBSplit mongoDBSplit = split.get();
                context.assignSplit(mongoDBSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", mongoDBSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.isWaitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        suspendStreamReaderIfNeed();
        wakeupStreamReaderIfNeed();
    }

    private void suspendStreamReaderIfNeed() {
        if (isSuspended(splitAssigner.status())) {
            for (int subtaskId : getRegisteredReader()) {
                context.sendEventToSourceReader(subtaskId, new SuspendStreamReaderEvent());
            }
            streamReaderIsSuspended = true;
        }
    }

    private void wakeupStreamReaderIfNeed() {
        if (isAssigningFinished(splitAssigner.status()) && streamReaderIsSuspended) {
            for (int subtaskId : getRegisteredReader()) {
                context.sendEventToSourceReader(subtaskId, new WakeupStreamReaderEvent());
            }
            streamReaderIsSuspended = false;
        }
    }

    private void wakeupSnapshotReader() {
        for (int readerId : this.getRegisteredReader()) {
            context.sendEventToSourceReader(readerId, new WakeupSnapshotReaderEvent());
        }
    }
}
