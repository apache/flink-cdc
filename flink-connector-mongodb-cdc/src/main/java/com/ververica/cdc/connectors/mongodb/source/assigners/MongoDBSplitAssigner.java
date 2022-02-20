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

package com.ververica.cdc.connectors.mongodb.source.assigners;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.util.Preconditions;

import com.mongodb.client.ChangeStreamIterable;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.MongoDBSplitters;
import com.ververica.cdc.connectors.mongodb.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBChangeStreamConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isAssigningFinished;
import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isInitialAssigningFinished;
import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isNewlyAddedAssigningFinished;
import static com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus.isSuspended;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamIterable;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getResumeToken;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.currentBsonTimestamp;

/**
 * A {@link MongoDBSplitAssigner} that splits collections into small snapshot chunk splits and also
 * continue with a stream split.
 */
public class MongoDBSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSplitAssigner.class);

    public static final String STREAM_SPLIT_ID = "stream-split";

    private final MongoDBSourceConfig sourceConfig;
    private final MongoDBChangeStreamConfig changeStreamConfig;

    private final int currentParallelism;
    private final MongoDBSplitters splitters;

    private final LinkedList<CollectionId> remainingCollections;
    private final List<CollectionId> alreadyProcessedCollections;

    private final List<MongoDBSnapshotSplit> remainingSnapshotSplits;
    private final Map<String, MongoDBSnapshotSplit> assignedSnapshotSplits;
    private final List<String> finishedSnapshotSplits;

    @Nullable private MongoDBStreamSplit remainingStreamSplit;

    private boolean isStreamSplitAssigned;

    private AssignerStatus assignerStatus;

    @Nullable private Long checkpointIdToFinish;

    public MongoDBSplitAssigner(
            MongoDBSourceConfig sourceConfig,
            MongoDBChangeStreamConfig changeStreamConfig,
            int currentParallelism,
            List<CollectionId> remainingCollections) {
        this(
                sourceConfig,
                changeStreamConfig,
                currentParallelism,
                false,
                remainingCollections,
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                new ArrayList<>(),
                null,
                AssignerStatus.INITIAL_ASSIGNING);
    }

    public MongoDBSplitAssigner(
            MongoDBSourceConfig sourceConfig,
            MongoDBChangeStreamConfig changeStreamConfig,
            int currentParallelism,
            boolean isStreamSplitAssigned,
            List<CollectionId> remainingCollections,
            List<CollectionId> alreadyProcessedCollections,
            List<MongoDBSnapshotSplit> remainingSnapshotSplits,
            Map<String, MongoDBSnapshotSplit> assignedSnapshotSplits,
            List<String> finishedSnapshotSplits,
            @Nullable MongoDBStreamSplit remainingStreamSplit,
            AssignerStatus assignerStatus) {
        this.sourceConfig = sourceConfig;
        this.changeStreamConfig = changeStreamConfig;
        this.currentParallelism = currentParallelism;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.remainingCollections = new LinkedList<>(remainingCollections);
        this.alreadyProcessedCollections = alreadyProcessedCollections;
        this.remainingSnapshotSplits = remainingSnapshotSplits;
        this.assignedSnapshotSplits = assignedSnapshotSplits;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
        this.remainingStreamSplit = remainingStreamSplit;
        this.assignerStatus = assignerStatus;
        this.splitters = new MongoDBSplitters(sourceConfig, currentParallelism);
    }

    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    public void open() {
        if (!isStreamSplitAssigned) {
            ChangeStreamIterable<Document> changeStreamIterable =
                    getChangeStreamIterable(clientFor(sourceConfig), changeStreamConfig);

            BsonDocument startupResumeToken = getResumeToken(changeStreamIterable);

            MongoDBChangeStreamOffset changeStreamOffset =
                    new MongoDBChangeStreamOffset(startupResumeToken, currentBsonTimestamp());

            LOG.info("Cached startup change stream offset : {}", changeStreamOffset);

            remainingStreamSplit =
                    new MongoDBStreamSplit(STREAM_SPLIT_ID, changeStreamConfig, changeStreamOffset);
        }

        if (sourceConfig.isScanNewlyAddedCollectionEnabled() && !remainingCollections.isEmpty()) {
            LOG.info(
                    "Found newly added collections, start capture newly added collections process");
            if (isAssigningFinished(assignerStatus)) {
                // start the newly added tables process under stream reading phase
                LOG.info(
                        "Found newly added collections, start capture "
                                + "newly added collections process under stream reading phase");
                this.suspend();
            }
        }
    }

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    public Optional<MongoDBSplit> getNext() {
        if (isSuspended(assignerStatus)) {
            // do not assign split until the assigner received SuspendStreamReaderAckEvent
            LOG.info("Wait until the stream reader has been suspended.");
            return Optional.empty();
        }

        if (noMoreSnapshotSplits()) {
            // stream split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (isInitialAssigningFinished(assignerStatus)) {
                // we need to wait snapshot-assigner to be finished before assigning the stream
                // split.
                // Otherwise, records emitted from stream split might be out-of-order.
                return Optional.ofNullable(assignStreamSplit());
            } else if (isNewlyAddedAssigningFinished(assignerStatus)) {
                return Optional.ofNullable(assignStreamSplit());
            } else {
                // stream split is not ready by now
                return Optional.empty();
            }
        } else {
            if (!remainingSnapshotSplits.isEmpty()) {
                // return remaining splits firstly
                Iterator<MongoDBSnapshotSplit> iterator = remainingSnapshotSplits.iterator();
                MongoDBSnapshotSplit split = iterator.next();
                iterator.remove();
                assignedSnapshotSplits.put(split.splitId(), split);
                return Optional.of(split);
            } else {
                // it's turn for next collection
                CollectionId nextCollection = remainingCollections.pollFirst();
                if (nextCollection != null) {
                    // split the given table into chunks (snapshot splits)
                    Collection<MongoDBSnapshotSplit> splits = splitters.split(nextCollection);
                    remainingSnapshotSplits.addAll(splits);
                    alreadyProcessedCollections.add(nextCollection);
                    return getNext();
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    /** Assign stream split. */
    private MongoDBStreamSplit assignStreamSplit() {
        if (remainingStreamSplit != null) {
            MongoDBStreamSplit streamSplit =
                    MongoDBStreamSplit.toNormalStreamSplit(
                            remainingStreamSplit, changeStreamConfig);

            remainingStreamSplit = null;
            isStreamSplitAssigned = true;
            return streamSplit;
        }
        return null;
    }

    /** Callback to handle the finished snapshot splits. */
    public void onFinishedSnapshotSplits(List<String> finishedSnapshotSplits) {
        this.finishedSnapshotSplits.addAll(finishedSnapshotSplits);
        if (allSnapshotSplitsFinished() && AssignerStatus.isAssigning(assignerStatus)) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and stream split.
            if (currentParallelism == 1) {
                assignerStatus = assignerStatus.onFinish();
                LOG.info(
                        "Snapshot split assigner received all splits finished and the job parallelism is 1, snapshot split assigner is turn into finished status.");
            } else {
                LOG.info(
                        "Snapshot split assigner received all splits finished, waiting for a complete checkpoint to mark the assigner finished.");
            }
        }
    }

    /**
     * Callback to handle the finished stream split. Create a new stream split with suspended stream
     * split's resume token.
     */
    public void onFinishedStreamSplit(MongoDBChangeStreamOffset changeStreamOffset) {
        Preconditions.checkState(
                isStreamSplitAssigned, "Stream split cannot be finished when it was not assigned");
        remainingStreamSplit =
                new MongoDBStreamSplit(STREAM_SPLIT_ID, changeStreamConfig, changeStreamOffset);
        isStreamSplitAssigned = false;
    }

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    public void addSplitsBack(Collection<MongoDBSplit> splits) {
        for (MongoDBSplit split : splits) {
            if (split.isSnapshotSplit()) {
                remainingSnapshotSplits.add(split.asSnapshotSplit());
                // we should remove the add-backed splits from the assigned list,
                // because they are failed
                assignedSnapshotSplits.remove(split.splitId());
                finishedSnapshotSplits.remove(split.splitId());
            } else {
                remainingStreamSplit = split.asStreamSplit();
                isStreamSplitAssigned = false;
            }
        }
    }

    /**
     * Whether the split assigner is still waiting for callback of finished splits, i.e. {@link
     * #onFinishedSnapshotSplits(List)}.
     */
    public boolean isWaitingForFinishedSplits() {
        return !allSnapshotSplitsFinished();
    }

    /**
     * Creates a snapshot of the state of this split assigner, to be stored in a checkpoint.
     *
     * <p>The snapshot should contain the latest state of the assigner: It should assume that all
     * operations that happened before the snapshot have successfully completed. For example all
     * splits assigned to readers via {@link #getNext()} don't need to be included in the snapshot
     * anymore.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshotted. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     */
    public PendingSplitsState snapshotState(long checkpointId) {
        LOG.debug("snapshotState() is called with checkpointId {}", checkpointId);
        PendingSplitsState state =
                new PendingSplitsState(
                        isStreamSplitAssigned,
                        remainingCollections,
                        alreadyProcessedCollections,
                        remainingSnapshotSplits,
                        assignedSnapshotSplits,
                        finishedSnapshotSplits,
                        remainingStreamSplit,
                        assignerStatus);

        if (checkpointIdToFinish == null
                && !isAssigningFinished(assignerStatus)
                && allSnapshotSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.debug("notifyCheckpointComplete() is called with checkpointId {}", checkpointId);
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null
                && !isAssigningFinished(assignerStatus)
                && allSnapshotSplitsFinished()) {
            if (checkpointId >= checkpointIdToFinish) {
                assignerStatus = assignerStatus.onFinish();
            }
            LOG.info("Snapshot split assigner is turn into finished status.");
        }
    }

    /** Gets the split assigner status, see {@code AssignerStatus}. */
    public AssignerStatus status() {
        return assignerStatus;
    }

    /**
     * Suspends the assigner under {@link AssignerStatus#INITIAL_ASSIGNING_FINISHED} or {@link
     * AssignerStatus#NEWLY_ADDED_ASSIGNING_FINISHED}.
     */
    public void suspend() {
        Preconditions.checkState(
                isAssigningFinished(assignerStatus), "Invalid assigner status {}", assignerStatus);
        assignerStatus = assignerStatus.suspend();
    }

    /** Wakes up the assigner under {@link AssignerStatus#SUSPENDED}. */
    public void wakeup() {
        Preconditions.checkState(
                isSuspended(assignerStatus), "Invalid assigner status {}", assignerStatus);
        assignerStatus = assignerStatus.wakeup();
    }

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    public void close() {
        // do nothing
    }

    private boolean noMoreSnapshotSplits() {
        return remainingCollections.isEmpty() && remainingSnapshotSplits.isEmpty();
    }

    private boolean allSnapshotSplitsFinished() {
        return noMoreSnapshotSplits()
                && assignedSnapshotSplits.size() == finishedSnapshotSplits.size();
    }
}
