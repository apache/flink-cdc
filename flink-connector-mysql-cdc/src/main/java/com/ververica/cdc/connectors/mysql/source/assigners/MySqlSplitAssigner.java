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

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.api.common.state.CheckpointListener;

import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@code MySqlSplitAssigner} is responsible for deciding what split should be processed. It
 * determines split processing order.
 */
public interface MySqlSplitAssigner {

    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    void open();

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    Optional<MySqlSplit> getNext();

    /**
     * Whether the split assigner is still waiting for callback of finished splits, i.e. {@link
     * #onFinishedSplits(Map)}.
     */
    boolean waitingForFinishedSplits();

    /**
     * Gets the finished splits' information. This is useful metadata to generate a binlog split
     * that considering finished snapshot splits.
     */
    List<FinishedSnapshotSplitInfo> getFinishedSplitInfos();

    /**
     * Callback to handle the finished splits with finished binlog offset. This is useful for
     * determine when to generate binlog split and what binlog split to generate.
     */
    void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplits(Collection<MySqlSplit> splits);

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
    PendingSplitsState snapshotState(long checkpointId);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    void notifyCheckpointComplete(long checkpointId);

    /** Gets the split assigner status, see {@code AssignerStatus}. */
    AssignerStatus getAssignerStatus();

    /** Starts assign newly added tables. */
    void startAssignNewlyAddedTables();

    /**
     * Callback to handle the binlog split has been updated in the newly added tables process. This
     * is useful to check the newly added tables has been finished or not.
     */
    void onBinlogSplitUpdated();

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    void close();
}
