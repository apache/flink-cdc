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

import com.ververica.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import io.debezium.relational.TableId;

import java.util.List;

/**
 * The {@code ChunkSplitter}'s task is to split table into a set of chunks or called splits (i.e.
 * {@link MySqlSnapshotSplit}).
 */
public interface ChunkSplitter {
    /**
     * Called to open the chunk splitter to acquire any resources, like threads or jdbc connections.
     */
    void open();

    /**
     * Called to split chunks for a table, the assigner could invoke this method multiple times to
     * receive all the splits.
     */
    List<MySqlSnapshotSplit> splitChunks(TableId tableId) throws Exception;

    /** Get whether the splitter has more chunks for current table. */
    boolean hasNextChunk();

    /**
     * Creates a snapshot of the state of this chunk splitter, to be stored in a checkpoint.
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
    ChunkSplitterState snapshotState(long checkpointId);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    void notifyCheckpointComplete(long checkpointId);

    /**
     * Called to close the splitter, in case it holds on to any resources, like threads or jdbc
     * connections.
     */
    void close() throws Exception;
}
