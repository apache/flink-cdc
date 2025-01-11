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

package org.apache.flink.cdc.connectors.base.source.assigner.splitter;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;

import io.debezium.relational.TableId;

import java.util.Collection;

/** The splitter used to split collection into a set of chunks. */
@Experimental
public interface ChunkSplitter {

    /**
     * Called to open the chunk splitter to acquire any resources, like threads or jdbc connections.
     */
    void open();

    /** Generates all snapshot splits (chunks) for the give data collection. */
    Collection<SnapshotSplit> generateSplits(TableId tableId) throws Exception;

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

    TableId getCurrentSplittingTableId();

    /**
     * Called to open the chunk splitter to release any resources, like threads or jdbc connections.
     */
    void close() throws Exception;
}
