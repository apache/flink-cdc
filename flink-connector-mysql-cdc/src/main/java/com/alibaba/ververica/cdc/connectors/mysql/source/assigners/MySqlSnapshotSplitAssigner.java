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

package com.alibaba.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.ververica.cdc.connectors.mysql.schema.MySqlSchema;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.closeMySqlConnection;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createTableFilters;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openMySqlConnection;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SNAPSHOT_CHUNK_SIZE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.listTables;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size.
 *
 * @see MySqlSourceOptions#SCAN_SNAPSHOT_CHUNK_SIZE
 */
public class MySqlSnapshotSplitAssigner implements MySqlSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitAssigner.class);

    private final List<TableId> alreadyProcessedTables;
    private final List<MySqlSnapshotSplit> remainingSplits;
    private final Map<String, MySqlSnapshotSplit> assignedSplits;
    private final Map<String, BinlogOffset> splitFinishedOffsets;
    private boolean assignerFinished;

    private final Configuration configuration;
    private final LinkedList<TableId> remainingTables;
    private final RelationalTableFilters tableFilters;
    private final int chunkSize;

    private MySqlConnection jdbc;
    private ChunkSplitter chunkSplitter;

    @Nullable private Long checkpointIdToFinish;

    public MySqlSnapshotSplitAssigner(Configuration configuration) {
        this(
                configuration,
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                new HashMap<>(),
                false);
    }

    public MySqlSnapshotSplitAssigner(
            Configuration configuration, SnapshotPendingSplitsState checkpoint) {
        this(
                configuration,
                checkpoint.getAlreadyProcessedTables(),
                checkpoint.getRemainingSplits(),
                checkpoint.getAssignedSplits(),
                checkpoint.getSplitFinishedOffsets(),
                checkpoint.isAssignerFinished());
    }

    private MySqlSnapshotSplitAssigner(
            Configuration configuration,
            List<TableId> alreadyProcessedTables,
            List<MySqlSnapshotSplit> remainingSplits,
            Map<String, MySqlSnapshotSplit> assignedSplits,
            Map<String, BinlogOffset> splitFinishedOffsets,
            boolean assignerFinished) {
        this.configuration = configuration;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerFinished = assignerFinished;
        this.remainingTables = new LinkedList<>();
        this.tableFilters = createTableFilters(configuration);
        this.chunkSize = configuration.get(SCAN_SNAPSHOT_CHUNK_SIZE);
        // TODO: the check should happen in factory
        checkState(
                chunkSize > 1,
                String.format(
                        "The value of option '%s' must larger than 1, but is %d",
                        SCAN_SNAPSHOT_CHUNK_SIZE.key(), chunkSize));
    }

    @Override
    public void open() {
        // discover captured tables
        jdbc = openMySqlConnection(configuration);
        chunkSplitter = createChunkSplitter(configuration, jdbc, chunkSize);
        if (!assignerFinished) {
            remainingTables.addAll(discoverCapturedTables());
        }
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<MySqlSnapshotSplit> iterator = remainingSplits.iterator();
            MySqlSnapshotSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for new table
            TableId nextTable = remainingTables.pollFirst();
            if (nextTable != null) {
                // split the given table into chunks (snapshot splits)
                Collection<MySqlSnapshotSplit> splits = chunkSplitter.enumerateSplits(nextTable);
                try {
                    remainingSplits.addAll(splits);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                alreadyProcessedTables.add(nextTable);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !allSplitsFinished();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSplitsFinished()) {
            LOG.info(
                    "Snapshot split assigner received all splits finished, waiting for a complete checkpoint to mark the assigner finished.");
        }
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        for (MySqlSplit split : splits) {
            remainingSplits.add(split.asSnapshotSplit());
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSplits.remove(split.splitId());
            splitFinishedOffsets.remove(split.splitId());
        }
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        SnapshotPendingSplitsState state =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        splitFinishedOffsets,
                        assignerFinished);
        // we need a complete checkpoint before mark this assigner to be finished, to wait for all
        // records of snapshot splits are completely processed
        if (checkpointIdToFinish == null && !assignerFinished && allSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null && !assignerFinished && allSplitsFinished()) {
            assignerFinished = checkpointId >= checkpointIdToFinish;
            LOG.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public void close() {
        if (jdbc != null) {
            closeMySqlConnection(jdbc);
        }
    }

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSplits() {
        return remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /**
     * Returns whether the snapshot split assigner is finished, which indicates there is no more
     * splits and all records of splits have been completely processed in the pipeline.
     */
    public boolean isFinished() {
        return assignerFinished;
    }

    public Map<String, MySqlSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSplitsFinished() {
        return noMoreSplits() && assignedSplits.size() == splitFinishedOffsets.size();
    }

    private List<TableId> discoverCapturedTables() {
        final List<TableId> capturedTableIds;
        try {
            capturedTableIds = listTables(jdbc, tableFilters);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            configuration.get(MySqlSourceOptions.DATABASE_NAME),
                            configuration.get(MySqlSourceOptions.TABLE_NAME)));
        }
        return capturedTableIds;
    }

    private static ChunkSplitter createChunkSplitter(
            Configuration configuration, MySqlConnection jdbc, int chunkSize) {
        MySqlSchema mySqlSchema = new MySqlSchema(toDebeziumConfig(configuration), jdbc);
        return new ChunkSplitter(jdbc, mySqlSchema, chunkSize);
    }
}
