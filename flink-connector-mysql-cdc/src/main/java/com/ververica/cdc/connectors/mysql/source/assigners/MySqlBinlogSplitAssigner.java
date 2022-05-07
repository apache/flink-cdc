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

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.jdbc.JdbcConnection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.currentBinlogOffset;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.earliestBinlogOffset;

/** A {@link MySqlSplitAssigner} which only read binlog from current binlog position. */
public class MySqlBinlogSplitAssigner implements MySqlSplitAssigner {

    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;

    private BinlogOffsetReadingMode binlogOffsetReadingMode;

    public MySqlBinlogSplitAssigner(MySqlSourceConfig sourceConfig) {
        this(sourceConfig, false, BinlogOffsetReadingMode.LATEST_OFFSET);
    }

    public MySqlBinlogSplitAssigner(MySqlSourceConfig sourceConfig, BinlogOffsetReadingMode binlogOffsetReadingMode) {
        this(sourceConfig, false, binlogOffsetReadingMode);
    }

    public MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, BinlogPendingSplitsState checkpoint) {
        this(sourceConfig, checkpoint.isBinlogSplitAssigned(), BinlogOffsetReadingMode.LATEST_OFFSET);
    }

    private MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, boolean isBinlogSplitAssigned, BinlogOffsetReadingMode binlogOffsetReadingMode) {
        this.sourceConfig = sourceConfig;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.binlogOffsetReadingMode = binlogOffsetReadingMode;
    }

    @Override
    public void open() {}

    @Override
    public Optional<MySqlSplit> getNext() {
        if (isBinlogSplitAssigned) {
            return Optional.empty();
        } else {
            isBinlogSplitAssigned = true;
            if (binlogOffsetReadingMode == BinlogOffsetReadingMode.EARLIEST_OFFSET){
                return Optional.of(createBinlogSplitFromEarliest());
            }else{
                return Optional.of(createBinlogSplitFromLatest());
            }
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
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        // we don't store the split, but will re-create binlog split later
        isBinlogSplitAssigned = false;
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new BinlogPendingSplitsState(isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
    }

    @Override
    public void suspend() {}

    @Override
    public void wakeup() {}

    @Override
    public void close() {}

    // ------------------------------------------------------------------------------------------

    private MySqlBinlogSplit createBinlogSplitFromLatest() {
        try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
            return new MySqlBinlogSplit(
                    BINLOG_SPLIT_ID,
                    currentBinlogOffset(jdbc),
                    BinlogOffset.NO_STOPPING_OFFSET,
                    new ArrayList<>(),
                    new HashMap<>(),
                    0);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
    }

    private MySqlBinlogSplit createBinlogSplitFromEarliest() {
        try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
            return new MySqlBinlogSplit(
                    BINLOG_SPLIT_ID,
                    earliestBinlogOffset(jdbc),
                    BinlogOffset.NO_STOPPING_OFFSET,
                    new ArrayList<>(),
                    new HashMap<>(),
                    0);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
    }

    /**
     * Reading mode for binlog offset
     */
    public enum BinlogOffsetReadingMode {
        LATEST_OFFSET,
        EARLIEST_OFFSET
    }
}
