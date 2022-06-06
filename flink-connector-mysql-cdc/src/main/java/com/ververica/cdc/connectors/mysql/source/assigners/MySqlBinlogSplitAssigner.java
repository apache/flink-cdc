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

import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.api.ValidationException;
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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.*;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_MODE;

/**
 * A {@link MySqlSplitAssigner} which only read binlog from current binlog position.
 */
public class MySqlBinlogSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitAssigner.class);

    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;
    private final StartupOptions startupOptions;

    public MySqlBinlogSplitAssigner(MySqlSourceConfig sourceConfig) {
        this(sourceConfig, false);
    }

    public MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, BinlogPendingSplitsState checkpoint) {
        this(sourceConfig, checkpoint.isBinlogSplitAssigned());
    }

    private MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig,
            boolean isBinlogSplitAssigned) {
        this.sourceConfig = sourceConfig;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.startupOptions = this.sourceConfig.getStartupOptions();
    }

    @Override
    public void open() {
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (isBinlogSplitAssigned) {
            return Optional.empty();
        } else {
            isBinlogSplitAssigned = true;
            StartupMode startupMode = this.sourceConfig.getStartupOptions().startupMode;
            switch (startupMode){
                case EARLIEST_OFFSET:
                    return Optional.of(createBinlogSplitFromEarliest());
                case LATEST_OFFSET:
                    return Optional.of(createBinlogSplitFromLatest());
                case TIMESTAMP:
                    return Optional.of(createBinlogSplitFromTimestamp());
                default:
                    throw new ValidationException(
                            String.format(
                                    "Invalid value for MySqlBinlogSplitAssigner '%s'. Supported values are [%s, %s, %s], but was: %s",
                                    SCAN_STARTUP_MODE.key(),
                                    StartupMode.EARLIEST_OFFSET,
                                    StartupMode.LATEST_OFFSET,
                                    StartupMode.TIMESTAMP,
                                    startupMode.name()));
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

    private MySqlBinlogSplit createBinlogSplitFromTimestamp() {
        BinlogOffset binlogOffset = createBinlogOffsetFromTimestamp();
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                binlogOffset,
                BinlogOffset.NO_STOPPING_OFFSET,
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }

    private BinlogOffset createBinlogOffsetFromTimestamp() {
        List<String> databaseList = sourceConfig.getDatabaseList();
        Preconditions.checkArgument(databaseList.size() == 1,
                "startup by timestamp only support 1 mysql database");

        Long startupTimestampMillis = this.startupOptions.startupTimestampMillis;
        Preconditions.checkArgument(startupTimestampMillis < System.currentTimeMillis(),
                "startup timestamp should be small than now .");

        final long beginTimestampMills = System.currentTimeMillis();
        LOG.info("Begin to find binlog offset by timestamp {}", startupTimestampMillis);
        try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
            BinlogOffset maxBinlogOffset = DebeziumUtils.currentBinlogOffset(jdbc);
            String binlogFile = maxBinlogOffset.getFilename();
            BinlogOffset seekBinlogOffset = null;
            boolean shouldBreak = false;
            while (!shouldBreak) {
                BinlogOffset binlogOffset = DebeziumUtils
                        .seekBinlogOffsetByTimestamp(binlogFile, maxBinlogOffset, sourceConfig, startupTimestampMillis);

                // Can not find the binlog offset if the binlog offset's timestamp is 0 .
                // see SeekBinlogTimestampListener to find more detail
                if (binlogOffset == null || binlogOffset.getTimestamp() == 0) {
                    maxBinlogOffset = binlogOffset;
                } else {
                    LOG.info("Binlog offset found it, binlogOffset = {}, spendMills = {}", binlogOffset,
                            System.currentTimeMillis() - beginTimestampMills);
                    seekBinlogOffset = binlogOffset;
                    shouldBreak = true;
                }

                // find next binlog file
                int binlogSeqNum = Integer.parseInt(binlogFile.substring(binlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    LOG.warn("Can not find binlog file by timestamp = {}", startupTimestampMillis);
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = binlogFile.substring(0, binlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    binlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }

            Preconditions.checkArgument(seekBinlogOffset != null,
                    "Can not find binlog position " +
                            "by timestamp " + startupTimestampMillis);
            return seekBinlogOffset;
        } catch (Exception e) {
            throw new FlinkRuntimeException("Can not find binlog position " +
                    "by timestamp " + startupTimestampMillis, e);
        }
    }

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

}