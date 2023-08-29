/*
 * Copyright 2023 Ververica Inc.
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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.relational.TableId;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Tests for {@link MySqlHybridSplitAssigner}. */
public class MySqlHybridSplitAssignerTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    public void testAssignMySqlBinlogSplitAfterAllSnapshotSplitsFinished() {

        final String captureTable = "customers";
        MySqlSourceConfig configuration = getConfig(new String[] {captureTable});

        // Step 1. Mock MySqlHybridSplitAssigner Object
        TableId tableId = new TableId(null, customerDatabase.getDatabaseName(), captureTable);
        RowType splitKeyType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();

        List<TableId> alreadyProcessedTables = Lists.newArrayList(tableId);
        List<MySqlSchemalessSnapshotSplit> remainingSplits = new ArrayList<>();

        Map<String, MySqlSchemalessSnapshotSplit> assignedSplits = new HashMap<>();
        Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            String splitId = customerDatabase.getDatabaseName() + "." + captureTable + ":" + i;
            Object[] splitStart = i == 0 ? null : new Object[] {i * 2};
            Object[] splitEnd = new Object[] {i * 2 + 2};
            BinlogOffset highWatermark =
                    BinlogOffset.ofBinlogFilePosition("mysql-bin.00001", i + 1);
            MySqlSchemalessSnapshotSplit mySqlSchemalessSnapshotSplit =
                    new MySqlSchemalessSnapshotSplit(
                            tableId, splitId, splitKeyType, splitStart, splitEnd, highWatermark);
            assignedSplits.put(splitId, mySqlSchemalessSnapshotSplit);
            splitFinishedOffsets.put(splitId, highWatermark);
        }

        SnapshotPendingSplitsState snapshotPendingSplitsState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        new HashMap<>(),
                        splitFinishedOffsets,
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        HybridPendingSplitsState checkpoint =
                new HybridPendingSplitsState(snapshotPendingSplitsState, false);
        final MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(configuration, DEFAULT_PARALLELISM, checkpoint);

        // step 2. Get the MySqlBinlogSplit after all snapshot splits finished
        Optional<MySqlSplit> binlogSplit = assigner.getNext();
        MySqlBinlogSplit mySqlBinlogSplit = binlogSplit.get().asBinlogSplit();

        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        final List<MySqlSchemalessSnapshotSplit> mySqlSchemalessSnapshotSplits =
                assignedSplits.values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        for (MySqlSchemalessSnapshotSplit split : mySqlSchemalessSnapshotSplits) {
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            split.getHighWatermark()));
        }

        MySqlBinlogSplit expected =
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.00001", 1),
                        BinlogOffset.ofNonStopping(),
                        finishedSnapshotSplitInfos,
                        new HashMap<>(),
                        finishedSnapshotSplitInfos.size());
        assertEquals(expected, mySqlBinlogSplit);
        assigner.close();
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }
}
