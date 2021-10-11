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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
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
public class MySqlHybridSplitAssignerTest extends MySqlParallelSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    public void testAssignMySqlBinlogSplitAfterAllSnapshotSplitsFinished() {

        final String captureTable = "customers";
        MySqlParallelSourceConfig configuration = getConfig(new String[] {captureTable});

        // Step 1. Mock MySqlHybridSplitAssigner Object
        TableId tableId = new TableId(null, customerDatabase.getDatabaseName(), captureTable);
        RowType splitKeyType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();

        List<TableId> alreadyProcessedTables = Lists.newArrayList(tableId);
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();

        Map<String, MySqlSnapshotSplit> assignedSplits = new HashMap<>();
        Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            String splitId = customerDatabase.getDatabaseName() + "." + captureTable + ":" + i;
            Object[] splitStart = i == 0 ? null : new Object[] {i * 2};
            Object[] splitEnd = new Object[] {i * 2 + 2};
            BinlogOffset highWatermark = new BinlogOffset("mysql-bin.00001", i + 1);
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            MySqlSnapshotSplit sqlSnapshotSplit =
                    new MySqlSnapshotSplit(
                            tableId,
                            splitId,
                            splitKeyType,
                            splitStart,
                            splitEnd,
                            highWatermark,
                            tableSchemas);
            assignedSplits.put(splitId, sqlSnapshotSplit);
            splitFinishedOffsets.put(splitId, highWatermark);
        }

        SnapshotPendingSplitsState snapshotPendingSplitsState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        splitFinishedOffsets,
                        true);
        HybridPendingSplitsState checkpoint =
                new HybridPendingSplitsState(snapshotPendingSplitsState, false);
        final MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(configuration, DEFAULT_PARALLELISM, checkpoint);

        // step 2. Get the MySqlBinlogSplit after all snapshot splits finished
        Optional<MySqlSplit> binlogSplit = assigner.getNext();
        MySqlBinlogSplit mySqlBinlogSplit = binlogSplit.get().asBinlogSplit();

        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        final List<MySqlSnapshotSplit> assignedSnapshotSplit =
                assignedSplits.values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        for (MySqlSnapshotSplit split : assignedSnapshotSplit) {
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
                        splitKeyType,
                        new BinlogOffset("mysql-bin.00001", 1),
                        BinlogOffset.NO_STOPPING_OFFSET,
                        finishedSnapshotSplitInfos,
                        new HashMap<>());
        assertEquals(expected, mySqlBinlogSplit);
    }

    private MySqlParallelSourceConfig getConfig(String[] captureTables) {
        List<String> captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .collect(Collectors.toList());

        return new MySqlParallelSourceConfig.Builder()
                .startupMode("initial")
                .capturedDatabases(customerDatabase.getDatabaseName())
                .capturedTables(String.join(",", captureTableIds))
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .build();
    }
}
