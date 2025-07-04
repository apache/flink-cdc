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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.MockMySqlSplitEnumeratorEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MySqlHybridSplitAssigner}. */
class MySqlHybridSplitAssignerTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeAll
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    void testAssignMySqlBinlogSplitAfterAllSnapshotSplitsFinished() {

        final String captureTable = "customers";
        MySqlSourceConfig configuration =
                getConfig(new String[] {captureTable}, StartupOptions.initial());

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
        MockMySqlSplitEnumeratorEnumeratorContext enumeratorContext =
                getMySqlSplitEnumeratorContext();
        final MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(
                        configuration, DEFAULT_PARALLELISM, checkpoint, enumeratorContext);

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
        Assertions.assertThat(mySqlBinlogSplit).isEqualTo(expected);
        assigner.close();
    }

    @Test
    void testAssigningInSnapshotOnlyMode() {
        final String captureTable = "customers";

        MySqlSourceConfig sourceConfig =
                getConfig(new String[] {captureTable}, StartupOptions.snapshot());

        // Create and initialize assigner
        MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(
                        sourceConfig,
                        1,
                        new ArrayList<>(),
                        false,
                        getMySqlSplitEnumeratorContext());
        assigner.open();

        // Get all snapshot splits
        List<MySqlSnapshotSplit> snapshotSplits = drainSnapshotSplits(assigner);

        // Generate fake finished offsets from 0 to snapshotSplits.size() - 1
        int i = 0;
        Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
        for (MySqlSnapshotSplit snapshotSplit : snapshotSplits) {
            BinlogOffset binlogOffset =
                    BinlogOffset.builder().setBinlogFilePosition("foo", i++).build();
            finishedOffsets.put(snapshotSplit.splitId(), binlogOffset);
        }
        assigner.onFinishedSplits(finishedOffsets);

        // Get the binlog split
        Optional<MySqlSplit> split = assigner.getNext();
        Assertions.assertThat(split).isPresent().get().isInstanceOf(MySqlBinlogSplit.class);
        MySqlBinlogSplit binlogSplit = split.get().asBinlogSplit();

        // Validate if the stopping offset of the binlog split is the maximum among all finished
        // offsets, which should be snapshotSplits.size() - 1
        Assertions.assertThat(binlogSplit.getEndingOffset())
                .isEqualTo(
                        BinlogOffset.builder()
                                .setBinlogFilePosition("foo", snapshotSplits.size() - 1)
                                .build());
    }

    private MySqlSourceConfig getConfig(String[] captureTables, StartupOptions startupOptions) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(startupOptions)
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }

    @Test
    public void testSetProcessingBacklog() {
        final String captureTable = "customers";
        MySqlSourceConfig configuration = getConfig(new String[] {captureTable});
        MockMySqlSplitEnumeratorEnumeratorContext enumeratorContext =
                getMySqlSplitEnumeratorContext();
        final MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(
                        configuration,
                        DEFAULT_PARALLELISM,
                        new ArrayList<>(),
                        false,
                        enumeratorContext);
        assertThat(enumeratorContext.isProcessingBacklog()).isFalse();
        assigner.open();
        assertThat(enumeratorContext.isProcessingBacklog()).isTrue();
        // Get all snapshot splits
        List<MySqlSnapshotSplit> snapshotSplits = drainSnapshotSplits(assigner);
        Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
        int i = 0;
        for (MySqlSnapshotSplit snapshotSplit : snapshotSplits) {
            BinlogOffset binlogOffset =
                    BinlogOffset.builder().setBinlogFilePosition("foo", i++).build();
            finishedOffsets.put(snapshotSplit.splitId(), binlogOffset);
        }
        assigner.onFinishedSplits(finishedOffsets);
        assertThat(enumeratorContext.isProcessingBacklog()).isFalse();
        assigner.close();
    }

    private MySqlSourceConfigFactory getConfigFactory(String[] captureTables) {
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
                .serverTimeZone(ZoneId.of("UTC").toString());
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        return getConfigFactory(captureTables).createConfig(0);
    }

    private List<MySqlSnapshotSplit> drainSnapshotSplits(MySqlHybridSplitAssigner assigner) {
        List<MySqlSnapshotSplit> snapshotSplits = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> split = assigner.getNext();
            if (!split.isPresent()) {
                break;
            }
            Assertions.assertThat(split.get()).isInstanceOf(MySqlSnapshotSplit.class);
            snapshotSplits.add(split.get().asSnapshotSplit());
        }
        return snapshotSplits;
    }
}
