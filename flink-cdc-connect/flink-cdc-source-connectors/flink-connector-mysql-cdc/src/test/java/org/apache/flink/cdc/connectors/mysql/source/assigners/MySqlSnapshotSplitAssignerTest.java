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
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset.ofEarliest;
import static org.apache.flink.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MySqlSnapshotSplitAssigner}. */
class MySqlSnapshotSplitAssignerTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeAll
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    void testAssignSingleTableSplits() {
        List<String> expected =
                Arrays.asList(
                        "customers_even_dist null [105]",
                        "customers_even_dist [105] [109]",
                        "customers_even_dist [109] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWhoseRowCntLessSplitSize() {
        List<String> expected = Collections.singletonList("customers null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2000,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignMultipleTableSplits() {
        List<String> expected =
                Arrays.asList(
                        "customers_even_dist null [105]",
                        "customers_even_dist [105] [109]",
                        "customers_even_dist [109] null",
                        "customers_sparse_dist null [10]",
                        "customers_sparse_dist [10] [18]",
                        "customers_sparse_dist [18] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist", "customers_sparse_dist"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignCompositePkTableSplitsUnevenlyWithChunkKeyColumn() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart null [KIND_007]",
                        "shopping_cart [KIND_007] [KIND_008]",
                        "shopping_cart [KIND_008] [KIND_009]",
                        "shopping_cart [KIND_009] [KIND_100]",
                        "shopping_cart [KIND_100] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart"},
                        "product_kind");
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignCompositePkTableSplitsEvenlyWithChunkKeyColumn() {
        List<String> expected =
                Arrays.asList(
                        "evenly_shopping_cart null [105]",
                        "evenly_shopping_cart [105] [109]",
                        "evenly_shopping_cart [109] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"evenly_shopping_cart"},
                        "product_no");
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignCompositePkTableWithWrongChunkKeyColumn() {
        Assertions.assertThatThrownBy(
                        () ->
                                getTestAssignSnapshotSplits(
                                        customerDatabase,
                                        4,
                                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                                .defaultValue(),
                                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                                .defaultValue(),
                                        new String[] {"customer_card"},
                                        "errorCol"))
                .hasStackTraceContaining(
                        "Chunk key column 'errorCol' doesn't exist in the columns [card_no,level,name,note] of the table");
    }

    @Test
    void testEnableAutoIncrementedKeyOptimization() {
        List<String> expected =
                Arrays.asList("shopping_cart_big null [3]", "shopping_cart_big [3] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart_big"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignSnapshotSplitsWithRandomPrimaryKey() {
        List<String> expected =
                Arrays.asList(
                        "address null [417111867899200427]",
                        "address [417111867899200427] [417420106184475563]",
                        "address [417420106184475563] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"address"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignSnapshotSplitsWithDecimalKey() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart_dec null [123458.1230]",
                        "shopping_cart_dec [123458.1230] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart_dec"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWithMultipleKey() {
        List<String> expected =
                Arrays.asList(
                        "customer_card null [20004]",
                        "customer_card [20004] [30006]",
                        "customer_card [30006] [30009]",
                        "customer_card [30009] [40001]",
                        "customer_card [40001] [50001]",
                        "customer_card [50001] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customer_card"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWithSparseDistributionSplitKey() {
        // test table with sparse split key order like 0,2,4,8 instead of 0,1,2,3
        // test sparse table with bigger distribution factor upper
        List<String> expected =
                Arrays.asList(
                        "customers_sparse_dist null [10]",
                        "customers_sparse_dist [10] [18]",
                        "customers_sparse_dist [18] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        2000.0d,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_sparse_dist"});
        assertThat(splits).isEqualTo(expected);

        // test sparse table with smaller distribution factor upper
        List<String> expected1 =
                Arrays.asList(
                        "customers_sparse_dist null [8]",
                        "customers_sparse_dist [8] [17]",
                        "customers_sparse_dist [17] null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        4,
                        2.0d,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_sparse_dist"});
        assertThat(splits1).isEqualTo(expected1);

        // test sparse table that the approximate row count is bigger than chunk size
        List<String> expected2 =
                Arrays.asList("customers_sparse_dist null [18]", "customers_sparse_dist [18] null");
        List<String> splits2 =
                getTestAssignSnapshotSplits(
                        8,
                        10d,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_sparse_dist"});
        assertThat(splits2).isEqualTo(expected2);
    }

    @Test
    void testAssignTableWithDenseDistributionSplitKey() {
        // test dense table with smaller dense distribution factor lower
        List<String> expected =
                Arrays.asList(
                        "customers_dense_dist null [2]",
                        "customers_dense_dist [2] [3]",
                        "customers_dense_dist [3] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_dense_dist"});
        assertThat(splits).isEqualTo(expected);

        // test dense table with bigger dense distribution factor lower
        List<String> expected1 =
                Arrays.asList("customers_dense_dist null [2]", "customers_dense_dist [2] null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        0.9d,
                        new String[] {"customers_dense_dist"});
        assertThat(splits1).isEqualTo(expected1);
    }

    @Test
    void testAssignTableWithSingleLine() {
        List<String> expected = Collections.singletonList("customer_card_single_line null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customer_card_single_line"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWithCombinedIntSplitKey() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart null [user_2]",
                        "shopping_cart [user_2] [user_4]",
                        "shopping_cart [user_4] [user_5]",
                        "shopping_cart [user_5] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWithConfiguredStringSplitKey() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart null [user_2]",
                        "shopping_cart [user_2] [user_4]",
                        "shopping_cart [user_4] [user_5]",
                        "shopping_cart [user_5] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignMinSplitSize() {
        List<String> expected =
                Arrays.asList(
                        "customers_even_dist null [103]",
                        "customers_even_dist [103] [105]",
                        "customers_even_dist [105] [107]",
                        "customers_even_dist [107] [109]",
                        "customers_even_dist [109] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignMaxSplitSize() {
        List<String> expected = Collections.singletonList("customers_even_dist null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        8096,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"});
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testUnMatchedPrimaryKey() {
        try {
            getTestAssignSnapshotSplits(
                    4,
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                    new String[] {"customer_card"});
        } catch (Throwable t) {
            assertThat(t)
                    .hasStackTraceContaining(
                            "The defined primary key [card_no] in Flink is not matched with actual primary key [card_no, level] in MySQL");
        }
    }

    @Test
    void testTableWithoutPrimaryKey() {
        String tableWithoutPrimaryKey = "customers_no_pk";

        Assertions.assertThatThrownBy(
                        () -> {
                            getTestAssignSnapshotSplits(
                                    4,
                                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                                    new String[] {tableWithoutPrimaryKey});
                        })
                .hasStackTraceContaining(
                        "To use incremental snapshot, 'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.");
    }

    @Test
    void testAssignTableWithoutPrimaryKeyWithChunkKeyColumn() {
        String tableWithoutPrimaryKey = "customers_no_pk";
        List<String> expected =
                Arrays.asList(
                        "customers_no_pk null [462]",
                        "customers_no_pk [462] [823]",
                        "customers_no_pk [823] [1184]",
                        "customers_no_pk [1184] [1545]",
                        "customers_no_pk [1545] [1906]",
                        "customers_no_pk [1906] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {tableWithoutPrimaryKey},
                        "id");
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testAssignTableWithPrimaryKeyWithChunkKeyColumnNotInPrimaryKey() {
        String tableWithoutPrimaryKey = "customers";
        List<String> expected =
                Arrays.asList(
                        "customers null [user_12]",
                        "customers [user_12] [user_15]",
                        "customers [user_15] [user_18]",
                        "customers [user_18] [user_20]",
                        "customers [user_20] [user_4]",
                        "customers [user_4] [user_7]",
                        "customers [user_7] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {tableWithoutPrimaryKey},
                        "name");
        assertThat(splits).isEqualTo(expected);
    }

    @Test
    void testEnumerateTablesLazily() {
        final MySqlSourceConfig configuration =
                getConfig(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"},
                        "id",
                        false,
                        false);

        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration,
                        DEFAULT_PARALLELISM,
                        new ArrayList<>(),
                        false,
                        getMySqlSplitEnumeratorContext());

        assertThat(assigner.needToDiscoveryTables()).isTrue();
        assigner.open();
        assertThat(assigner.getNext()).isPresent();
        assertThat(assigner.needToDiscoveryTables()).isFalse();
    }

    @Test
    void testScanNewlyAddedTableStartFromInitialAssigningFinishedCheckpoint() {
        List<String> expected =
                Arrays.asList(
                        "customers_sparse_dist [109] null",
                        "customers_even_dist null [10]",
                        "customers_even_dist [10] [18]",
                        "customers_even_dist [18] null",
                        "customer_card_single_line null null");
        assertThat(
                        getTestAssignSnapshotSplitsFromCheckpoint(
                                AssignerStatus.INITIAL_ASSIGNING_FINISHED))
                .isEqualTo(expected);
    }

    @Test
    void testScanNewlyAddedTableStartFromNewlyAddedAssigningSnapshotFinishedCheckpoint() {
        List<String> expected =
                Arrays.asList(
                        "customers_sparse_dist [109] null",
                        "customers_even_dist null [10]",
                        "customers_even_dist [10] [18]",
                        "customers_even_dist [18] null");
        assertThat(
                        getTestAssignSnapshotSplitsFromCheckpoint(
                                AssignerStatus.NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED))
                .isEqualTo(expected);
    }

    @Test
    void testSplitEvenlySizedChunksEndingFirst() {
        List<String> expected =
                Arrays.asList(
                        "evenly_shopping_cart [109] null",
                        "evenly_shopping_cart null [105]",
                        "evenly_shopping_cart [105] [109]");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"evenly_shopping_cart"},
                        "product_no",
                        true);
        assertThat(splits).isEqualTo(expected);
    }

    private List<String> getTestAssignSnapshotSplits(
            int splitSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            String[] captureTables) {
        return getTestAssignSnapshotSplits(
                customerDatabase,
                splitSize,
                distributionFactorUpper,
                distributionFactorLower,
                captureTables,
                null);
    }

    private List<String> getTestAssignSnapshotSplits(
            UniqueDatabase database,
            int splitSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            String[] captureTables,
            String chunkKeyColumn) {
        return getTestAssignSnapshotSplits(
                database,
                splitSize,
                distributionFactorUpper,
                distributionFactorLower,
                captureTables,
                chunkKeyColumn,
                false);
    }

    private List<String> getTestAssignSnapshotSplits(
            UniqueDatabase database,
            int splitSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            String[] captureTables,
            String chunkKeyColumn,
            boolean assignUnboundedChunkFirst) {
        MySqlSourceConfig configuration =
                getConfig(
                        database,
                        splitSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        captureTables,
                        chunkKeyColumn,
                        false,
                        assignUnboundedChunkFirst);
        List<TableId> remainingTables =
                Arrays.stream(captureTables)
                        .map(t -> database.getDatabaseName() + "." + t)
                        .map(TableId::parse)
                        .collect(Collectors.toList());
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration,
                        DEFAULT_PARALLELISM,
                        remainingTables,
                        false,
                        getMySqlSplitEnumeratorContext());
        return getSplitsFromAssigner(assigner);
    }

    private List<String> getTestAssignSnapshotSplitsFromCheckpoint(AssignerStatus assignerStatus) {
        TableId newTable =
                TableId.parse(customerDatabase.getDatabaseName() + ".customer_card_single_line");
        TableId processedTable =
                TableId.parse(customerDatabase.getDatabaseName() + ".customers_sparse_dist");
        TableId splitTable =
                TableId.parse(customerDatabase.getDatabaseName() + ".customers_even_dist");
        String[] captureTables = {newTable.table(), processedTable.table(), splitTable.table()};
        MySqlSourceConfig configuration =
                getConfig(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        captureTables,
                        null,
                        true,
                        false);
        List<TableId> remainingTables = new ArrayList<>();
        List<TableId> alreadyProcessedTables = new ArrayList<>();
        alreadyProcessedTables.add(processedTable);

        RowType splitKeyType =
                ChunkUtils.getChunkKeyColumnType(
                        Column.editor().name("id").type("INT").jdbcType(4).create(), true);
        List<MySqlSchemalessSnapshotSplit> remainingSplits =
                Arrays.asList(
                        new MySqlSchemalessSnapshotSplit(
                                processedTable,
                                processedTable + ":2",
                                splitKeyType,
                                new Object[] {109},
                                null,
                                null),
                        new MySqlSchemalessSnapshotSplit(
                                splitTable,
                                splitTable + ":0",
                                splitKeyType,
                                null,
                                new Object[] {10},
                                null),
                        new MySqlSchemalessSnapshotSplit(
                                splitTable,
                                splitTable + ":1",
                                splitKeyType,
                                new Object[] {10},
                                new Object[] {18},
                                null),
                        new MySqlSchemalessSnapshotSplit(
                                splitTable,
                                splitTable + ":2",
                                splitKeyType,
                                new Object[] {18},
                                null,
                                null));

        Map<String, MySqlSchemalessSnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(
                processedTable + ":0",
                new MySqlSchemalessSnapshotSplit(
                        processedTable,
                        processedTable + ":0",
                        splitKeyType,
                        null,
                        new Object[] {105},
                        null));
        assignedSplits.put(
                processedTable + ":1",
                new MySqlSchemalessSnapshotSplit(
                        processedTable,
                        processedTable + ":1",
                        splitKeyType,
                        new Object[] {105},
                        new Object[] {109},
                        null));
        Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();
        splitFinishedOffsets.put(processedTable + ":0", ofEarliest());
        SnapshotPendingSplitsState checkpoint =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        new HashMap<>(),
                        splitFinishedOffsets,
                        assignerStatus,
                        remainingTables,
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration,
                        DEFAULT_PARALLELISM,
                        checkpoint,
                        getMySqlSplitEnumeratorContext());
        return getSplitsFromAssigner(assigner);
    }

    private List<String> getSplitsFromAssigner(final MySqlSnapshotSplitAssigner assigner) {
        assigner.open();
        List<MySqlSplit> sqlSplits = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> split = assigner.getNext();
            if (split.isPresent()) {
                sqlSplits.add(split.get());
            } else {
                break;
            }
        }
        assigner.close();

        return sqlSplits.stream()
                .map(
                        split -> {
                            if (split.isSnapshotSplit()) {
                                return split.asSnapshotSplit().getTableId().table()
                                        + " "
                                        + Arrays.toString(split.asSnapshotSplit().getSplitStart())
                                        + " "
                                        + Arrays.toString(split.asSnapshotSplit().getSplitEnd());
                            } else {
                                return split.toString();
                            }
                        })
                .collect(Collectors.toList());
    }

    private MySqlSourceConfig getConfig(
            UniqueDatabase database,
            int splitSize,
            double distributionFactorUpper,
            double distributionLower,
            String[] captureTables,
            String chunkKeyColumn,
            boolean scanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst) {
        Map<ObjectPath, String> chunkKeys = new HashMap<>();
        for (String table : captureTables) {
            chunkKeys.put(new ObjectPath(database.getDatabaseName(), table), chunkKeyColumn);
        }
        String[] fullNames = new String[captureTables.length];
        for (int i = 0; i < captureTables.length; i++) {
            fullNames[i] = database.getDatabaseName() + "." + captureTables[i];
        }
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(database.getDatabaseName())
                .tableList(fullNames)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .splitSize(splitSize)
                .fetchSize(2)
                .distributionFactorUpper(distributionFactorUpper)
                .distributionFactorLower(distributionLower)
                .username(database.getUsername())
                .password(database.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .chunkKeyColumn(chunkKeys)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                .assignUnboundedChunkFirst(assignUnboundedChunkFirst)
                .createConfig(0);
    }
}
