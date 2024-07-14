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
import org.apache.flink.util.ExceptionUtils;

import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.junit.BeforeClass;
import org.junit.Test;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link MySqlSnapshotSplitAssigner}. */
public class MySqlSnapshotSplitAssignerTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    public void testAssignSingleTableSplits() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWhoseRowCntLessSplitSize() {
        List<String> expected = Arrays.asList("customers null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2000,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMultipleTableSplits() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignCompositePkTableSplitsUnevenlyWithChunkKeyColumn() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignCompositePkTableSplitsEvenlyWithChunkKeyColumn() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignCompositePkTableWithWrongChunkKeyColumn() {
        try {
            getTestAssignSnapshotSplits(
                    customerDatabase,
                    4,
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                    new String[] {"customer_card"},
                    "errorCol");
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "Chunk key column 'errorCol' doesn't exist in the columns [card_no,level,name,note] of the table")
                            .isPresent());
        }
    }

    @Test
    public void testEnableAutoIncrementedKeyOptimization() {
        List<String> expected =
                Arrays.asList("shopping_cart_big null [3]", "shopping_cart_big [3] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"shopping_cart_big"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignSnapshotSplitsWithRandomPrimaryKey() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignSnapshotSplitsWithDecimalKey() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithMultipleKey() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithSparseDistributionSplitKey() {
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
        assertEquals(expected, splits);

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
        assertEquals(expected1, splits1);

        // test sparse table that the approximate row count is bigger than chunk size
        List<String> expected2 =
                Arrays.asList("customers_sparse_dist null [18]", "customers_sparse_dist [18] null");
        List<String> splits2 =
                getTestAssignSnapshotSplits(
                        8,
                        10d,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_sparse_dist"});
        assertEquals(expected2, splits2);
    }

    @Test
    public void testAssignTableWithDenseDistributionSplitKey() {
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
        assertEquals(expected, splits);

        // test dense table with bigger dense distribution factor lower
        List<String> expected1 =
                Arrays.asList("customers_dense_dist null [2]", "customers_dense_dist [2] null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        2,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        0.9d,
                        new String[] {"customers_dense_dist"});
        assertEquals(expected1, splits1);
    }

    @Test
    public void testAssignTableWithSingleLine() {
        List<String> expected = Collections.singletonList("customer_card_single_line null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customer_card_single_line"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithCombinedIntSplitKey() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithConfiguredStringSplitKey() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMinSplitSize() {
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
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMaxSplitSize() {
        List<String> expected = Collections.singletonList("customers_even_dist null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        8096,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"});
        assertEquals(expected, splits);
    }

    @Test
    public void testUnMatchedPrimaryKey() {
        try {
            getTestAssignSnapshotSplits(
                    4,
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                    new String[] {"customer_card"});
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The defined primary key [card_no] in Flink is not matched with actual primary key [card_no, level] in MySQL")
                            .isPresent());
        }
    }

    @Test
    public void testTableWithoutPrimaryKey() {
        String tableWithoutPrimaryKey = "customers_no_pk";
        try {
            getTestAssignSnapshotSplits(
                    4,
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                    CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                    new String[] {tableWithoutPrimaryKey});
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.")
                            .isPresent());
        }
    }

    @Test
    public void testAssignTableWithoutPrimaryKeyWithChunkKeyColumn() {
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
        assertEquals(expected, splits);
    }

    public void testAssignTableWithPrimaryKeyWithChunkKeyColumnNotInPrimaryKey() {
        String tableWithoutPrimaryKey = "customers";
        List<String> expected =
                Arrays.asList(
                        "customers null [user_5]",
                        "customers [user_5] [user_9]",
                        "customers [user_9] [user_13]",
                        "customers [user_13] [user_17]",
                        "customers [user_17] [user_20]",
                        "customers [user_20] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {tableWithoutPrimaryKey},
                        "name");
        assertEquals(expected, splits);
    }

    @Test
    public void testEnumerateTablesLazily() {
        final MySqlSourceConfig configuration =
                getConfig(
                        customerDatabase,
                        4,
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        new String[] {"customers_even_dist"},
                        "id",
                        false);

        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration, DEFAULT_PARALLELISM, new ArrayList<>(), false);

        assertTrue(assigner.needToDiscoveryTables());
        assigner.open();
        assertTrue(assigner.getNext().isPresent());
        assertFalse(assigner.needToDiscoveryTables());
    }

    @Test
    public void testScanNewlyAddedTableStartFromCheckpoint() {
        List<String> expected =
                Arrays.asList(
                        "customers_sparse_dist [109] null",
                        "customers_even_dist null [10]",
                        "customers_even_dist [10] [18]",
                        "customers_even_dist [18] null",
                        "customer_card_single_line null null");
        assertEquals(expected, getTestAssignSnapshotSplitsFromCheckpoint());
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
        MySqlSourceConfig configuration =
                getConfig(
                        database,
                        splitSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        captureTables,
                        chunkKeyColumn,
                        false);
        List<TableId> remainingTables =
                Arrays.stream(captureTables)
                        .map(t -> database.getDatabaseName() + "." + t)
                        .map(TableId::parse)
                        .collect(Collectors.toList());
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration, DEFAULT_PARALLELISM, remainingTables, false);
        return getSplitsFromAssigner(assigner);
    }

    private List<String> getTestAssignSnapshotSplitsFromCheckpoint() {
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
                        true);
        List<TableId> remainingTables = new ArrayList<>();
        List<TableId> alreadyProcessedTables = new ArrayList<>();
        alreadyProcessedTables.add(processedTable);

        RowType splitKeyType =
                ChunkUtils.getChunkKeyColumnType(
                        Column.editor().name("id").type("INT").jdbcType(4).create());
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
                        AssignerStatus.INITIAL_ASSIGNING,
                        remainingTables,
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(configuration, DEFAULT_PARALLELISM, checkpoint);
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
            boolean scanNewlyAddedTableEnabled) {
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
                .createConfig(0);
    }
}
