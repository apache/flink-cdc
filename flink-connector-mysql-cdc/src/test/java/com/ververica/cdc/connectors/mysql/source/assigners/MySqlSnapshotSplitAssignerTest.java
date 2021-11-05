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

import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.relational.TableId;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.EVENLY_DISTRIBUTION_FACTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
                        "customers null [462]",
                        "customers [462] [823]",
                        "customers [823] [1184]",
                        "customers [1184] [1545]",
                        "customers [1545] [1906]",
                        "customers [1906] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWhoseRowCntLessSplitSize() {
        List<String> expected = Arrays.asList("customers null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2000,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMultipleTableSplits() {
        List<String> expected =
                Arrays.asList(
                        "customers null [462]",
                        "customers [462] [823]",
                        "customers [823] [1184]",
                        "customers [1184] [1545]",
                        "customers [1545] [1906]",
                        "customers [1906] null",
                        "customers_1 null [462]",
                        "customers_1 [462] [823]",
                        "customers_1 [823] [1184]",
                        "customers_1 [1184] [1545]",
                        "customers_1 [1545] [1906]",
                        "customers_1 [1906] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {
                            customerDatabase.getDatabaseName() + ".customers",
                            customerDatabase.getDatabaseName() + ".customers_1"
                        });
        assertEquals(expected, splits);
    }

    @Test
    public void testEnableAutoIncrementedKeyOptimization() {
        List<String> expected =
                Arrays.asList("shopping_cart_big null [3]", "shopping_cart_big [3] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".shopping_cart_big"});
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
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".address"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignSnapshotSplitsWithDecimalKey() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart_dec null [124812.1230]",
                        "shopping_cart_dec [124812.1230] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".shopping_cart_dec"});
        assertEquals(expected, splits);
    }

    private List<String> getTestAssignSnapshotSplits(
            int splitSize, double evenlyDistributionFactor, String[] captureTables) {
        MySqlSourceConfig configuration =
                getConfig(splitSize, evenlyDistributionFactor, captureTables);
        List<TableId> remainingTables =
                Arrays.stream(captureTables).map(TableId::parse).collect(Collectors.toList());
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration, DEFAULT_PARALLELISM, remainingTables, false);

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
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customer_card"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithSparseDistributionSplitKey() {
        // test table with sparse split key order like 0,10000,20000,3000 instead of 0,1,2,3
        List<String> expected =
                Arrays.asList(
                        "customer_card null [26317]",
                        "customer_card [26317] [32633]",
                        "customer_card [32633] [38949]",
                        "customer_card [38949] [45265]",
                        "customer_card [45265] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        2000.0d,
                        new String[] {customerDatabase.getDatabaseName() + ".customer_card"});
        assertEquals(expected, splits);

        // test table with sparse split key and big chunk size
        List<String> expected1 = Arrays.asList("customer_card null null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        8096,
                        10000.0d,
                        new String[] {customerDatabase.getDatabaseName() + ".customer_card"});
        assertEquals(expected1, splits1);
    }

    @Test
    public void testAssignTableWithSingleLine() {
        List<String> expected = Collections.singletonList("customer_card_single_line null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {
                            customerDatabase.getDatabaseName() + ".customer_card_single_line"
                        });
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
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".shopping_cart"});
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
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".shopping_cart"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMinSplitSize() {
        List<String> expected =
                Arrays.asList(
                        "customers null [281]",
                        "customers [281] [461]",
                        "customers [461] [641]",
                        "customers [641] [821]",
                        "customers [821] [1001]",
                        "customers [1001] [1181]",
                        "customers [1181] [1361]",
                        "customers [1361] [1541]",
                        "customers [1541] [1721]",
                        "customers [1721] [1901]",
                        "customers [1901] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMaxSplitSize() {
        List<String> expected = Collections.singletonList("customers null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2000,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testUnMatchedPrimaryKey() {
        try {
            getTestAssignSnapshotSplits(
                    4,
                    EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                    new String[] {customerDatabase.getDatabaseName() + ".customer_card"});
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The defined primary key [card_no] in Flink is not matched with actual primary key [card_no, level] in MySQL")
                            .isPresent());
        }
    }

    private MySqlSourceConfig getConfig(
            int splitSize, double evenlyDistributionFactor, String[] captureTables) {
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTables)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .splitSize(splitSize)
                .fetchSize(2)
                .evenlyDistributionFactor(evenlyDistributionFactor)
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }
}
