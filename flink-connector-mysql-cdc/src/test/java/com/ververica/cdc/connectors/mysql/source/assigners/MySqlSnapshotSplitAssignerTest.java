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
                        "customers_2 null [105]",
                        "customers_2 [105] [109]",
                        "customers_2 [109] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers_2"});
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
                        "customers_2 null [106]",
                        "customers_2 [106] null",
                        "customers_3 null [12]",
                        "customers_3 [12] [22]",
                        "customers_3 [22] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        5,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {
                            customerDatabase.getDatabaseName() + ".customers_2",
                            customerDatabase.getDatabaseName() + ".customers_3"
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
                        "shopping_cart_dec null [123458.1230]",
                        "shopping_cart_dec [123458.1230] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".shopping_cart_dec"});
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
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customer_card"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithSparseDistributionSplitKey() {
        // test table with sparse split key order like 0,2,4,8 instead of 0,1,2,3
        List<String> expected =
                Arrays.asList(
                        "customers_3 null [10]", "customers_3 [10] [18]", "customers_3 [18] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        4,
                        2000.0d,
                        new String[] {customerDatabase.getDatabaseName() + ".customers_3"});
        assertEquals(expected, splits);

        // test table with sparse split key and big chunk size
        List<String> expected1 = Arrays.asList("customers_3 null null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        16,
                        10000.0d,
                        new String[] {customerDatabase.getDatabaseName() + ".customers_3"});
        assertEquals(expected1, splits1);
    }

    @Test
    public void testAssignTableWithDenseDistributionSplitKey() {
        // test table with dense distribution which one split key may contains multiple rows
        List<String> expected = Arrays.asList("customers_4 null [3]", "customers_4 [3] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers_4"});
        assertEquals(expected, splits);

        // test table with dense distribution and big chunk size
        List<String> expected1 = Arrays.asList("customers_4 null null");
        List<String> splits1 =
                getTestAssignSnapshotSplits(
                        10,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers_4"});
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
                        "customers_2 null [103]",
                        "customers_2 [103] [105]",
                        "customers_2 [105] [107]",
                        "customers_2 [107] [109]",
                        "customers_2 [109] null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        2,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers_2"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMaxSplitSize() {
        List<String> expected = Collections.singletonList("customers_2 null null");
        List<String> splits =
                getTestAssignSnapshotSplits(
                        8096,
                        EVENLY_DISTRIBUTION_FACTOR.defaultValue(),
                        new String[] {customerDatabase.getDatabaseName() + ".customers_2"});
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
