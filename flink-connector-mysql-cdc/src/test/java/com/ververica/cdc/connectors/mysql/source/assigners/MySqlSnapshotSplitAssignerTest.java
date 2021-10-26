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

import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
                        "customers null [109]",
                        "customers [109] [118]",
                        "customers [118] [1009]",
                        "customers [1009] [1012]",
                        "customers [1012] [1015]",
                        "customers [1015] [1018]",
                        "customers [1018] null");
        List<String> splits = getTestAssignSnapshotSplits(4, new String[] {"customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWhoseRowCntLessSplitSize() {
        List<String> expected = Arrays.asList("customers null null");
        List<String> splits = getTestAssignSnapshotSplits(2000, new String[] {"customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMultipleTableSplits() {
        List<String> expected =
                Arrays.asList(
                        "customers null [109]",
                        "customers [109] [118]",
                        "customers [118] [1009]",
                        "customers [1009] [1012]",
                        "customers [1012] [1015]",
                        "customers [1015] [1018]",
                        "customers [1018] null",
                        "customers_1 null [109]",
                        "customers_1 [109] [118]",
                        "customers_1 [118] [1009]",
                        "customers_1 [1009] [1012]",
                        "customers_1 [1012] [1015]",
                        "customers_1 [1015] [1018]",
                        "customers_1 [1018] null");
        List<String> splits =
                getTestAssignSnapshotSplits(4, new String[] {"customers", "customers_1"});
        assertEquals(expected, splits);
    }

    @Test
    public void testEnableAutoIncrementedKeyOptimization() {
        List<String> expected =
                Arrays.asList("shopping_cart_big null [3]", "shopping_cart_big [3] null");
        List<String> splits = getTestAssignSnapshotSplits(2, new String[] {"shopping_cart_big"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignSnapshotSplitsWithRandomPrimaryKey() {
        List<String> expected =
                Arrays.asList(
                        "address null [417111867899200427]",
                        "address [417111867899200427] [417420106184475563]",
                        "address [417420106184475563] null");
        List<String> splits = getTestAssignSnapshotSplits(4, new String[] {"address"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignSnapshotSplitsWithDecimalKey() {
        List<String> expected =
                Arrays.asList(
                        "shopping_cart_dec null [124456.4560]",
                        "shopping_cart_dec [124456.4560] null");
        List<String> splits = getTestAssignSnapshotSplits(2, new String[] {"shopping_cart_dec"});
        assertEquals(expected, splits);
    }

    private List<String> getTestAssignSnapshotSplits(int splitSize, String[] captureTables) {
        MySqlSourceConfig configuration = getConfig(splitSize, captureTables);
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration,
                        DEFAULT_PARALLELISM,
                        MySqlValidator.builder()
                                .dbzProperties(configuration.getDbzProperties())
                                .build());

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
        List<String> splits = getTestAssignSnapshotSplits(4, new String[] {"customer_card"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignTableWithSingleLine() {
        List<String> expected = Collections.singletonList("customer_card_single_line null null");
        List<String> splits =
                getTestAssignSnapshotSplits(4, new String[] {"customer_card_single_line"});
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
        List<String> splits = getTestAssignSnapshotSplits(4, new String[] {"shopping_cart"});
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
        List<String> splits = getTestAssignSnapshotSplits(4, new String[] {"shopping_cart"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMinSplitSize() {
        List<String> expected =
                Arrays.asList(
                        "customers null [102]",
                        "customers [102] [103]",
                        "customers [103] [109]",
                        "customers [109] [110]",
                        "customers [110] [111]",
                        "customers [111] [118]",
                        "customers [118] [121]",
                        "customers [121] [123]",
                        "customers [123] [1009]",
                        "customers [1009] [1010]",
                        "customers [1010] [1011]",
                        "customers [1011] [1012]",
                        "customers [1012] [1013]",
                        "customers [1013] [1014]",
                        "customers [1014] [1015]",
                        "customers [1015] [1016]",
                        "customers [1016] [1017]",
                        "customers [1017] [1018]",
                        "customers [1018] [1019]",
                        "customers [1019] null");
        List<String> splits = getTestAssignSnapshotSplits(2, new String[] {"customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testAssignMaxSplitSize() {
        List<String> expected = Collections.singletonList("customers null null");
        List<String> splits = getTestAssignSnapshotSplits(2000, new String[] {"customers"});
        assertEquals(expected, splits);
    }

    @Test
    public void testUnMatchedPrimaryKey() {
        try {
            getTestAssignSnapshotSplits(4, new String[] {"customer_card"});
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The defined primary key [card_no] in Flink is not matched with actual primary key [card_no, level] in MySQL")
                            .isPresent());
        }
    }

    private MySqlSourceConfig getConfig(int splitSize, String[] captureTables) {
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
                .splitSize(splitSize)
                .fetchSize(2)
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }
}
