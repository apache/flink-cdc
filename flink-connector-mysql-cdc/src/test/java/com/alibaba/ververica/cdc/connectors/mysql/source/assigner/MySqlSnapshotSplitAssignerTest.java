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

package com.alibaba.ververica.cdc.connectors.mysql.source.assigner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.ExceptionUtils;

import com.alibaba.ververica.cdc.connectors.mysql.MySqlTestBase;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link MySqlSnapshotSplitAssigner}. */
public class MySqlSnapshotSplitAssignerTest extends MySqlTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    public void testAssignSingleTableSplits() {
        String[] expected =
                new String[] {
                    "customers null [109]",
                    "customers [109] [118]",
                    "customers [118] [1009]",
                    "customers [1009] [1012]",
                    "customers [1012] [1015]",
                    "customers [1015] [1018]",
                    "customers [1018] [2000]",
                    "customers [2000] null"
                };
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> splits = getTestAssignSnapshotSplits(4, pkType, new String[] {"customers"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignMultipleTableSplits() {
        String[] expected =
                new String[] {
                    "customers null [109]",
                    "customers [109] [118]",
                    "customers [118] [1009]",
                    "customers [1009] [1012]",
                    "customers [1012] [1015]",
                    "customers [1015] [1018]",
                    "customers [1018] [2000]",
                    "customers [2000] null",
                    "customers_1 null [109]",
                    "customers_1 [109] [118]",
                    "customers_1 [118] [1009]",
                    "customers_1 [1009] [1012]",
                    "customers_1 [1012] [1015]",
                    "customers_1 [1015] [1018]",
                    "customers_1 [1018] [2000]",
                    "customers_1 [2000] null"
                };
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(4, pkType, new String[] {"customers", "customers_1"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testEnableAutoIncrementedKeyOptimization() {
        String[] expected =
                new String[] {
                    "shopping_cart_big null [1]",
                    "shopping_cart_big [1] [3]",
                    "shopping_cart_big [3] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("product_no", DataTypes.DECIMAL(20, 0)))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(2, pkType, new String[] {"shopping_cart_big"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignSnapshotSplitsWithDecimalKey() {
        String[] expected =
                new String[] {
                    "shopping_cart_dec null [124456.4560]",
                    "shopping_cart_dec [124456.4560] [125489.6789]",
                    "shopping_cart_dec [125489.6789] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("product_no", DataTypes.DECIMAL(10, 4)))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(2, pkType, new String[] {"shopping_cart_dec"});
        assertArrayEquals(expected, splits.toArray());
    }

    private List<String> getTestAssignSnapshotSplits(
            int splitSize, RowType pkType, String[] captureTables) {
        Configuration configuration = getConfig();
        configuration.setString("scan.snapshot.chunk.size", String.valueOf(splitSize));
        List<String> captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .collect(Collectors.toList());
        configuration.setString("table.whitelist", String.join(",", captureTableIds));

        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration, pkType, new ArrayList<>(), new ArrayList<>());

        assigner.open();
        List<MySqlSplit> sqlSplits = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> split = assigner.getNext(null);
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
        String[] expected =
                new String[] {
                    "customer_card null [20004]",
                    "customer_card [20004] [30009]",
                    "customer_card [30009] [40001]",
                    "customer_card [40001] [50001]",
                    "customer_card [50001] [50003]",
                    "customer_card [50003] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(4, pkType, new String[] {"customer_card"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignTableWithSingleLine() {
        String[] expected =
                new String[] {
                    "customer_card_single_line null [20001]",
                    "customer_card_single_line [20001] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(4, pkType, new String[] {"customer_card_single_line"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignTableWithCombinedIntSplitKey() {
        String[] expected =
                new String[] {
                    "shopping_cart null [102]",
                    "shopping_cart [102] [401]",
                    "shopping_cart [401] [501]",
                    "shopping_cart [501] [701]",
                    "shopping_cart [701] [801]",
                    "shopping_cart [801] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("product_no", DataTypes.INT()),
                                        DataTypes.FIELD("user_id", DataTypes.STRING()),
                                        DataTypes.FIELD("product_kind", DataTypes.STRING()))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(4, pkType, new String[] {"shopping_cart"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignTableWithConfiguredStringSplitKey() {
        String[] expected =
                new String[] {
                    "shopping_cart null [user_1]",
                    "shopping_cart [user_1] [user_4]",
                    "shopping_cart [user_4] [user_5]",
                    "shopping_cart [user_5] [user_6]",
                    "shopping_cart [user_6] null"
                };
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("user_id", DataTypes.STRING()),
                                        DataTypes.FIELD("product_no", DataTypes.INT()),
                                        DataTypes.FIELD("product_kind", DataTypes.STRING()))
                                .getLogicalType();
        List<String> splits =
                getTestAssignSnapshotSplits(4, pkType, new String[] {"shopping_cart"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignMinSplitSize() {
        String[] expected =
                new String[] {
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
                    "customers [1019] [2000]",
                    "customers [2000] null"
                };
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> splits = getTestAssignSnapshotSplits(2, pkType, new String[] {"customers"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testAssignMaxSplitSize() {
        String[] expected = new String[] {"customers null [2000]", "customers [2000] null"};
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> splits = getTestAssignSnapshotSplits(2000, pkType, new String[] {"customers"});
        assertArrayEquals(expected, splits.toArray());
    }

    @Test
    public void testInvalidSplitSize() {
        try {
            final RowType pkType =
                    (RowType)
                            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()))
                                    .getLogicalType();
            getTestAssignSnapshotSplits(1, pkType, new String[] {"customers"});
            fail("should fail.");
        } catch (IllegalStateException e) {
            assertThat(
                    e,
                    containsMessage(
                            "The value of option 'scan.snapshot.chunk.size' must bigger than 1, but is 1"));
        }
    }

    @Test
    public void testUnMatchedPrimaryKey() {
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("card_no", DataTypes.BIGINT()))
                                .getLogicalType();
        try {
            getTestAssignSnapshotSplits(4, pkType, new String[] {"customer_card"});
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The defined primary key [card_no] in Flink is not matched with actual primary key [card_no, level] in MySQL")
                            .isPresent());
        }
    }

    private Configuration getConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.whitelist", customerDatabase.getDatabaseName());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customerDatabase.getUsername());
        properties.put("database.password", customerDatabase.getPassword());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id.range", "1001,1004");
        properties.put("scan.snapshot.fetch.size", "2");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", DATABASE_HISTORY_INSTANCE_NAME);
        return Configuration.fromMap(properties);
    }
}
