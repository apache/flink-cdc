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

package org.apache.flink.cdc.common.route;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Unit test for {@link TableIdRouter}. */
public class TableIdRouterTest extends SchemaTestBase {

    private static List<String> testRoute(String tableId) {
        return TABLE_ID_ROUTER.route(TableId.parse(tableId)).stream()
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    private static String testConvert(String input) {
        return TableIdRouter.convertTableListToRegExpPattern(input);
    }

    @Test
    void testConvertingDebeziumTableIdToStandardRegex() {
        assertThat(testConvert("foo.bar")).isEqualTo("foo\\.bar");
        assertThat(testConvert("foo.bar,foo.baz")).isEqualTo("foo\\.bar|foo\\.baz");
        assertThat(testConvert("db.\\.*")).isEqualTo("db\\..*");
    }

    @Test
    void testImplicitRoute() {
        assertThat(testRoute("db_0.table_1")).containsExactlyInAnyOrder("db_0.table_1");
        assertThat(testRoute("db_0.table_2")).containsExactlyInAnyOrder("db_0.table_2");
        assertThat(testRoute("db_0.table_3")).containsExactlyInAnyOrder("db_0.table_3");
    }

    @Test
    void testOneToOneRoute() {
        assertThat(testRoute("db_1.table_1")).containsExactlyInAnyOrder("db_1.table_1");
        assertThat(testRoute("db_1.table_2")).containsExactlyInAnyOrder("db_1.table_2");
        assertThat(testRoute("db_1.table_3")).containsExactlyInAnyOrder("db_1.table_3");
    }

    @Test
    void testTwistedOneToOneRoute() {
        assertThat(testRoute("db_2.table_1")).containsExactlyInAnyOrder("db_2.table_2");
        assertThat(testRoute("db_2.table_2")).containsExactlyInAnyOrder("db_2.table_3");
        assertThat(testRoute("db_2.table_3")).containsExactlyInAnyOrder("db_2.table_1");
    }

    @Test
    void testMergingTablesRoute() {
        assertThat(testRoute("db_3.table_1")).containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(testRoute("db_3.table_2")).containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(testRoute("db_3.table_3")).containsExactlyInAnyOrder("db_3.table_merged");
    }

    @Test
    void testBroadcastingRoute() {
        assertThat(testRoute("db_4.table_1"))
                .containsExactlyInAnyOrder("db_4.table_a", "db_4.table_b", "db_4.table_c");
        assertThat(testRoute("db_4.table_2"))
                .containsExactlyInAnyOrder("db_4.table_b", "db_4.table_c");
        assertThat(testRoute("db_4.table_3")).containsExactlyInAnyOrder("db_4.table_c");
    }

    @Test
    void testRepSymRoute() {
        assertThat(testRoute("db_5.table_1"))
                .containsExactlyInAnyOrder("db_5.prefix_table_1_suffix");
        assertThat(testRoute("db_5.table_2"))
                .containsExactlyInAnyOrder("db_5.prefix_table_2_suffix");
        assertThat(testRoute("db_5.table_3"))
                .containsExactlyInAnyOrder("db_5.prefix_table_3_suffix");
    }

    @Test
    void testGroupSourceTablesByRouteRule() {
        Set<TableId> tableIdSet =
                new HashSet<>(
                        Arrays.asList(
                                TableId.parse("db_1.table_1"),
                                TableId.parse("db_1.table_2"),
                                TableId.parse("db_1.table_3"),
                                TableId.parse("db_2.table_1"),
                                TableId.parse("db_2.table_2"),
                                TableId.parse("db_2.table_3"),
                                TableId.parse("db_3.table_1"),
                                TableId.parse("db_3.table_2"),
                                TableId.parse("db_3.table_3"),
                                TableId.parse("db_4.table_1"),
                                TableId.parse("db_4.table_2"),
                                TableId.parse("db_4.table_3"),
                                TableId.parse("db_5.table_1"),
                                TableId.parse("db_5.table_2"),
                                TableId.parse("db_5.table_3")));
        assertThat(TABLE_ID_ROUTER.groupSourceTablesByRouteRule(tableIdSet))
                .containsExactlyInAnyOrder(
                        new HashSet<>(Arrays.asList(TableId.parse("db_1.table_1"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_1.table_2"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_1.table_3"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_2.table_1"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_2.table_2"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_2.table_3"))),
                        new HashSet<>(
                                Arrays.asList(
                                        TableId.parse("db_3.table_1"),
                                        TableId.parse("db_3.table_2"),
                                        TableId.parse("db_3.table_3"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_1"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_1"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_1"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_2"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_2"))),
                        new HashSet<>(Arrays.asList(TableId.parse("db_4.table_3"))),
                        new HashSet<>(
                                Arrays.asList(
                                        TableId.parse("db_5.table_1"),
                                        TableId.parse("db_5.table_2"),
                                        TableId.parse("db_5.table_3"))),
                        new HashSet<>(),
                        new HashSet<>(),
                        new HashSet<>());
    }

    @Test
    void testRegExpCapturingGroupExpression() {
        assertThat(Stream.of("re_1.table_1", "re_22.table_22", "re_333.table_333"))
                .map(TableIdRouterTest::testRoute)
                .map(List::toString)
                .containsExactly(
                        "[database.another_table_with_111_index]",
                        "[database.another_table_with_222222_index]",
                        "[database.another_table_with_333333333_index]");

        assertThat(Stream.of("inv_1.table_foo", "inv_22.table_bar", "inv_333.table_baz"))
                .map(TableIdRouterTest::testRoute)
                .map(List::toString)
                .containsExactly("[table_foo.inv_1]", "[table_bar.inv_22]", "[table_baz.inv_333]");
    }

    private static List<String> testStdRegExpRoute(
            String sourceRouteRule, String sinkRouteRule, List<String> sourceTables) {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(new RouteRule(sourceRouteRule, sinkRouteRule)),
                        RouteMode.ALL_MATCH);
        return sourceTables.stream()
                .map(TableId::parse)
                .map(router::route)
                .map(List::toString)
                .collect(Collectors.toList());
    }

    @Test
    void testRegExpComplexRouting() {
        // Capture the entire database.
        List<String> tablesToRoute =
                List.of("db1.tbl1", "db1.tbl2", "db1.tbl3", "db2.tbl2", "db2.tbl3", "db3.tbl3");
        assertThat(testStdRegExpRoute("db1.(\\.*)", "db1.combined", tablesToRoute))
                .containsExactly(
                        "[db1.combined]",
                        "[db1.combined]",
                        "[db1.combined]",
                        "[db2.tbl2]",
                        "[db2.tbl3]",
                        "[db3.tbl3]");

        // Capture the entire database and append prefixes.
        assertThat(testStdRegExpRoute("db1.(\\.*)", "db1.pre_$1", tablesToRoute))
                .containsExactly(
                        "[db1.pre_tbl1]",
                        "[db1.pre_tbl2]",
                        "[db1.pre_tbl3]",
                        "[db2.tbl2]",
                        "[db2.tbl3]",
                        "[db3.tbl3]");

        // Capture the entire database and append suffixes.
        assertThat(testStdRegExpRoute("db1.(\\.*)", "db1.$1_suf", tablesToRoute))
                .containsExactly(
                        "[db1.tbl1_suf]",
                        "[db1.tbl2_suf]",
                        "[db1.tbl3_suf]",
                        "[db2.tbl2]",
                        "[db2.tbl3]",
                        "[db3.tbl3]");

        // Capture the entire database and append extract parts.
        assertThat(testStdRegExpRoute("db1.tbl(\\.*)", "db1.no$1", tablesToRoute))
                .containsExactly(
                        "[db1.no1]",
                        "[db1.no2]",
                        "[db1.no3]",
                        "[db2.tbl2]",
                        "[db2.tbl3]",
                        "[db3.tbl3]");

        // Capture databases and append database prefix.
        assertThat(testStdRegExpRoute("(\\.*).tbl3", "pre_$1.tbl3", tablesToRoute))
                .containsExactly(
                        "[db1.tbl1]",
                        "[db1.tbl2]",
                        "[pre_db1.tbl3]",
                        "[db2.tbl2]",
                        "[pre_db2.tbl3]",
                        "[pre_db3.tbl3]");

        // Capture databases and append database suffix.
        assertThat(testStdRegExpRoute("(\\.*).tbl3", "$1_suf.tbl3", tablesToRoute))
                .containsExactly(
                        "[db1.tbl1]",
                        "[db1.tbl2]",
                        "[db1_suf.tbl3]",
                        "[db2.tbl2]",
                        "[db2_suf.tbl3]",
                        "[db3_suf.tbl3]");

        // Capture databases and extract database parts.
        assertThat(testStdRegExpRoute("db(\\.*).(tbl\\.*)", "no$1.$2", tablesToRoute))
                .containsExactly(
                        "[no1.tbl1]",
                        "[no1.tbl2]",
                        "[no1.tbl3]",
                        "[no2.tbl2]",
                        "[no2.tbl3]",
                        "[no3.tbl3]");

        // Capture multiple parts and append extra tags.
        assertThat(
                        testStdRegExpRoute(
                                "db(\\.*).tbl(\\.*)", "Database$1.Collection$2", tablesToRoute))
                .containsExactly(
                        "[Database1.Collection1]",
                        "[Database1.Collection2]",
                        "[Database1.Collection3]",
                        "[Database2.Collection2]",
                        "[Database2.Collection3]",
                        "[Database3.Collection3]");
    }

    // ==================== Alternation / replaceAll regression (FLINK-38779 style)
    // ====================

    private static List<String> routeWith(String source, String sink, List<String> tables) {
        TableIdRouter router =
                new TableIdRouter(
                        Collections.singletonList(new RouteRule(source, sink)),
                        RouteMode.ALL_MATCH);
        return tables.stream()
                .map(TableId::parse)
                .map(router::route)
                .map(ids -> ids.stream().map(TableId::toString).collect(Collectors.toList()))
                .map(list -> list.get(0))
                .collect(Collectors.toList());
    }

    @Test
    void testAlternationOrderInDatabasePart() {
        List<String> testTables =
                Arrays.asList(
                        "saas_pw_00.users",
                        "saas_pw_01.orders",
                        "saas_pw_02.items",
                        "saas_pw.config");

        assertThat(
                        routeWith(
                                "(saas_pw_00|saas_pw_01|saas_pw_02|saas_pw).(\\.*)",
                                "target_db.merged",
                                testTables))
                .as("Longer-first order should route all tables to target_db.merged")
                .containsExactly(
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged");

        assertThat(
                        routeWith(
                                "(saas_pw|saas_pw_00|saas_pw_01|saas_pw_02).(\\.*)",
                                "target_db.merged",
                                testTables))
                .as("Shorter-first order should also route all tables to target_db.merged")
                .containsExactly(
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged");

        assertThat(
                        routeWith(
                                "(saas_pw_00|saas_pw|saas_pw_01|saas_pw_02).(\\.*)",
                                "target_db.merged",
                                testTables))
                .as("Shorter-in-middle order should also route all tables to target_db.merged")
                .containsExactly(
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged");
    }

    @Test
    void testAlternationOrderInTablePart() {
        List<String> testTables =
                Arrays.asList(
                        "mydb.saas_pw_00", "mydb.saas_pw_01", "mydb.saas_pw_02", "mydb.saas_pw");

        assertThat(
                        routeWith(
                                "mydb.(saas_pw_00|saas_pw_01|saas_pw_02|saas_pw)",
                                "target_db.merged",
                                testTables))
                .as("Longer-first in table part")
                .containsExactly(
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged");

        assertThat(
                        routeWith(
                                "mydb.(saas_pw|saas_pw_00|saas_pw_01|saas_pw_02)",
                                "target_db.merged",
                                testTables))
                .as("Shorter-first in table part")
                .containsExactly(
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged",
                        "target_db.merged");
    }

    @Test
    void testAlternationOrderWithCaptureGroupReplacement() {
        List<String> testTables =
                Arrays.asList("order.users", "order_detail.users", "order_archive.users");

        assertThat(
                        routeWith(
                                "(order|order_detail|order_archive).(\\.*)",
                                "warehouse.$1_$2",
                                testTables))
                .as("Capture group $1 should reflect full match, not partial")
                .containsExactly(
                        "warehouse.order_users",
                        "warehouse.order_detail_users",
                        "warehouse.order_archive_users");

        assertThat(
                        routeWith(
                                "(order_archive|order_detail|order).(\\.*)",
                                "warehouse.$1_$2",
                                testTables))
                .as("Capture group with longer-first should produce identical results")
                .containsExactly(
                        "warehouse.order_users",
                        "warehouse.order_detail_users",
                        "warehouse.order_archive_users");
    }

    @Test
    void testSinkTableWithLiteralDollarSign() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(new RouteRule("mydb.tbl", "price$market.data")),
                        RouteMode.ALL_MATCH);
        List<String> result =
                router.route(TableId.parse("mydb.tbl")).stream()
                        .map(TableId::toString)
                        .collect(Collectors.toList());
        assertThat(result).containsExactly("price$market.data");
    }

    @Test
    void testSinkTableWithTrailingDollarSign() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(new RouteRule("mydb.tbl", "target.price$")), RouteMode.ALL_MATCH);
        List<String> result =
                router.route(TableId.parse("mydb.tbl")).stream()
                        .map(TableId::toString)
                        .collect(Collectors.toList());
        assertThat(result).containsExactly("target.price$");
    }

    @Test
    void testSinkTableWithBackslash() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(new RouteRule("mydb.tbl", "target.path\\data")),
                        RouteMode.ALL_MATCH);
        List<String> result =
                router.route(TableId.parse("mydb.tbl")).stream()
                        .map(TableId::toString)
                        .collect(Collectors.toList());
        assertThat(result).containsExactly("target.path\\data");
    }

    @Test
    void testShardingTablesMergeToDataWarehouse() {
        List<String> shardingTables =
                List.of(
                        "order_01.order_info_0001",
                        "order_02.order_info_0002",
                        "order_99.order_info_9999",
                        "order_1000.order_info_0001");

        assertThat(
                        testStdRegExpRoute(
                                "order_\\d+.order_info_\\d+",
                                "ods.ods_order_order_info",
                                shardingTables))
                .containsExactly(
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]");

        assertThat(
                        testStdRegExpRoute(
                                "(order)_\\d+.(order_info)_\\d+", "ods.ods_$1_$2", shardingTables))
                .containsExactly(
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]",
                        "[ods.ods_order_order_info]");

        assertThat(
                        testStdRegExpRoute(
                                "(\\.*)_\\d+.(\\.*)_\\d+",
                                "ods.ods_$1_$2",
                                List.of(
                                        "order_01.order_info_0001",
                                        "user_01.user_info_0001",
                                        "product_99.product_detail_9999")))
                .containsExactly(
                        "[ods.ods_order_order_info]",
                        "[ods.ods_user_user_info]",
                        "[ods.ods_product_product_detail]");
    }
}
