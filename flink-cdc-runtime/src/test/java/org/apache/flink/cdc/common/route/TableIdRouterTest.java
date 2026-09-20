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

    // ---- Case-insensitive route tests based on real-world production rules ----

    private static List<String> routeWith(TableIdRouter router, String tableId) {
        return router.route(TableId.parse(tableId)).stream()
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    @Test
    void testCamelCaseDatabaseNameMatchesLowerCase() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "shortPlay.hi_label_language_new",
                                        "ods.hi_label_language_new")));
        Stream.of(
                        "shortplay.hi_label_language_new",
                        "shortPlay.hi_label_language_new",
                        "SHORTPLAY.HI_LABEL_LANGUAGE_NEW")
                .forEach(
                        tableId ->
                                assertThat(routeWith(router, tableId))
                                        .containsExactly("ods.hi_label_language_new"));
    }

    @Test
    void testMixedCaseDatabaseNamesInSameJob() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "shortPlay.hi_short_play_config",
                                        "ods.hi_short_play_config"),
                                new RouteRule(
                                        "shortplay.hi_task_center_config",
                                        "ods.ods_mysql2sr_shortplay_hi_task_center_config_rt"),
                                new RouteRule(
                                        "shortplay.hi_daily_report_country_income",
                                        "ods.ods_mysql_shortplay_hi_daily_report_country_income_i_rt")));

        assertThat(
                        Stream.of(
                                "shortplay.hi_short_play_config",
                                "shortPlay.hi_task_center_config",
                                "otherdb.some_table"))
                .map(id -> routeWith(router, id).toString())
                .containsExactly(
                        "[ods.hi_short_play_config]",
                        "[ods.ods_mysql2sr_shortplay_hi_task_center_config_rt]",
                        "[otherdb.some_table]");
    }

    @Test
    void testUpperCaseSourceTableMatchesLowerCase() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "devops_ci_process.T_PIPELINE_BUILD_HISTORY",
                                        "ods_mbu_pdw_devops_ci_process.t_pipeline_build_history"),
                                new RouteRule(
                                        "devops_ci_process.T_PIPELINE_INFO",
                                        "ods_mbu_pdw_devops_ci_process.t_pipeline_info")));

        assertThat(
                        Stream.of(
                                "devops_ci_process.t_pipeline_build_history",
                                "devops_ci_process.T_PIPELINE_BUILD_HISTORY",
                                "devops_ci_process.t_pipeline_info"))
                .map(id -> routeWith(router, id).toString())
                .containsExactly(
                        "[ods_mbu_pdw_devops_ci_process.t_pipeline_build_history]",
                        "[ods_mbu_pdw_devops_ci_process.t_pipeline_build_history]",
                        "[ods_mbu_pdw_devops_ci_process.t_pipeline_info]");
    }

    @Test
    void testAllUpperCaseTableNamesInWmsRules() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule("wms.GSP_DRUG_ISH_REC", "gsp_wms.GSP_DRUG_ISH_REC"),
                                new RouteRule("wms.GSP_OSH_REC", "gsp_wms.GSP_OSH_REC"),
                                new RouteRule(
                                        "wms.wms_goods_attribute", "gsp_wms.wms_goods_attribute")));

        assertThat(
                        Stream.of(
                                "wms.gsp_drug_ish_rec",
                                "WMS.GSP_DRUG_ISH_REC",
                                "wms.WMS_GOODS_ATTRIBUTE"))
                .map(id -> routeWith(router, id).toString())
                .containsExactly(
                        "[gsp_wms.GSP_DRUG_ISH_REC]",
                        "[gsp_wms.GSP_DRUG_ISH_REC]",
                        "[gsp_wms.wms_goods_attribute]");
    }

    @Test
    void testCamelCaseTableNames() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "hotelbooking.SupplierBookingHotelConfirmCodeUpdateRecord",
                                        "didadata.booking.supplier_booking_hotel_confirm_code_update_record"),
                                new RouteRule(
                                        "hotelbooking.ChannelBookingRateAdjust",
                                        "didadata.booking.channel_booking_reate_adjust")));

        Stream.of(
                        "hotelbooking.supplierbookinghotelconfirmcodeupdaterecord",
                        "hotelbooking.SupplierBookingHotelConfirmCodeUpdateRecord")
                .forEach(
                        tableId ->
                                assertThat(routeWith(router, tableId))
                                        .containsExactly(
                                                "didadata.booking.supplier_booking_hotel_confirm_code_update_record"));
    }

    @Test
    void testCommaSeparatedTablesWithRepSymAndCaseInsensitive() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "PaymentDB.tblRefundRecord,PaymentDB.tblSignRecord,PaymentDB.tblDeductRecord",
                                        "ods.ods_paymentdb_<>_rt",
                                        "<>")));

        assertThat(
                        Stream.of(
                                "paymentdb.tblRefundRecord",
                                "PaymentDB.tblSignRecord",
                                "paymentdb.tbldeductrecord",
                                "PaymentDB.tblOther"))
                .map(id -> routeWith(router, id).toString())
                .containsExactly(
                        "[ods.ods_paymentdb_tblRefundRecord_rt]",
                        "[ods.ods_paymentdb_tblSignRecord_rt]",
                        "[ods.ods_paymentdb_tbldeductrecord_rt]",
                        "[PaymentDB.tblOther]");
    }

    @Test
    void testWildcardRouteWithCaseMismatch() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "shortPlay.hi_watch_history\\.*", "ods.hi_watch_history")));

        Stream.of(
                        "shortplay.hi_watch_history_01",
                        "SHORTPLAY.HI_WATCH_HISTORY_99",
                        "shortPlay.hi_watch_history")
                .forEach(
                        tableId ->
                                assertThat(routeWith(router, tableId))
                                        .containsExactly("ods.hi_watch_history"));
    }

    @Test
    void testMergingShardedTablesWithCaseMismatch() {
        TableIdRouter router =
                new TableIdRouter(
                        List.of(
                                new RouteRule(
                                        "shortPlay.hi_bonus_record_v2_t0", "ods.hi_bonus_record"),
                                new RouteRule(
                                        "shortPlay.hi_bonus_record_v2_t1", "ods.hi_bonus_record")));

        Stream.of("shortplay.hi_bonus_record_v2_t0", "SHORTPLAY.HI_BONUS_RECORD_V2_T1")
                .forEach(
                        tableId ->
                                assertThat(routeWith(router, tableId))
                                        .containsExactly("ods.hi_bonus_record"));
    }

    @Test
    void testRouteWithoutBackReferenceWithMultiDigitSuffix() {
        // Regression test: when the source-table regex matches multi-digit suffixes (e.g. 10-16)
        // but the sink-table does NOT contain a `$1` back-reference, every matched source table
        // should still be routed to the exact sink-table declared in the routing rule. Previously
        // TableIdRouter.resolveReplacement used Matcher.find(), which only matched a prefix of
        // the source table ID, leaving the unmatched tail characters to be appended to the
        // sink-table after replaceAll. As a result, table `db_6.table_13` was wrongly routed to
        // `new_db_6.table_merged3` (and similarly `db_6.table_14` to `new_db_6.table_merged4`).
        List<String> sourceTables =
                List.of(
                        "db_6.table_1",
                        "db_6.table_2",
                        "db_6.table_9",
                        "db_6.table_10",
                        "db_6.table_11",
                        "db_6.table_13",
                        "db_6.table_14",
                        "db_6.table_15",
                        "db_6.table_16");
        assertThat(
                        testStdRegExpRoute(
                                "db_6.table_([1-9]|1[0-6])", "new_db_6.table_merged", sourceTables))
                .containsExactly(
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]",
                        "[new_db_6.table_merged]");
    }

    @Test
    void testRouteWithBackReferenceAndMultiDigitCapture() {
        // Companion test for the same fix: when the sink-table uses a `$1` back-reference, the
        // captured group must be the FULL multi-digit value (e.g. `13`), not just a single digit.
        // Previously the alternation order in `([1-9]|1[0-6])` together with find() caused
        // `[1-9]` to win against `1[0-6]` for input `13`, capturing only `1` and producing a
        // sink-table name `..._1` followed by leftover `3`.
        List<String> sourceTables =
                List.of("db_6.table_1", "db_6.table_10", "db_6.table_13", "db_6.table_16");
        assertThat(
                        testStdRegExpRoute(
                                "db_6.table_([1-9]|1[0-6])", "new_db_6.table_$1", sourceTables))
                .containsExactly(
                        "[new_db_6.table_1]",
                        "[new_db_6.table_10]",
                        "[new_db_6.table_13]",
                        "[new_db_6.table_16]");
    }

    @Test
    void testRouteWithBackReferenceSuffixAndMultiDigitCapture() {
        // Genuine multi-character capture-group regression test: the sink-table template
        // `new_db_6.table_$1_suffix` puts a literal `_suffix` AFTER the `$1` back-reference. With
        // the pre-fix `Matcher.find()` + `Matcher.replaceAll()` implementation, the regex
        // `new_db_6\.table_([1-9]|1[0-6])` matched the prefix `new_db_6.table_1` of the source
        // table id `new_db_6.table_13`, leaving the unmatched tail `3` to be appended to the
        // sink-table, producing the wrong sink-table id `new_db_6.table_1_suffix3` (note the `3`
        // at the very end, after `_suffix`). The corrected implementation uses `matches()` and
        // therefore consumes the entire source table id, yielding the expected
        // `new_db_6.table_13_suffix`.
        List<String> sourceTables =
                List.of(
                        "new_db_6.table_1",
                        "new_db_6.table_2",
                        "new_db_6.table_9",
                        "new_db_6.table_10",
                        "new_db_6.table_13",
                        "new_db_6.table_16");
        assertThat(
                        testStdRegExpRoute(
                                "new_db_6.table_([1-9]|1[0-6])",
                                "new_db_6.table_$1_suffix",
                                sourceTables))
                .containsExactly(
                        "[new_db_6.table_1_suffix]",
                        "[new_db_6.table_2_suffix]",
                        "[new_db_6.table_9_suffix]",
                        "[new_db_6.table_10_suffix]",
                        "[new_db_6.table_13_suffix]",
                        "[new_db_6.table_16_suffix]");
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
}
