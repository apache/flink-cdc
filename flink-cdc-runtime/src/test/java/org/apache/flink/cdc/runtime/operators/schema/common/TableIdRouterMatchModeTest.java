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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.route.TableIdRouter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Unit test for {@link TableIdRouter} with match-mode support. */
public class TableIdRouterMatchModeTest {

    @Test
    void testFirstMatchMode() {
        // Setup routing rules for first-match mode
        List<RouteRule> routingRules =
                Arrays.asList(
                        // Sharded tables should be merged
                        new RouteRule("mydb.order_\\.*", "ods_db.ods_orders"),
                        new RouteRule("mydb.product_\\.*", "ods_db.ods_products"),
                        // Catch-all rule for one-to-one mapping
                        new RouteRule("mydb.\\.*", "ods_db.ods_<>", "<>"));

        TableIdRouter router = new TableIdRouter(routingRules, RouteMode.FIRST_MATCH);

        // Test sharded order tables - should match first rule and stop
        assertThat(route(router, "mydb.order_1")).containsExactly("ods_db.ods_orders");
        assertThat(route(router, "mydb.order_2")).containsExactly("ods_db.ods_orders");
        assertThat(route(router, "mydb.order_100")).containsExactly("ods_db.ods_orders");

        // Test sharded product tables - should match second rule and stop
        assertThat(route(router, "mydb.product_1")).containsExactly("ods_db.ods_products");
        assertThat(route(router, "mydb.product_2")).containsExactly("ods_db.ods_products");

        // Test non-sharded tables - should match third rule (catch-all)
        assertThat(route(router, "mydb.user")).containsExactly("ods_db.ods_user");
        assertThat(route(router, "mydb.customer")).containsExactly("ods_db.ods_customer");
        assertThat(route(router, "mydb.config")).containsExactly("ods_db.ods_config");
    }

    @Test
    void testAllMatchMode() {
        // Setup routing rules for all-match mode (default behavior)
        List<RouteRule> routingRules =
                Arrays.asList(
                        new RouteRule("mydb.order_\\.*", "ods_db.ods_orders"),
                        new RouteRule("mydb.\\.*", "ods_db.ods_<>", "<>"));

        TableIdRouter router = new TableIdRouter(routingRules, RouteMode.ALL_MATCH);

        // Test sharded order tables - should match BOTH rules
        assertThat(route(router, "mydb.order_1"))
                .containsExactlyInAnyOrder("ods_db.ods_orders", "ods_db.ods_order_1");
        assertThat(route(router, "mydb.order_2"))
                .containsExactlyInAnyOrder("ods_db.ods_orders", "ods_db.ods_order_2");

        // Test non-sharded tables - should match only second rule
        assertThat(route(router, "mydb.user")).containsExactly("ods_db.ods_user");
    }

    @Test
    void testFirstMatchWithNoMatchingRules() {
        List<RouteRule> routingRules =
                Arrays.asList(
                        new RouteRule("mydb.order_\\.*", "ods_db.ods_orders"),
                        new RouteRule("mydb.product_\\.*", "ods_db.ods_products"));

        TableIdRouter router = new TableIdRouter(routingRules, RouteMode.FIRST_MATCH);

        // Table that doesn't match any rule should route to itself (implicit routing)
        assertThat(route(router, "otherdb.user")).containsExactly("otherdb.user");
    }

    @Test
    void testAllMatchWithMultipleMatchingRules() {
        // Setup multiple overlapping rules
        List<RouteRule> routingRules =
                Arrays.asList(
                        new RouteRule("db.table_\\.*", "db.merged_1"),
                        new RouteRule("db.table_\\.*", "db.merged_2"),
                        new RouteRule("db.table_\\.*", "db.merged_3"));

        TableIdRouter router = new TableIdRouter(routingRules, RouteMode.ALL_MATCH);

        // Should match all three rules
        assertThat(route(router, "db.table_1"))
                .containsExactlyInAnyOrder("db.merged_1", "db.merged_2", "db.merged_3");
    }

    private static List<String> route(TableIdRouter router, String tableId) {
        return router.route(TableId.parse(tableId)).stream()
                .map(TableId::toString)
                .collect(Collectors.toList());
    }
}
