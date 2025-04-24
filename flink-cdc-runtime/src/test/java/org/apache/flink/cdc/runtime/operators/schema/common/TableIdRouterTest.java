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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Unit test for {@link TableIdRouter}. */
public class TableIdRouterTest extends SchemaTestBase {

    private static List<String> testRoute(String tableId) {
        return TABLE_ID_ROUTER.route(TableId.parse(tableId)).stream()
                .map(TableId::toString)
                .collect(Collectors.toList());
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
                        new HashSet<>());
    }
}
