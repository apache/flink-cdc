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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.utils.SnapshotFilterUtils}. */
public class SnapshotFilterUtilsTest {

    @Test
    public void testGetSnapshotFilter() {
        Map<String, String> map = new HashMap<>();
        map.put("db.user", "id > 100");
        map.put("db.order_[0-9]+", "id > 200");
        Assertions.assertThat(SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.user")))
                .isEqualTo("id > 100");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.order_1")))
                .isEqualTo("id > 200");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.order_2")))
                .isEqualTo("id > 200");
        Assertions.assertThat(SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.shop")))
                .isNull();
    }

    @Test
    public void testGetSnapshotFilterPreservesOrder() {
        // Use LinkedHashMap to ensure deterministic order
        Map<String, String> map = new LinkedHashMap<>();
        map.put("db.table_a", "id > 100");
        map.put("db.table_b", "id > 200");
        map.put("db.table_c", "id > 300");

        // Verify each table matches its corresponding filter
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.table_a")))
                .isEqualTo("id > 100");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.table_b")))
                .isEqualTo("id > 200");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.table_c")))
                .isEqualTo("id > 300");

        // Test with regex patterns - non-overlapping patterns
        Map<String, String> regexMap = new LinkedHashMap<>();
        regexMap.put("db.order_[0-9]+", "id > 100");
        regexMap.put("db.user_[0-9]+", "id > 200");
        regexMap.put("db.product_[0-9]+", "id > 300");

        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(
                                regexMap, TableId.parse("db.order_1")))
                .isEqualTo("id > 100");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(regexMap, TableId.parse("db.user_2")))
                .isEqualTo("id > 200");
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(
                                regexMap, TableId.parse("db.product_3")))
                .isEqualTo("id > 300");

        // Test with overlapping patterns - this is the critical case that reproduces the original
        // bug
        // Multiple patterns match the same table, should return the first one in insertion order
        Map<String, String> overlappingMap = new LinkedHashMap<>();
        overlappingMap.put("db.order_[0-9]+", "id > 100"); // Broader pattern - matches order_1
        overlappingMap.put("db.order_1", "id > 200"); // More specific - also matches order_1
        overlappingMap.put("db.order_[1-9]", "id > 300"); // Also matches order_1

        // All three patterns match db.order_1, but should consistently return the first one
        Assertions.assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(
                                overlappingMap, TableId.parse("db.order_1")))
                .isEqualTo("id > 100"); // First pattern wins

        // Verify this is deterministic by calling multiple times
        for (int i = 0; i < 10; i++) {
            Assertions.assertThat(
                            SnapshotFilterUtils.getSnapshotFilter(
                                    overlappingMap, TableId.parse("db.order_1")))
                    .isEqualTo("id > 100");
        }
    }
}
