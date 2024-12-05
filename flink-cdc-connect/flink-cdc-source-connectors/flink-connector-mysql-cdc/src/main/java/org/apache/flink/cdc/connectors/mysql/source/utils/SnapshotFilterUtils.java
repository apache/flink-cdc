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

import org.apache.flink.cdc.common.schema.Selectors;

import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.Map;

/** Utilities to filter snapshot of table. */
public class SnapshotFilterUtils {

    private SnapshotFilterUtils() {}

    private static final Map<Map<String, String>, Map<Selectors, String>> cache = new HashMap<>();

    /**
     * Don't worry about atomicity. We don't need to use the synchronized keyword to ensure thread
     * safety here. Since filters come from the source configuration, they shouldn’t be changed
     * during runtime. So the result will always be idempotent.
     */
    private static Map<Selectors, String> toSelector(Map<String, String> filters) {
        Map<Selectors, String> cached = cache.get(filters);
        if (cached != null) {
            return cached;
        }

        Map<Selectors, String> snapshotFilters = new HashMap<>();
        filters.forEach(
                (table, filter) -> {
                    Selectors selector =
                            new Selectors.SelectorsBuilder().includeTables(table).build();
                    snapshotFilters.put(selector, filter);
                });
        cache.put(filters, snapshotFilters);

        return snapshotFilters;
    }

    public static String getSnapshotFilter(Map<String, String> filters, TableId tableId) {
        Map<Selectors, String> snapshotFilters = toSelector(filters);

        String filter = null;
        for (Selectors selector : snapshotFilters.keySet()) {
            if (selector.isMatch(
                    org.apache.flink.cdc.common.event.TableId.tableId(
                            tableId.catalog(), tableId.table()))) {
                filter = snapshotFilters.get(selector);
                break;
            }
        }
        return filter;
    }
}
