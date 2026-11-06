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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Utilities to filter snapshot of table. */
public class SnapshotFilterUtils {

    private SnapshotFilterUtils() {}

    private static final Map<List<Map.Entry<String, String>>, Map<Selectors, String>> cache =
            new ConcurrentHashMap<>();

    /**
     * Converts the given filters to a map keyed by {@link Selectors}, caching the result.
     *
     * <p>The immutable list of entries is an order-sensitive cache key, so configurations with the
     * same rules in different orders retain their own matching precedence.
     */
    private static Map<Selectors, String> toSelector(Map<String, String> filters) {
        List<Map.Entry<String, String>> immutableFilters =
                Collections.unmodifiableList(
                        filters.entrySet().stream()
                                .map(AbstractMap.SimpleImmutableEntry::new)
                                .collect(Collectors.toList()));

        return cache.computeIfAbsent(
                immutableFilters,
                key -> {
                    // Use LinkedHashMap to preserve insertion order for deterministic matching
                    Map<Selectors, String> snapshotFilters = new LinkedHashMap<>();
                    key.forEach(
                            entry -> {
                                Selectors selector =
                                        new Selectors.SelectorsBuilder()
                                                .includeTables(entry.getKey())
                                                .build();
                                snapshotFilters.put(selector, entry.getValue());
                            });
                    return snapshotFilters;
                });
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
