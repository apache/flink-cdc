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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Selectors;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Calculates how upstream data change events should be dispatched to downstream tables. Returns one
 * or many destination Table IDs based on provided routing rules.
 */
public class TableIdRouter {

    private final List<Tuple3<Selectors, String, String>> routes;
    private final LoadingCache<TableId, List<TableId>> routingCache;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    public TableIdRouter(List<RouteRule> routingRules) {
        this.routes = new ArrayList<>();
        for (RouteRule rule : routingRules) {
            try {
                String tableInclusions = rule.sourceTable;
                Selectors selectors =
                        new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
                routes.add(new Tuple3<>(selectors, rule.sinkTable, rule.replaceSymbol));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Failed to parse regular expression in routing rule %s. Notice that `.` is used to separate Table ID components. To use it as a regex token, put a `\\` before to escape it.",
                                rule),
                        e);
            }
        }
        this.routingCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, List<TableId>>() {
                                    @Override
                                    public @Nonnull List<TableId> load(@Nonnull TableId key) {
                                        return calculateRoute(key);
                                    }
                                });
    }

    public List<TableId> route(TableId sourceTableId) {
        return routingCache.getUnchecked(sourceTableId);
    }

    private List<TableId> calculateRoute(TableId sourceTableId) {
        List<TableId> routedTableIds =
                routes.stream()
                        .filter(route -> route.f0.isMatch(sourceTableId))
                        .map(route -> resolveReplacement(sourceTableId, route))
                        .collect(Collectors.toList());
        if (routedTableIds.isEmpty()) {
            routedTableIds.add(sourceTableId);
        }
        return routedTableIds;
    }

    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Selectors, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        }
        return TableId.parse(route.f1);
    }

    /**
     * Group the source tables that conform to the same routing rule together. The total number of
     * groups is less than or equal to the number of routing rules. For the source tables within
     * each group, their table structures will be merged to obtain the widest table structure in
     * that group. The structures of all tables within the group will be expanded to this widest
     * table structure.
     *
     * @param tableIdSet The tables need to be grouped by the router
     * @return The tables grouped by the router
     */
    public List<Set<TableId>> groupSourceTablesByRouteRule(Set<TableId> tableIdSet) {
        if (routes.isEmpty()) {
            return new ArrayList<>();
        }
        List<Set<TableId>> routedTableIds =
                routes.stream()
                        .map(
                                route -> {
                                    return tableIdSet.stream()
                                            .filter(
                                                    tableId -> {
                                                        return route.f0.isMatch(tableId);
                                                    })
                                            .collect(Collectors.toSet());
                                })
                        .collect(Collectors.toList());
        return routedTableIds;
    }
}
