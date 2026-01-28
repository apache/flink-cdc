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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.schema.Selectors;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Calculates how upstream data change events should be dispatched to downstream tables. Returns one
 * or many destination Table IDs based on provided routing rules.
 */
@PublicEvolving
public class TableIdRouter {

    private static final Logger LOG = LoggerFactory.getLogger(TableIdRouter.class);
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final List<Tuple3<Pattern, String, String>> routes;
    private final LoadingCache<TableId, List<TableId>> routingCache;
    private final RouteMode routeMode;

    private static final String DOT_PLACEHOLDER = "_dot_placeholder_";

    /**
     * Currently, The supported regular syntax is not exactly the same in {@link Selectors}.
     *
     * <p>The main discrepancies are :
     *
     * <p>1) {@link Selectors} use {@code ,} to split table names instead of `|`.
     *
     * <p>2) If there is a need to use a dot ({@code .}) in a regular expression to match any
     * character, it is necessary to escape the dot with a backslash.
     *
     * <p>3) The unescaped {@code .} is used as the separator of database and table name. When
     * converting to Debezium style, it is expected to be escaped to match the dot ({@code .})
     * literally instead of the meta-character.
     */
    public static String convertTableListToRegExpPattern(String tables) {
        LOG.info("Rewriting CDC style table capture list: {}", tables);

        // In CDC-style table matching, table names could be separated by `,` character.
        // Convert it to `|` as it's standard RegEx syntax.
        tables =
                Arrays.stream(tables.split(",")).map(String::trim).collect(Collectors.joining("|"));
        LOG.info("Expression after replacing comma with vert separator: {}", tables);

        // Essentially, we're just trying to swap escaped `\\.` and unescaped `.`.
        // In our table matching syntax, `\\.` means RegEx token matcher and `.` means database &
        // table name separator.
        // On the contrary, while we're matching TableId string, `\\.` means matching the "dot"
        // literal and `.` is the meta-character.

        // Step 1: escape the dot with a backslash, but keep it as a placeholder (like `$`).
        // For example, `db\.*.tbl\.*` => `db$*.tbl$*`
        String unescapedTables = tables.replace("\\.", DOT_PLACEHOLDER);
        LOG.info("Expression after un-escaping dots as RegEx meta-character: {}", unescapedTables);

        // Step 2: replace all remaining dots (`.`) to quoted version (`\.`), as a separator between
        // database and table names.
        // For example, `db$*.tbl$*` => `db$*\.tbl$*`
        String unescapedTablesWithDbTblSeparator = unescapedTables.replace(".", "\\.");
        LOG.info("Re-escaping dots as TableId delimiter: {}", unescapedTablesWithDbTblSeparator);

        // Step 3: restore placeholder to normal RegEx matcher (`.`)
        // For example, `db$*\.tbl$*` => `db.*\.tbl.*`
        String standardRegExpTableCaptureList =
                unescapedTablesWithDbTblSeparator.replace(DOT_PLACEHOLDER, ".");
        LOG.info("Final standard RegExp table capture list: {}", standardRegExpTableCaptureList);

        return standardRegExpTableCaptureList;
    }

    public TableIdRouter(List<RouteRule> routingRules, RouteMode routeMode) {
        this.routeMode = routeMode;
        this.routes = new ArrayList<>();
        for (RouteRule rule : routingRules) {
            try {
                routes.add(
                        new Tuple3<>(
                                Pattern.compile(convertTableListToRegExpPattern(rule.sourceTable)),
                                rule.sinkTable,
                                rule.replaceSymbol));
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
        List<TableId> routedTableIds = new ArrayList<>();

        for (Tuple3<Pattern, String, String> route : routes) {
            if (matches(route.f0, sourceTableId)) {
                routedTableIds.add(resolveReplacement(sourceTableId, route));

                // If match mode is FIRST_MATCH, stop after the first match
                if (routeMode == RouteMode.FIRST_MATCH) {
                    break;
                }
            }
        }

        if (routedTableIds.isEmpty()) {
            routedTableIds.add(sourceTableId);
        }
        return routedTableIds;
    }

    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Pattern, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        } else {
            Matcher matcher = route.f0.matcher(originalTable.toString());
            if (matcher.find()) {
                return TableId.parse(matcher.replaceAll(route.f1));
            }
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
        return routes.stream()
                .map(
                        route ->
                                tableIdSet.stream()
                                        .filter(tableId -> matches(route.f0, tableId))
                                        .collect(Collectors.toSet()))
                .collect(Collectors.toList());
    }

    private static boolean matches(Pattern pattern, TableId tableId) {
        return pattern.matcher(tableId.toString()).matches();
    }
}
