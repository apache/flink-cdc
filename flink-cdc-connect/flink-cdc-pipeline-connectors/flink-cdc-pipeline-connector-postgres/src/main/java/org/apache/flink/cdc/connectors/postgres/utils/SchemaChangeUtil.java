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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils.toCdcTableId;
import static org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils.toSchema;

/**
 * Utilities for inferring CDC schema change events by comparing before/after Debezium table
 * schemas.
 *
 * <p>PostgreSQL DDL has exactly four operations that structurally change a table schema:
 *
 * <ul>
 *   <li><b>Add column</b> — always appended at the end (PostgreSQL does not support adding a column
 *       at an arbitrary position).
 *   <li><b>Drop column</b> — removes a column by name.
 *   <li><b>Rename column</b> — changes a column's name while preserving its position and type.
 *   <li><b>Alter column type</b> — changes a column's type while preserving its name and position.
 * </ul>
 *
 * <p>Ideally, PostgreSQL's {@code pg_attribute.attnum} would be the best way to distinguish whether
 * a column was dropped-and-recreated or simply renamed. However, the pgoutput relation message does
 * not include {@code attnum}. Even querying the database for the current attnum after the fact is
 * unreliable due to temporal mismatch — for example, if column {@code C} is renamed to {@code D}
 * and a new column {@code C} is added between two relation messages, a retroactive attnum query
 * would see the <em>new</em> {@code C}'s attnum, not the original one, leading to incorrect
 * inference.
 *
 * <p>Instead, this utility infers schema changes by comparing the before/after column lists and
 * computing the <b>minimum edit</b> using the four operations above. The main inherent ambiguity is
 * the last-column scenario: renaming the last column looks identical to dropping it and adding a
 * new column with the same name at the end, since both produce the same before/after column list.
 * The algorithm resolves this by always preferring the interpretation with the fewest operations
 * (rename costs 1; drop + add costs 2), so a last-column name change is inferred as a rename.
 */
public class SchemaChangeUtil {
    /**
     * Infers the minimum list of schema change events that transform {@code tableBefore} into
     * {@code tableAfter}. Returns a {@link CreateTableEvent} if {@code tableBefore} is null.
     */
    public static List<SchemaChangeEvent> inferSchemaChangeEvent(
            io.debezium.relational.TableId dbzTableId,
            @Nullable Table tableBefore,
            Table tableAfter,
            PostgresSourceConfig sourceConfig,
            PostgresDialect dialect) {

        if (tableBefore == null) {
            return Collections.singletonList(toCreateTableEvent(tableAfter, sourceConfig, dialect));
        }

        TableId cdcTableId =
                toCdcTableId(
                        dbzTableId,
                        sourceConfig.getDatabaseList().get(0),
                        sourceConfig.isIncludeDatabaseInTableId());
        PostgresConnectorConfig dbzConfig = sourceConfig.getDbzConnectorConfig();

        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            TypeRegistry typeRegistry = connection.getTypeRegistry();
            return inferMinimalSchemaChanges(
                    cdcTableId,
                    tableBefore.columns(),
                    tableAfter.columns(),
                    dbzConfig,
                    typeRegistry);
        }
    }

    public static CreateTableEvent toCreateTableEvent(
            Table table, PostgresSourceConfig sourceConfig, PostgresDialect dialect) {
        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            return toCreateTableEvent(table, sourceConfig, connection.getTypeRegistry());
        }
    }

    private static CreateTableEvent toCreateTableEvent(
            Table table, PostgresSourceConfig sourceConfig, TypeRegistry typeRegistry) {
        return new CreateTableEvent(
                toCdcTableId(
                        table.id(),
                        sourceConfig.getDatabaseList().get(0),
                        sourceConfig.isIncludeDatabaseInTableId()),
                toSchema(table, sourceConfig.getDbzConnectorConfig(), typeRegistry));
    }

    /**
     * Finds the minimum schema change events to transform beforeCols into afterCols using recursion
     * with memoization. Available operations: rename column, add column at last, drop column, alter
     * column type. Recursion depth bounded by total column count.
     */
    private static List<SchemaChangeEvent> inferMinimalSchemaChanges(
            TableId cdcTableId,
            List<Column> beforeCols,
            List<Column> afterCols,
            PostgresConnectorConfig dbzConfig,
            TypeRegistry typeRegistry) {

        int n = beforeCols.size();
        int m = afterCols.size();

        // memo[i][j] = min cost from state (i, j), -1 means unvisited
        int[][] memo = new int[n + 1][m + 1];
        for (int[] row : memo) {
            Arrays.fill(row, -1);
        }

        // Fill memoization table via recursion
        minCost(0, 0, n, m, beforeCols, afterCols, memo, dbzConfig, typeRegistry);

        // Traceback to build schema change events
        return tracebackEvents(
                cdcTableId, n, m, beforeCols, afterCols, memo, dbzConfig, typeRegistry);
    }

    /**
     * Recursively computes the minimum number of individual column operations to align
     * before[i..n-1] with after[j..m-1], where {@code i} is the current index into {@code
     * beforeCols} (0..n) and {@code j} is the current index into {@code afterCols} (0..m). Boundary
     * cases ({@code i == n} or {@code j == m}) return immediately without list access. At each
     * non-boundary state, either drop before[i] (cost 1) or match before[i] to after[j] (cost =
     * rename(0/1) + alterType(0/1)). Unmatched after columns are added at the end.
     */
    private static int minCost(
            int i,
            int j,
            int n,
            int m,
            List<Column> beforeCols,
            List<Column> afterCols,
            int[][] memo,
            PostgresConnectorConfig dbzConfig,
            TypeRegistry typeRegistry) {

        if (i == n) {
            return m - j; // add remaining after columns
        }
        if (j == m) {
            return n - i; // drop remaining before columns
        }
        if (memo[i][j] != -1) {
            return memo[i][j];
        }

        // Option 1: drop beforeCols[i]
        int dropCost =
                1 + minCost(i + 1, j, n, m, beforeCols, afterCols, memo, dbzConfig, typeRegistry);

        // Option 2: match beforeCols[i] to afterCols[j]
        int matchCost =
                columnMatchCost(beforeCols.get(i), afterCols.get(j), dbzConfig, typeRegistry)
                        + minCost(
                                i + 1,
                                j + 1,
                                n,
                                m,
                                beforeCols,
                                afterCols,
                                memo,
                                dbzConfig,
                                typeRegistry);

        memo[i][j] = Math.min(dropCost, matchCost);
        return memo[i][j];
    }

    /**
     * Computes the min cost of matching a before column to an after column (0, 1, or 2). if column
     * name is same and type is same, cost = 0; if column name is same and type is different, cost =
     * 1(alter type); if column name is different and type is same, cost = 1(rename); if column name
     * is different and type is different, cost = 2(rename + alter type);
     */
    private static int columnMatchCost(
            Column before,
            Column after,
            PostgresConnectorConfig dbzConfig,
            TypeRegistry typeRegistry) {
        int cost = 0;
        if (!before.name().equals(after.name())) {
            cost++;
        }
        DataType beforeType = PostgresTypeUtils.fromDbzColumn(before, dbzConfig, typeRegistry);
        DataType afterType = PostgresTypeUtils.fromDbzColumn(after, dbzConfig, typeRegistry);
        if (!beforeType.equals(afterType)) {
            cost++;
        }
        return cost;
    }

    /**
     * Traces back through the memoization table to reconstruct the optimal sequence of schema
     * change events. Events are emitted per-column in the natural left-to-right scan order.
     */
    private static List<SchemaChangeEvent> tracebackEvents(
            TableId cdcTableId,
            int n,
            int m,
            List<Column> beforeCols,
            List<Column> afterCols,
            int[][] memo,
            PostgresConnectorConfig dbzConfig,
            TypeRegistry typeRegistry) {

        List<SchemaChangeEvent> events = new ArrayList<>();

        int i = 0;
        int j = 0;
        while (i < n && j < m) {
            int dropCost = 1 + memoOrBase(i + 1, j, n, m, memo);
            int matchCost =
                    columnMatchCost(beforeCols.get(i), afterCols.get(j), dbzConfig, typeRegistry)
                            + memoOrBase(i + 1, j + 1, n, m, memo);

            if (dropCost <= matchCost) {
                events.add(
                        new DropColumnEvent(
                                cdcTableId, Collections.singletonList(beforeCols.get(i).name())));
                i++;
            } else {
                Column bc = beforeCols.get(i);
                Column ac = afterCols.get(j);

                if (!bc.name().equals(ac.name())) {
                    events.add(
                            new RenameColumnEvent(
                                    cdcTableId, Collections.singletonMap(bc.name(), ac.name())));
                }

                DataType beforeType = PostgresTypeUtils.fromDbzColumn(bc, dbzConfig, typeRegistry);
                DataType afterType = PostgresTypeUtils.fromDbzColumn(ac, dbzConfig, typeRegistry);
                if (!beforeType.equals(afterType)) {
                    events.add(
                            new AlterColumnTypeEvent(
                                    cdcTableId, Collections.singletonMap(ac.name(), afterType)));
                }

                i++;
                j++;
            }
        }

        // Drop remaining before columns
        while (i < n) {
            events.add(
                    new DropColumnEvent(
                            cdcTableId, Collections.singletonList(beforeCols.get(i).name())));
            i++;
        }

        // Add remaining after columns at last. Adds are always trailing because the only
        // available operation is "add column at last" (PostgreSQL ALTER TABLE ADD COLUMN always
        // appends). Surviving before-columns (after drops/renames/alter-types) map to a prefix
        // of the after-columns; the unmatched suffix can only be fulfilled by appending.
        while (j < m) {
            events.add(
                    new AddColumnEvent(
                            cdcTableId,
                            Collections.singletonList(
                                    AddColumnEvent.last(
                                            PostgresSchemaUtils.toColumn(
                                                    afterCols.get(j), dbzConfig, typeRegistry)))));
            j++;
        }

        return events;
    }

    /** Returns memoized cost or computes base case for boundary conditions. */
    private static int memoOrBase(int i, int j, int n, int m, int[][] memo) {
        if (i == n) {
            return m - j;
        }
        if (j == m) {
            return n - i;
        }
        return memo[i][j];
    }
}
