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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/** Query-related Utilities for GaussDB CDC source. */
public class GaussDBQueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBQueryUtils.class);

    private GaussDBQueryUtils() {}

    /**
     * Builds split scan query for snapshot splits.
     *
     * <p>For bounded splits, the query uses the inclusive lower bound and exclusive upper bound:
     *
     * <pre>
     * SELECT * FROM table WHERE pk >= ? AND pk &lt; ? ORDER BY pk
     * </pre>
     */
    public static String buildSplitScanQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            List<String> uuidFields) {
        final List<String> safeUuidFields =
                uuidFields == null ? Collections.emptyList() : uuidFields;
        final String quotedTable = quote(tableId);
        final String orderBy = buildOrderBy(pkRowType);

        if (isFirstSplit && isLastSplit) {
            return String.format("SELECT * FROM %s ORDER BY %s", quotedTable, orderBy);
        }

        final String pkExpr = buildPkExpression(pkRowType);
        final String startParams = buildPkParams(pkRowType, safeUuidFields);
        final String endParams = buildPkParams(pkRowType, safeUuidFields);

        if (isFirstSplit) {
            return String.format(
                    "SELECT * FROM %s WHERE %s < %s ORDER BY %s",
                    quotedTable, pkExpr, endParams, orderBy);
        }
        if (isLastSplit) {
            return String.format(
                    "SELECT * FROM %s WHERE %s >= %s ORDER BY %s",
                    quotedTable, pkExpr, startParams, orderBy);
        }
        return String.format(
                "SELECT * FROM %s WHERE %s >= %s AND %s < %s ORDER BY %s",
                quotedTable, pkExpr, startParams, pkExpr, endParams, orderBy);
    }

    /**
     * Creates a {@link PreparedStatement} for reading one snapshot split, with configured fetch
     * size.
     */
    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }

            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        final String quotedColumn = jdbc.quotedColumnIdString(splitColumn.name());
        final String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= %s ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quoteForMinMax(jdbc, splitColumn),
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        castParam(splitColumn),
                        quotedColumn,
                        chunkSize);
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, includedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        final String query =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quoteForMinMax(jdbc, column), quoteForMinMax(jdbc, column), quote(tableId));
        return jdbc.queryAndMap(
                query,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rowToArray(rs, 2);
                });
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        final String quotedColumn = jdbc.quotedColumnIdString(column.name());
        final String query =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > %s",
                        quoteForMinMax(jdbc, column),
                        quote(tableId),
                        quotedColumn,
                        castParam(column));
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    LOG.debug("{} => {}", query, rs.getObject(1));
                    return rs.getObject(1);
                });
    }

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        final String query =
                "SELECT reltuples::bigint"
                        + " FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = ?"
                        + " AND c.relname = ?";
        return jdbc.prepareQueryAndMap(
                query,
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                },
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getLong(1);
                });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('"');
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private static String buildOrderBy(RowType pkRowType) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String fieldName : pkRowType.getFieldNames()) {
            joiner.add(quote(fieldName));
        }
        return joiner.toString();
    }

    private static String buildPkExpression(RowType pkRowType) {
        if (pkRowType.getFieldCount() == 1) {
            return quote(pkRowType.getFieldNames().get(0));
        }
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (String fieldName : pkRowType.getFieldNames()) {
            joiner.add(quote(fieldName));
        }
        return joiner.toString();
    }

    private static String buildPkParams(RowType pkRowType, List<String> uuidFields) {
        if (pkRowType.getFieldCount() == 1) {
            String fieldName = pkRowType.getFieldNames().get(0);
            return castParam(uuidFields.contains(fieldName));
        }
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (String fieldName : pkRowType.getFieldNames()) {
            joiner.add(castParam(uuidFields.contains(fieldName)));
        }
        return joiner.toString();
    }

    private static String quoteForMinMax(JdbcConnection jdbc, Column column) {
        final String quotedColumn = jdbc.quotedColumnIdString(column.name());
        return isUUID(column) ? castToText(quotedColumn) : quotedColumn;
    }

    private static String castParam(Column column) {
        return castParam(isUUID(column));
    }

    private static String castParam(boolean isUUID) {
        return isUUID ? castToUuid("?") : "?";
    }

    private static String castToUuid(String value) {
        return String.format("(%s)::uuid", value);
    }

    private static String castToText(String value) {
        return String.format("(%s)::text", value);
    }

    private static boolean isUUID(Column column) {
        String typeName = column.typeName();
        if (typeName == null) {
            return false;
        }
        String normalized = typeName.trim();
        if ((normalized.startsWith("\"") && normalized.endsWith("\""))
                || (normalized.startsWith("'") && normalized.endsWith("'"))) {
            normalized = normalized.substring(1, normalized.length() - 1).trim();
        }
        int lastDot = normalized.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < normalized.length() - 1) {
            normalized = normalized.substring(lastDot + 1);
        }
        normalized = normalized.toLowerCase(Locale.ROOT);
        return "uuid".equals(normalized);
    }
}
