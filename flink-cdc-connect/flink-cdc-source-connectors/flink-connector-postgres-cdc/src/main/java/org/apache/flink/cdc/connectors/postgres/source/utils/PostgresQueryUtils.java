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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/** Query-related Utilities for Postgres CDC source. */
public class PostgresQueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresQueryUtils.class);

    private PostgresQueryUtils() {}

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quoteForMinMax(column), quoteForMinMax(column), quote(tableId));
        return jdbc.queryAndMap(
                minMaxQuery,
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        minMaxQuery));
                    }
                    return rowToArray(rs, 2);
                });
    }

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        // https://stackoverflow.com/questions/7943233/fast-way-to-discover-the-row-count-of-a-table-in-postgresql
        // NOTE: it requires ANALYZE or VACUUM to be run first in PostgreSQL.
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
                    LOG.info("queryApproximateRowCnt: {} => {}", query, rs.getLong(1));
                    return rs.getLong(1);
                });
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        final String query =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > %s",
                        quoteForMinMax(column),
                        quote(tableId),
                        quote(column.name()),
                        castParam(column));
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    LOG.info("{} => {}", query, rs.getObject(1));
                    return rs.getObject(1);
                });
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumn.name());
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= %s ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quoteForMinMax(splitColumn),
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
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    public static String buildSplitScanQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            List<String> uuidFields) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, uuidFields, -1, true);
    }

    private static String buildSplitQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            List<String> uuidFields,
            int limitSize,
            boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ", uuidFields);
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ", uuidFields);
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ", uuidFields);
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ", uuidFields);
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ", uuidFields);
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ", uuidFields);
            condition = sql.toString();
        }

        if (isScanningData) {
            return buildSelectWithRowLimits(
                    tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
        } else {
            final String orderBy =
                    pkRowType.getFieldNames().stream().collect(Collectors.joining(", "));
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(pkRowType),
                    getMaxPrimaryKeyColumnsProjection(pkRowType),
                    Optional.ofNullable(condition),
                    orderBy);
        }
    }

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
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    public static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String quoteForMinMax(Column column) {
        String quoteColumn = quote(column.name());
        return isUUID(column) ? castToText(quoteColumn) : quoteColumn;
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

    public static boolean isUUID(Column column) {
        return column.typeName().equals("uuid");
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

    private static void addPrimaryKeyColumnsToCondition(
            RowType pkRowType, StringBuilder sql, String predicate, List<String> uuidFields) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            String fieldName = fieldNamesIt.next();
            boolean isUUID = uuidFields.contains(fieldName);
            sql.append(quote(fieldName)).append(predicate).append(castParam(isUUID));
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(quote(fieldNamesIt.next()));
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append("MAX(" + quote(fieldNamesIt.next()) + ")");
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String buildSelectWithRowLimits(
            TableId tableId,
            int limit,
            String projection,
            Optional<String> condition,
            Optional<String> orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quote(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        if (orderBy.isPresent()) {
            sql.append(" ORDER BY ").append(orderBy.get());
        }
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }
        return sql.toString();
    }

    private static String buildSelectWithBoundaryRowLimits(
            TableId tableId,
            int limit,
            String projection,
            String maxColumnProjection,
            Optional<String> condition,
            String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(maxColumnProjection);
        sql.append(" FROM (");
        sql.append("SELECT ");
        sql.append(projection);
        sql.append(" FROM ");
        sql.append(quote(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
        sql.append(") T");
        return sql.toString();
    }
}
