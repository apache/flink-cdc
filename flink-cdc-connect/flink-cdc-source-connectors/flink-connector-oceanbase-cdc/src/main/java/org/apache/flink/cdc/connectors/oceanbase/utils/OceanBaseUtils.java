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

package org.apache.flink.cdc.connectors.oceanbase.utils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** Utils for OceanBase. */
public class OceanBaseUtils {

    public static boolean isOceanBaseDriver(String driverClass) {
        return "com.oceanbase.jdbc.Driver".equals(driverClass)
                || "com.alipay.oceanbase.jdbc.Driver".equals(driverClass);
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = jdbc.quotedColumnIdString(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quotedColumn,
                        quotedColumn,
                        jdbc.quotedTableIdString(tableId),
                        quotedColumn,
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

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        final String useDatabaseStatement = String.format("USE `%s`;", tableId.catalog());
        final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", tableId.table());
        jdbc.executeWithoutCommitting(useDatabaseStatement);
        return jdbc.queryAndMap(
                rowCountQuery,
                rs -> {
                    if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(5);
                });
    }

    public static String buildSplitScanQuery(
            TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, true);
    }

    private static String buildSplitQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int limitSize,
            boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
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

    public static RowType getSplitType(Table table) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
        }

        // use first field in primary key as the split key
        return getSplitType(primaryKeys.get(0));
    }

    public static RowType getSplitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), OceanBaseTypeUtils.fromDbzColumn(splitColumn)))
                        .getLogicalType();
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
            RowType pkRowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next());
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
            sql.append("MAX(" + fieldNamesIt.next() + ")");
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
        sql.append(quotedTableIdString(tableId));
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
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
        sql.append(") T");
        return sql.toString();
    }

    private static String quotedTableIdString(TableId tableId) {
        if (tableId.catalog() != null) {
            return tableId.toQuotedString('`');
        }
        return tableId.toDoubleQuotedString();
    }
}
