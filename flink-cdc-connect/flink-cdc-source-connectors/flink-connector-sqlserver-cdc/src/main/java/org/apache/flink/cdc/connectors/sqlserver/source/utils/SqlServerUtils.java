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

package org.apache.flink.cdc.connectors.sqlserver.source.utils;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnOffset;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerTopicSelector;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** The utils for SqlServer data source. */
public class SqlServerUtils {

    public SqlServerUtils() {}

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        final String useDatabaseStatement = String.format("USE %s;", quote(tableId.catalog()));
        final String rowCountQuery =
                String.format(
                        "SELECT Total_Rows = SUM(st.row_count) FROM sys"
                                + ".dm_db_partition_stats st WHERE object_name(object_id) = '%s' AND index_id < 2;",
                        tableId.table());
        jdbc.executeWithoutCommitting(useDatabaseStatement);
        return jdbc.queryAndMap(
                rowCountQuery,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(1);
                });
    }

    /**
     * Returns the next LSN to be read from the database. This is the LSN of the last record that
     * was read from the database.
     */
    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT TOP (%s) %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                + ") AS T",
                        quotedColumn,
                        chunkSize,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        quotedColumn);
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

    public static Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
        }

        if (chunkKeyColumn != null) {
            Optional<Column> targetPkColumn =
                    primaryKeys.stream()
                            .filter(col -> chunkKeyColumn.equals(col.name()))
                            .findFirst();
            if (targetPkColumn.isPresent()) {
                return targetPkColumn.get();
            }
            throw new ValidationException(
                    String.format(
                            "Chunk key column '%s' doesn't exist in the primary key [%s] of the table %s.",
                            chunkKeyColumn,
                            primaryKeys.stream().map(Column::name).collect(Collectors.joining(",")),
                            table.id()));
        }

        // use first field in primary key as the split key
        return primaryKeys.get(0);
    }

    public static RowType getSplitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), SqlServerTypeUtils.fromDbzColumn(splitColumn)))
                        .getLogicalType();
    }

    public static Offset getLsn(SourceRecord record) {
        return getLsnPosition(record.sourceOffset());
    }

    public static LsnOffset getLsnPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        Lsn changeLsn = Lsn.valueOf(offsetStrMap.get(SourceInfo.CHANGE_LSN_KEY));
        Lsn commitLsn = Lsn.valueOf(offsetStrMap.get(SourceInfo.COMMIT_LSN_KEY));
        return new LsnOffset(changeLsn, commitLsn, null);
    }

    /** Fetch current largest log sequence number (LSN) of the database. */
    public static LsnOffset currentLsn(SqlServerConnection connection) {
        try {
            Lsn maxLsn = connection.getMaxTransactionLsn(connection.database());
            return new LsnOffset(maxLsn, maxLsn, null);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    /** Get split scan query for the given table. */
    public static String buildSplitScanQuery(
            TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, true);
    }

    /** Get table split data PreparedStatement. */
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

    public static SqlServerDatabaseSchema createSqlServerDatabaseSchema(
            SqlServerConnectorConfig connectorConfig, SqlServerConnection connection) {
        TopicSelector<TableId> topicSelector =
                SqlServerTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        SqlServerValueConverters valueConverters =
                new SqlServerValueConverters(
                        connectorConfig.getDecimalMode(),
                        connectorConfig.getTemporalPrecisionMode(),
                        connectorConfig.binaryHandlingMode());

        return new SqlServerDatabaseSchema(
                connectorConfig,
                connection.getDefaultValueConverter(),
                valueConverters,
                topicSelector,
                schemaNameAdjuster);
    }

    // --------------------------private method-------------------------------

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
            final String orderBy = String.join(", ", pkRowType.getFieldNames());
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(pkRowType),
                    getMaxPrimaryKeyColumnsProjection(pkRowType),
                    Optional.ofNullable(condition),
                    orderBy);
        }
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private static String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append("MAX(").append(fieldNamesIt.next()).append(")");
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
        if (limit > 0) {
            sql.append(" TOP( ").append(limit).append(") ");
        }
        sql.append(projection).append(" FROM ");
        sql.append(quoteSchemaAndTable(tableId));
        condition.ifPresent(s -> sql.append(" WHERE ").append(s));
        orderBy.ifPresent(s -> sql.append(" ORDER BY ").append(s));
        return sql.toString();
    }

    private static String quoteSchemaAndTable(TableId tableId) {
        StringBuilder quoted = new StringBuilder();

        if (tableId.schema() != null && !tableId.schema().isEmpty()) {
            quoted.append(quote(tableId.schema())).append(".");
        }

        quoted.append(quote(tableId.table()));
        return quoted.toString();
    }

    /**
     * @link <a
     *     href="https://learn.microsoft.com/en-us/sql/t-sql/functions/quotename-transact-sql">QUOTENAME</a>
     */
    public static String quote(String name) {
        return "[" + name.replace("]", "]]") + "]";
    }

    public static String quote(TableId tableId) {
        return String.format("%s.%s", quote(tableId.schema()), quote(tableId.table()));
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
        sql.append(" TOP( ").append(limit).append(") ");
        sql.append(projection);
        sql.append(" FROM ");
        sql.append(quoteSchemaAndTable(tableId));
        condition.ifPresent(s -> sql.append(" WHERE ").append(s));
        sql.append(" ORDER BY ").append(orderBy);
        sql.append(") T");
        return sql.toString();
    }

    public static int compare(Object obj1, Object obj2, Column splitColumn) {
        if (splitColumn.typeName().equals(SqlServerTypeUtils.UNIQUEIDENTIFIRER)) {
            return new SQLServerUUIDComparator()
                    .compare(UUID.fromString(obj1.toString()), UUID.fromString(obj2.toString()));
        }
        return ObjectUtils.compare(obj1, obj2);
    }

    /**
     * Comparator for SQL Server UUIDs. SQL Server compares UUIDs in a different order than Java.
     * Reference code: <a
     * href="https://github.com/dotnet/runtime/blob/5535e31a712343a63f5d7d796cd874e563e5ac14/src/libraries/System.Data.Common/src/System/Data/SQLTypes/SQLGuid.cs#L113">SQLGuid.cs::CompareTo</a>
     * Reference doc: <a
     * href="https://learn.microsoft.com/uk-ua/sql/connect/ado-net/sql/compare-guid-uniqueidentifier-values?view=sql-server-ver16">Comparing
     * GUID and uniqueidentifier values</a>
     */
    static class SQLServerUUIDComparator implements Comparator<UUID> {

        private static final int SIZE_OF_GUID = 16;
        private static final byte[] GUID_ORDER = {
            10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3
        };

        public int compare(UUID uuid1, UUID uuid2) {
            byte[] bytes1 = uuidToBytes(uuid1);
            byte[] bytes2 = uuidToBytes(uuid2);

            for (int i = 0; i < SIZE_OF_GUID; i++) {
                byte b1 = bytes1[GUID_ORDER[i]];
                byte b2 = bytes2[GUID_ORDER[i]];
                if (b1 != b2) {
                    return (b1 & 0xFF) - (b2 & 0xFF); // Unsigned byte comparison
                }
            }
            return 0;
        }

        private byte[] uuidToBytes(UUID uuid) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        }
    }
}
