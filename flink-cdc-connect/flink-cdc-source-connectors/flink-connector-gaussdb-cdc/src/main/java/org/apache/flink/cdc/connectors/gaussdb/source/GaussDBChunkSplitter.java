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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.utils.JdbcChunkUtils;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.GaussDBTypeUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/**
 * The splitter to split the table into chunks using primary-key (by default) or a given split key.
 */
@Internal
public class GaussDBChunkSplitter extends JdbcSourceChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBChunkSplitter.class);
    private static final String NO_PRIMARY_KEY_MESSAGE =
            "GaussDB table doesn't have primary keys, skip chunk splitting.";

    public GaussDBChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        super(sourceConfig, dialect, chunkSplitterState);
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) throws Exception {
        // For tables without primary keys, return empty split list instead of failing validation.
        // Note: ongoing splitting must continue to preserve ChunkSplitterState correctness.
        if (hasNextChunk()) {
            return super.generateSplits(tableId);
        }

        try {
            return super.generateSplits(tableId);
        } catch (RuntimeException e) {
            ValidationException validationException = findCause(e, ValidationException.class);
            if (validationException != null
                    && validationException.getMessage() != null
                    && NO_PRIMARY_KEY_MESSAGE.equals(validationException.getMessage())) {
                return Collections.emptyList();
            }
            throw e;
        }
    }

    @Override
    protected Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        if (table.primaryKeyColumns().isEmpty()) {
            throw new ValidationException(NO_PRIMARY_KEY_MESSAGE);
        }
        // Use first column of primary key columns by default, allow overriding by chunkKeyColumn
        // for primary-key tables.
        return JdbcChunkUtils.getSplitColumn(table, chunkKeyColumn);
    }

    @Override
    public Object queryNextChunkMax(
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
                        quoteTable(tableId),
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

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
            throws SQLException {
        final String query =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quoteForMinMax(jdbc, splitColumn),
                        quoteForMinMax(jdbc, splitColumn),
                        quoteTable(tableId));
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

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column splitColumn, Object excludedLowerBound)
            throws SQLException {
        final String quotedColumn = jdbc.quotedColumnIdString(splitColumn.name());
        final String query =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > %s",
                        quoteForMinMax(jdbc, splitColumn),
                        quoteTable(tableId),
                        quotedColumn,
                        castParam(splitColumn));

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

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // Same catalog query pattern as PostgreSQL.
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

    @Override
    protected DataType fromDbzColumn(Column splitColumn) {
        DataType dataType = GaussDBTypeUtils.convertGaussDBType(splitColumn.typeName());
        return splitColumn.isOptional() ? dataType : dataType.notNull();
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static String quoteTable(TableId tableId) {
        return tableId.toQuotedString('"');
    }

    private static String quoteForMinMax(JdbcConnection jdbc, Column column) {
        final String quotedColumn = jdbc.quotedColumnIdString(column.name());
        return isUUID(column) ? castToText(quotedColumn) : quotedColumn;
    }

    private static String castParam(Column column) {
        return isUUID(column) ? castToUuid("?") : "?";
    }

    private static String castToUuid(String value) {
        return String.format("(%s)::uuid", value);
    }

    private static String castToText(String value) {
        return String.format("(%s)::text", value);
    }

    private static boolean isUUID(Column column) {
        // Debezium may return "uuid" or schema-qualified/cased names, normalize it.
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

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T findCause(Throwable throwable, Class<T> causeType) {
        Throwable current = throwable;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return (T) current;
            }
            current = current.getCause();
        }
        return null;
    }
}
