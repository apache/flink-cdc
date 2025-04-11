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

package org.apache.flink.cdc.connectors.base.source.utils;

import org.apache.flink.table.api.ValidationException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/** Utilities to split chunks of table. */
public class JdbcChunkUtils {

    /**
     * Query the maximum and minimum value of the column in the table. e.g. query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param quotedTableName table identity.
     * @param quotedColumnName column name.
     * @return maximum and minimum value.
     */
    public static Object[] queryMinMax(
            JdbcConnection jdbc, String quotedTableName, String quotedColumnName)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quotedColumnName, quotedColumnName, quotedTableName);
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

    /**
     * Query the minimum value of the column in the table, and the minimum value must greater than
     * the excludedLowerBound value. e.g. prepare query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param quotedTableName table identity.
     * @param quotedColumnName column name.
     * @param excludedLowerBound the minimum value should be greater than this value.
     * @return minimum value.
     */
    public static Object queryMin(
            JdbcConnection jdbc,
            String quotedTableName,
            String quotedColumnName,
            Object excludedLowerBound)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quotedColumnName, quotedTableName, quotedColumnName);
        return jdbc.prepareQueryAndMap(
                minQuery,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        // this should never happen
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", minQuery));
                    }
                    return rs.getObject(1);
                });
    }

    /**
     * Get the column which is seen as chunk key.
     *
     * @param table table identity.
     * @param chunkKeyColumn column name which is seen as chunk key, if chunkKeyColumn is null, use
     *     primary key instead. @Column the column which is seen as chunk key.
     */
    public static Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty() && chunkKeyColumn == null) {
            throw new ValidationException(
                    "To use incremental snapshot, 'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.");
        }

        List<Column> searchColumns = table.columns();
        if (chunkKeyColumn != null) {
            Optional<Column> targetPkColumn =
                    searchColumns.stream()
                            .filter(col -> chunkKeyColumn.equals(col.name()))
                            .findFirst();
            if (targetPkColumn.isPresent()) {
                return targetPkColumn.get();
            }
            throw new ValidationException(
                    String.format(
                            "Chunk key column '%s' doesn't exist in the columns [%s] of the table %s.",
                            chunkKeyColumn,
                            searchColumns.stream()
                                    .map(Column::name)
                                    .collect(Collectors.joining(",")),
                            table.id()));
        }

        // use first column of primary key columns as the chunk key column by default
        return primaryKeys.get(0);
    }
}
