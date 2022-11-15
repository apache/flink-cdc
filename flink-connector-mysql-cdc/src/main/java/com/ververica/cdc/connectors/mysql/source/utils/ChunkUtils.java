/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** Utilities to split chunks of table. */
public class ChunkUtils {

    private ChunkUtils() {}

    public static RowType getChunkKeyColumnType(
            Table table, Map<ObjectPath, String> chunkKeyColumns) {
        return getChunkKeyColumnType(getChunkKeyColumn(table, chunkKeyColumns));
    }

    public static RowType getChunkKeyColumnType(Column chunkKeyColumn) {
        return (RowType)
                ROW(FIELD(chunkKeyColumn.name(), MySqlTypeUtils.fromDbzColumn(chunkKeyColumn)))
                        .getLogicalType();
    }

    /**
     * Get the chunk key column. This column could be set by `chunkKeyColumn`. If the table doesn't
     * have primary keys, `chunkKeyColumn` must be set. If the table has primary keys,
     * `chunkKeyColumn` must be a column of them or else null. When the parameter `chunkKeyColumn`
     * is not set and the table has primary keys, return the first column of primary keys.
     */
    public static Column getChunkKeyColumn(Table table, Map<ObjectPath, String> chunkKeyColumns) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        String chunkKeyColumn = findChunkKeyColumn(table.id(), chunkKeyColumns);
        if (primaryKeys.isEmpty() && chunkKeyColumn == null) {
            throw new ValidationException(
                    "'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.");
        }
        List<Column> searchColumns = primaryKeys.isEmpty() ? table.columns() : primaryKeys;
        if (chunkKeyColumn != null) {
            Optional<Column> targetColumn =
                    searchColumns.stream()
                            .filter(col -> chunkKeyColumn.equals(col.name()))
                            .findFirst();
            if (targetColumn.isPresent()) {
                return targetColumn.get();
            }
            throw new ValidationException(
                    String.format(
                            "Chunk key column '%s' doesn't exist in the %s [%s] of the table %s.",
                            chunkKeyColumn,
                            primaryKeys.isEmpty() ? "user specified columns" : "primary keys",
                            searchColumns.stream()
                                    .map(Column::name)
                                    .collect(Collectors.joining(",")),
                            table.id()));
        }

        // use the first column of primary key columns as the chunk key column by default
        return primaryKeys.get(0);
    }

    @Nullable
    private static String findChunkKeyColumn(
            TableId tableId, Map<ObjectPath, String> chunkKeyColumns) {
        for (ObjectPath table : chunkKeyColumns.keySet()) {
            Tables.TableFilter filter =
                    DebeziumUtils.createTableFilter(table.getDatabaseName(), table.getFullName());
            if (filter.isIncluded(tableId)) {
                return chunkKeyColumns.get(table);
            }
        }
        return null;
    }

    /** Returns next meta group id according to received meta number and meta group size. */
    public static int getNextMetaGroupId(int receivedMetaNum, int metaGroupSize) {
        Preconditions.checkState(metaGroupSize > 0);
        return receivedMetaNum / metaGroupSize;
    }
}
