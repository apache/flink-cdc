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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTypeUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** Utilities to split chunks of table. */
public class ChunkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkUtils.class);

    private ChunkUtils() {}

    public static RowType getChunkKeyColumnType(
            Table table,
            Map<ObjectPath, String> chunkKeyColumns,
            boolean tinyInt1isBit,
            boolean ignoreNoPrimaryKeyTable) {
        return getChunkKeyColumnType(
                getChunkKeyColumn(table, chunkKeyColumns, ignoreNoPrimaryKeyTable), tinyInt1isBit);
    }

    public static RowType getChunkKeyColumnType(Column chunkKeyColumn, boolean tinyInt1isBit) {
        return (RowType)
                ROW(FIELD(
                                chunkKeyColumn.name(),
                                MySqlTypeUtils.fromDbzColumn(chunkKeyColumn, tinyInt1isBit)))
                        .getLogicalType();
    }

    /**
     * Get the chunk key column. This column could be set by `chunkKeyColumn`. If the table doesn't
     * have primary keys, `chunkKeyColumn` must be set. When the parameter `chunkKeyColumn` is not
     * set and the table has primary keys, return the first column of primary keys.
     */
    public static Column getChunkKeyColumn(
            Table table, Map<ObjectPath, String> chunkKeyColumns, boolean ignoreNoPrimaryKeyTable) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        String chunkKeyColumn = findChunkKeyColumn(table.id(), chunkKeyColumns);
        if (primaryKeys.isEmpty() && chunkKeyColumn == null) {
            if (ignoreNoPrimaryKeyTable) {
                LOG.warn(
                        "Table {} has no primary key and no chunk key column specified. This table will be skipped.",
                        table.id());
                return null;
            } else {
                throw new ValidationException(
                        String.format(
                                "Table %s has no primary key and no chunk key column specified. "
                                        + "To use incremental snapshot, either: "
                                        + "1. Set 'scan.incremental.snapshot.chunk.key-column' for this table, or "
                                        + "2. Set 'scan.incremental.snapshot.ignore-no-primary-key-table' to true to skip tables without primary keys.",
                                table.id()));
            }
        }

        List<Column> searchColumns = table.columns();
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
                            "Chunk key column '%s' doesn't exist in the columns [%s] of the table %s.",
                            chunkKeyColumn,
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
