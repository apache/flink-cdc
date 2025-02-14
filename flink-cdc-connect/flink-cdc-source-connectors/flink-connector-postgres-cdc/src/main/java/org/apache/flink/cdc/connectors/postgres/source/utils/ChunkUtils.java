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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities to split chunks of table. */
public class ChunkUtils {
    public static RowType getSplitType(Column splitColumn) {
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        splitColumn.name(),
                                        PostgresTypeUtils.fromDbzColumn(splitColumn)))
                        .getLogicalType();
    }

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
