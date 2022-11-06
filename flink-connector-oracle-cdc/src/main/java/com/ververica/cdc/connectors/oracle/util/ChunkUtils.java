/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oracle.util;

import org.apache.flink.table.api.ValidationException;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import oracle.sql.ROWID;

import javax.annotation.Nullable;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities to split chunks of table. */
public class ChunkUtils {

    private ChunkUtils() {}

    public static Column getChunkKeyColumn(Table table, @Nullable String chunkKeyColumn) {
        List<Column> primaryKeys = table.primaryKeyColumns();

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

        // Use the ROWID column as the chunk key column by default for oracle cdc connector
        return Column.editor().jdbcType(Types.VARCHAR).name(ROWID.class.getSimpleName()).create();
    }
}
