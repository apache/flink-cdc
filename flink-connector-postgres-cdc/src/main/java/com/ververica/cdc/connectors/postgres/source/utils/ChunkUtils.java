package com.ververica.cdc.connectors.postgres.source.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
}
