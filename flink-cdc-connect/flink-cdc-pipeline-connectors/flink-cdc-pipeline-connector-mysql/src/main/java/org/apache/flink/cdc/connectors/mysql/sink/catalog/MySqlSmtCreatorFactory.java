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

package org.apache.flink.cdc.connectors.mysql.sink.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.jdbc.catalog.JdbcColumn;
import org.apache.flink.cdc.connectors.mysql.sink.type.MySqlTypeTransformer;

import java.util.List;
import java.util.stream.Collectors;

/** MySQL creator sql statement factory. */
public class MySqlSmtCreatorFactory {
    public static final MySqlSmtCreatorFactory INSTANCE = new MySqlSmtCreatorFactory();

    private static final String RENAME_DDL = "ALTER TABLE %s RENAME COLUMN `%s` TO `%s`;";
    private static final String DROP_COLUMN_DDL = "ALTER TABLE %s DROP COLUMN `%s`;";

    public String buildUpsertSql(TableId tableId, List<Column> columns) {
        String tableName = tableId.identifier();
        // Building column names and value placeholders
        String columnNames =
                columns.stream().map(Column::getName).collect(Collectors.joining(", "));

        String valuePlaceholders =
                columns.stream().map(column -> "?").collect(Collectors.joining(", "));

        // Building the initial insert part
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ")
                .append(tableName)
                .append(" (")
                .append(columnNames)
                .append(") VALUES (")
                .append(valuePlaceholders)
                .append(") ON DUPLICATE KEY UPDATE ");

        // Building the update part
        String updatePart =
                columns.stream()
                        .map(column -> column.getName() + " = VALUES(" + column.getName() + ")")
                        .collect(Collectors.joining(", "));

        query.append(updatePart).append(";");

        return query.toString();
    }

    public String buildDeleteSql(TableId tableId, List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("DELETE FROM %s ", tableId.identifier()));
        builder.append("WHERE ");
        primaryKeys.forEach(
                pk -> {
                    builder.append(pk).append(" = ? AND ");
                });
        // remove latest " AND "
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }

    public String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addedColumns) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE %s ", tableId.identifier()));

        String columnsStmt =
                addedColumns.stream()
                        .map(this::buildAddColumnStmt)
                        .collect(Collectors.joining(", "));

        builder.append(columnsStmt);
        builder.append(";");

        return builder.toString();
    }

    public String buildAlterColumnTypeSql(TableId tableId, String columnName, DataType columnType) {
        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(columnBuilder);
        columnType.accept(transformer);
        JdbcColumn type = columnBuilder.build();

        return String.format(
                "ALTER TABLE %s MODIFY COLUMN %s %s;",
                tableId.identifier(), columnName, type.getColumnType());
    }

    private String buildAddColumnStmt(AddColumnEvent.ColumnWithPosition columnWithPosition) {
        Column column = columnWithPosition.getAddColumn();
        StringBuilder builder = new StringBuilder();

        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(columnBuilder);
        column.getType().accept(transformer);
        JdbcColumn type = columnBuilder.build();

        builder.append("ADD COLUMN `")
                .append(column.getName())
                .append("` ")
                .append(type.getColumnType());

        if (column.getComment() != null) {
            builder.append(" COMMENT '").append(column.getComment()).append("'");
        }

        switch (columnWithPosition.getPosition()) {
            case FIRST:
                builder.append(" FIRST");
                break;
            case AFTER:
                builder.append(" AFTER `")
                        .append(columnWithPosition.getExistedColumnName())
                        .append("`");
                break;
            case BEFORE:
                builder.append(" BEFORE `")
                        .append(columnWithPosition.getExistedColumnName())
                        .append("`");
                break;
            case LAST:
            default:
                break;
        }

        return builder.toString();
    }

    public String buildRenameColumnSql(
            TableId tableId, String oldColumnName, String newColumnName) {
        return String.format(RENAME_DDL, tableId.identifier(), oldColumnName, newColumnName);
    }

    public String buildDropColumnSql(TableId tableId, String columnName) {
        return String.format(DROP_COLUMN_DDL, tableId.identifier(), columnName);
    }

    public String buildCreateTableSql(TableId tableId, Schema schema, boolean ignoreIfExists) {
        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format(
                        "CREATE TABLE %s %s",
                        ignoreIfExists ? "IF NOT EXISTS" : "", tableId.identifier()));
        builder.append(" (\n");
        String columnsStmt =
                schema.getColumns().stream()
                        .map(this::buildColumnStmt)
                        .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);

        if (!schema.primaryKeys().isEmpty()) {
            builder.append(",\n");
            String tableKeys =
                    schema.primaryKeys().stream()
                            .map(key -> "`" + key + "`")
                            .collect(Collectors.joining(", "));
            builder.append(String.format("PRIMARY KEY (%s)", tableKeys));
        }
        builder.append("\n) ");
        builder.append(";");
        return builder.toString();
    }

    public String buildColumnStmt(Column column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getName());
        builder.append("` ");
        // build column type
        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(columnBuilder);
        column.getType().accept(transformer);
        JdbcColumn type = columnBuilder.build();
        builder.append(type.getColumnType());

        if (!StringUtils.isNullOrWhitespaceOnly(column.getComment())) {
            builder.append(String.format(" COMMENT \"%s\"", column.getComment()));
        }
        return builder.toString();
    }
}
