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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parser for Canal DDL messages that converts SQL statements into Flink CDC SchemaChangeEvents.
 *
 * <p>Canal DDL messages have the following format:
 *
 * <pre>{@code
 * {
 *   "isDdl": true,
 *   "sql": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
 *   "type": "CREATE",
 *   "database": "mydb",
 *   "table": "users"
 * }
 * }</pre>
 */
public class CanalDdlParser {

    private static final Logger LOG = LoggerFactory.getLogger(CanalDdlParser.class);

    private static final Pattern DROP_TABLE_PATTERN =
            Pattern.compile(
                    "DROP\\s+TABLE\\s+(IF\\s+EXISTS\\s+)?([^\\s;]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRUNCATE_TABLE_PATTERN =
            Pattern.compile("TRUNCATE\\s+(TABLE\\s+)?([^\\s;]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern RENAME_TABLE_PATTERN =
            Pattern.compile(
                    "RENAME\\s+TABLE\\s+([^\\s]+)\\s+TO\\s+([^\\s;]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_TABLE_PATTERN =
            Pattern.compile(
                    "CREATE\\s+TABLE\\s+(IF\\s+NOT\\s+EXISTS\\s+)?([^\\s(]+)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern ALTER_TABLE_PATTERN =
            Pattern.compile("ALTER\\s+TABLE\\s+([^\\s]+)", Pattern.CASE_INSENSITIVE);

    private final LinkedList<SchemaChangeEvent> parsedEvents = new LinkedList<>();

    /** Parse the SQL statement and return the corresponding SchemaChangeEvents. */
    public List<SchemaChangeEvent> parse(
            String sql, String database, String table, @Nullable String schemaName) {
        parsedEvents.clear();

        String trimmedSql = sql.trim().replaceAll(";$", "");

        if (trimmedSql.isEmpty()) {
            LOG.warn("Empty SQL statement, skipping.");
            return parsedEvents;
        }

        String upperSql = trimmedSql.toUpperCase();

        if (upperSql.startsWith("DROP TABLE")) {
            parseDropTable(trimmedSql, database, table, schemaName);
        } else if (upperSql.startsWith("TRUNCATE")) {
            parseTruncateTable(trimmedSql, database, table, schemaName);
        } else if (upperSql.startsWith("RENAME TABLE")) {
            parseRenameTable(trimmedSql, database, schemaName);
        } else if (upperSql.startsWith("CREATE TABLE")) {
            parseCreateTable(trimmedSql, database, table, schemaName);
        } else if (upperSql.startsWith("ALTER TABLE")) {
            parseAlterTable(trimmedSql, database, table, schemaName);
        } else {
            LOG.warn(
                    "Unsupported DDL type: {}",
                    trimmedSql.substring(0, Math.min(50, trimmedSql.length())));
        }

        return parsedEvents;
    }

    private TableId createTableId(String database, String table, @Nullable String schemaName) {
        if (schemaName != null && !schemaName.isEmpty()) {
            return TableId.tableId(database, schemaName, table);
        } else if (database != null && !database.isEmpty()) {
            return TableId.tableId(database, table);
        } else {
            return TableId.tableId(table);
        }
    }

    private void parseDropTable(
            String sql, String database, String table, @Nullable String schemaName) {
        Matcher matcher = DROP_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            String tableName = extractTableName(matcher.group(2));
            TableId tableId = createTableId(database, tableName, schemaName);
            parsedEvents.add(new DropTableEvent(tableId));
        } else {
            TableId tableId = createTableId(database, table, schemaName);
            parsedEvents.add(new DropTableEvent(tableId));
        }
    }

    private void parseTruncateTable(
            String sql, String database, String table, @Nullable String schemaName) {
        Matcher matcher = TRUNCATE_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            String tableName = extractTableName(matcher.group(2));
            if (tableName == null || tableName.isEmpty()) {
                tableName = extractTableName(matcher.group(1));
            }
            TableId tableId =
                    createTableId(database, tableName != null ? tableName : table, schemaName);
            parsedEvents.add(new TruncateTableEvent(tableId));
        }
    }

    private void parseRenameTable(String sql, String database, @Nullable String schemaName) {
        Matcher matcher = RENAME_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            String oldTableName = extractTableName(matcher.group(1));
            String newTableName = extractTableName(matcher.group(2));
            TableId oldTableId = createTableId(database, oldTableName, schemaName);
            TableId newTableId = createTableId(database, newTableName, schemaName);
            parsedEvents.add(new DropTableEvent(oldTableId));
            parsedEvents.add(new CreateTableEvent(newTableId, Schema.newBuilder().build()));
        }
    }

    private void parseCreateTable(
            String sql, String database, String table, @Nullable String schemaName) {
        Matcher matcher = CREATE_TABLE_PATTERN.matcher(sql);
        String tableName = table;
        if (matcher.find()) {
            tableName = extractTableName(matcher.group(2));
        }

        TableId tableId = createTableId(database, tableName, schemaName);
        Schema schema = parseCreateTableSchema(sql);

        parsedEvents.add(new CreateTableEvent(tableId, schema));
    }

    private Schema parseCreateTableSchema(String sql) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        int startParen = sql.indexOf('(');
        int endParen = findMatchingParen(sql, startParen);

        if (startParen == -1 || endParen == -1) {
            return schemaBuilder.build();
        }

        String columnsStr = sql.substring(startParen + 1, endParen);
        List<String> columnDefinitions = parseCommaSeparated(columnsStr);

        List<String> primaryKeys = new ArrayList<>();

        for (String colDef : columnDefinitions) {
            colDef = colDef.trim();
            if (colDef.isEmpty()) {
                continue;
            }

            if (colDef.toUpperCase().startsWith("PRIMARY KEY")) {
                Matcher pkMatcher =
                        Pattern.compile("PRIMARY\\s+KEY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE)
                                .matcher(colDef);
                if (pkMatcher.find()) {
                    String pkCols = pkMatcher.group(1);
                    for (String pkCol : pkCols.split(",")) {
                        primaryKeys.add(pkCol.trim());
                    }
                }
                continue;
            }

            ColumnWithPk columnWithPk = parseColumnDefinitionWithPk(colDef);
            if (columnWithPk != null && columnWithPk.column != null) {
                schemaBuilder.physicalColumn(
                        columnWithPk.column.getName(), columnWithPk.column.getType());
                if (columnWithPk.isPrimaryKey) {
                    primaryKeys.add(columnWithPk.column.getName());
                }
            }
        }

        if (!primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(primaryKeys);
        }

        return schemaBuilder.build();
    }

    private static class ColumnWithPk {
        final Column column;
        final boolean isPrimaryKey;

        ColumnWithPk(Column column, boolean isPrimaryKey) {
            this.column = column;
            this.isPrimaryKey = isPrimaryKey;
        }
    }

    private ColumnWithPk parseColumnDefinitionWithPk(String colDef) {
        try {
            String upperDef = colDef.toUpperCase();
            boolean isPrimaryKey = upperDef.contains("PRIMARY KEY");

            String[] parts = colDef.split("\\s+", 3);
            if (parts.length < 2) {
                return null;
            }

            String columnName = parts[0];
            String typeStr = parts[1].toUpperCase();

            DataType dataType = parseDataType(typeStr);
            if (dataType == null) {
                return null;
            }

            return new ColumnWithPk(new PhysicalColumn(columnName, dataType, null), isPrimaryKey);
        } catch (Exception e) {
            LOG.debug("Failed to parse column definition '{}': {}", colDef, e.getMessage());
            return null;
        }
    }

    private Column parseColumnDefinition(String colDef) {
        try {
            String[] parts = colDef.split("\\s+", 3);
            if (parts.length < 2) {
                return null;
            }

            String columnName = parts[0];
            String typeStr = parts[1].toUpperCase();

            DataType dataType = parseDataType(typeStr);
            if (dataType == null) {
                return null;
            }

            return new PhysicalColumn(columnName, dataType, null);
        } catch (Exception e) {
            LOG.debug("Failed to parse column definition '{}': {}", colDef, e.getMessage());
            return null;
        }
    }

    private DataType parseDataType(String typeStr) {
        Matcher matcher =
                Pattern.compile("^([A-Za-z]+)(\\((\\d+)(,\\s*(\\d+))?\\))?$").matcher(typeStr);
        if (!matcher.matches()) {
            LOG.debug("Cannot parse data type: {}", typeStr);
            return null;
        }

        String typeName = matcher.group(1).toUpperCase();
        int precision = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
        int scale = matcher.group(5) != null ? Integer.parseInt(matcher.group(5)) : 0;

        return mapMySqlType(typeName, precision, scale);
    }

    private void parseAlterTable(
            String sql, String database, String table, @Nullable String schemaName) {
        Matcher matcher = ALTER_TABLE_PATTERN.matcher(sql);
        String tableName = table;
        if (matcher.find()) {
            tableName = extractTableName(matcher.group(1));
        }

        TableId tableId = createTableId(database, tableName, schemaName);

        String upperSql = sql.toUpperCase();

        if (upperSql.contains("DROP COLUMN")) {
            parseDropColumns(sql, tableId);
        }

        if (upperSql.contains("ADD COLUMN")) {
            parseAddColumns(sql, tableId);
        }

        if (upperSql.contains("MODIFY COLUMN") || upperSql.contains("CHANGE COLUMN")) {
            parseAlterColumnType(sql, tableId);
        }

        if (upperSql.contains("RENAME COLUMN")) {
            parseRenameColumn(sql, tableId);
        }
    }

    private void parseDropColumns(String sql, TableId tableId) {
        List<String> droppedColumns = new ArrayList<>();
        Matcher matcher =
                Pattern.compile("DROP\\s+COLUMN\\s+([^,\\s;]+)", Pattern.CASE_INSENSITIVE)
                        .matcher(sql);
        while (matcher.find()) {
            droppedColumns.add(matcher.group(1));
        }
        if (!droppedColumns.isEmpty()) {
            parsedEvents.add(new DropColumnEvent(tableId, droppedColumns));
        }
    }

    private void parseAddColumns(String sql, TableId tableId) {
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        Pattern pattern = Pattern.compile("ADD\\s+COLUMN\\s+([^,;]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String colDef = matcher.group(1).trim();
            Column column = parseColumnDefinition(colDef);
            if (column != null) {
                addedColumns.add(AddColumnEvent.last((PhysicalColumn) column));
            }
        }

        if (!addedColumns.isEmpty()) {
            parsedEvents.add(new AddColumnEvent(tableId, addedColumns));
        }
    }

    private void parseAlterColumnType(String sql, TableId tableId) {
        Map<String, DataType> typeMapping = new HashMap<>();

        Pattern modifyPattern =
                Pattern.compile(
                        "MODIFY\\s+COLUMN\\s+([^\\s]+)\\s+([^,;\\s]+)", Pattern.CASE_INSENSITIVE);
        Matcher modifyMatcher = modifyPattern.matcher(sql);
        while (modifyMatcher.find()) {
            String colName = modifyMatcher.group(1);
            String typeStr = modifyMatcher.group(2);
            DataType dataType = parseDataType(typeStr);
            if (dataType != null) {
                typeMapping.put(colName, dataType);
            }
        }

        Pattern changePattern =
                Pattern.compile(
                        "CHANGE\\s+COLUMN\\s+[^\\s]+\\s+([^\\s]+)\\s+([^,;\\s]+)",
                        Pattern.CASE_INSENSITIVE);
        Matcher changeMatcher = changePattern.matcher(sql);
        while (changeMatcher.find()) {
            String colName = changeMatcher.group(1);
            String typeStr = changeMatcher.group(2);
            DataType dataType = parseDataType(typeStr);
            if (dataType != null) {
                typeMapping.put(colName, dataType);
            }
        }

        if (!typeMapping.isEmpty()) {
            parsedEvents.add(new AlterColumnTypeEvent(tableId, typeMapping));
        }
    }

    private void parseRenameColumn(String sql, TableId tableId) {
        Map<String, String> nameMapping = new HashMap<>();
        Pattern pattern =
                Pattern.compile(
                        "RENAME\\s+COLUMN\\s+([^\\s]+)\\s+TO\\s+([^\\s;]+)",
                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String oldName = matcher.group(1);
            String newName = matcher.group(2);
            nameMapping.put(oldName, newName);
        }

        if (!nameMapping.isEmpty()) {
            parsedEvents.add(new RenameColumnEvent(tableId, nameMapping));
        }
    }

    private int findMatchingParen(String str, int start) {
        int depth = 1;
        for (int i = start + 1; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            } else if (c == '\'' || c == '"') {
                i = skipQuotedString(str, i);
            }
        }
        return -1;
    }

    private int skipQuotedString(String str, int start) {
        char quote = str.charAt(start);
        for (int i = start + 1; i < str.length(); i++) {
            if (str.charAt(i) == quote && str.charAt(i - 1) != '\\') {
                return i;
            }
        }
        return str.length() - 1;
    }

    private List<String> parseCommaSeparated(String str) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parenDepth = 0;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') {
                parenDepth++;
                current.append(c);
            } else if (c == ')') {
                parenDepth--;
                current.append(c);
            } else if (c == ',' && parenDepth == 0) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            result.add(current.toString());
        }

        return result;
    }

    private String extractTableName(String str) {
        if (str == null) {
            return null;
        }
        str = str.trim();
        if (str.startsWith("`") && str.endsWith("`")) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    private static DataType mapMySqlType(String typeName, int precision, int scale) {
        switch (typeName.toUpperCase()) {
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(precision > 0 ? precision : 10, scale);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME(3);
            case "DATETIME":
                return DataTypes.TIMESTAMP(3);
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP(3);
            case "CHAR":
                return DataTypes.CHAR(precision > 0 ? precision : 1);
            case "VARCHAR":
                return DataTypes.VARCHAR(precision > 0 ? precision : 255);
            case "TEXT":
            case "LONGTEXT":
            case "MEDIUMTEXT":
            case "TINYTEXT":
                return DataTypes.STRING();
            case "BLOB":
            case "LONGBLOB":
            case "MEDIUMBLOB":
            case "TINYBLOB":
                return DataTypes.BYTES();
            case "BOOLEAN":
            case "BIT":
                return DataTypes.BOOLEAN();
            case "ENUM":
            case "SET":
                return DataTypes.STRING();
            case "JSON":
                return DataTypes.STRING();
            default:
                LOG.warn("Unknown MySQL type '{}', defaulting to STRING", typeName);
                return DataTypes.STRING();
        }
    }
}
