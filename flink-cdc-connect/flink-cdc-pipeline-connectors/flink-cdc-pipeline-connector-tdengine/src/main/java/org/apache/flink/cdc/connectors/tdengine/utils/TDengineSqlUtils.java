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

package org.apache.flink.cdc.connectors.tdengine.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.tdengine.serde.TDengineRowData;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/** SQL builder for TDengine. */
public class TDengineSqlUtils {

    private TDengineSqlUtils() {}

    public static TDengineTableInfo resolveTableInfo(
            TableId tableId, TDengineDataSinkConfig config) {
        String stable =
                config.getStableMappings()
                        .getOrDefault(
                                tableId,
                                config.getStableName().isEmpty()
                                        ? deriveStableName(tableId, config)
                                        : config.getStableName());
        return new TDengineTableInfo(config.getDatabaseName(), stable);
    }

    public static String createDatabaseSql(TDengineDataSinkConfig config) {
        return "CREATE DATABASE IF NOT EXISTS "
                + TDengineNameUtils.quoteIdentifier(config.getDatabaseName());
    }

    public static String createStableSql(
            TDengineTableInfo tableInfo, Schema schema, TDengineDataSinkConfig config) {
        StringJoiner columns = new StringJoiner(", ");
        String timestampField = config.getTimestampField();
        String subtableField = config.getSubtableField();
        columns.add(
                TDengineNameUtils.quoteIdentifier(timestampField)
                        + " "
                        + TDengineTypeUtils.toTDengineType(
                                requireColumn(schema, timestampField).getType(), true, config));
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()
                    || column.getName().equals(timestampField)
                    || column.getName().equals(subtableField)
                    || config.getTagFields().contains(column.getName())) {
                continue;
            }
            columns.add(
                    TDengineNameUtils.quoteIdentifier(column.getName())
                            + " "
                            + TDengineTypeUtils.toTDengineType(column.getType(), false, config));
        }

        StringJoiner tags = new StringJoiner(", ");
        for (String tagField : config.getTagFields()) {
            Column column = requireColumn(schema, tagField);
            tags.add(
                    TDengineNameUtils.quoteIdentifier(tagField)
                            + " "
                            + TDengineTypeUtils.toTDengineType(column.getType(), false, config));
        }

        String tagClause = tags.length() == 0 ? "TAGS ()" : "TAGS (" + tags + ")";
        return "CREATE STABLE IF NOT EXISTS "
                + tableInfo.qualifiedStableName()
                + " ("
                + columns
                + ") "
                + tagClause;
    }

    public static String addColumnSql(
            TDengineTableInfo tableInfo, Column column, TDengineDataSinkConfig config) {
        return "ALTER STABLE "
                + tableInfo.qualifiedStableName()
                + " ADD COLUMN "
                + TDengineNameUtils.quoteIdentifier(column.getName())
                + " "
                + TDengineTypeUtils.toTDengineType(column.getType(), false, config);
    }

    public static String insertSql(TDengineTableInfo tableInfo, List<TDengineRowData> rows) {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                builder.append(' ');
            }
            builder.append(insertFragment(tableInfo, rows.get(i)));
        }
        return builder.toString();
    }

    public static int estimateInsertSqlBytes(TDengineTableInfo tableInfo, TDengineRowData row) {
        return "INSERT INTO ".length()
                + insertFragment(tableInfo, row).getBytes(StandardCharsets.UTF_8).length
                + 1;
    }

    public static String sqlLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return "0x" + toHex((byte[]) value);
        }
        return "'" + value.toString().replace("\\", "\\\\").replace("'", "''") + "'";
    }

    public static Column requireColumn(Schema schema, String columnName) {
        return schema.getColumn(columnName)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Column " + columnName + " is absent from schema."));
    }

    private static String deriveStableName(TableId tableId, TDengineDataSinkConfig config) {
        String raw = tableId.toString();
        return config.isNameNormalizeEnabled()
                ? TDengineNameUtils.normalizeIdentifier(raw, "stable")
                : TDengineNameUtils.validateIdentifier(raw, "stable");
    }

    private static String joinIdentifiers(List<String> identifiers) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String identifier : identifiers) {
            joiner.add(TDengineNameUtils.quoteIdentifier(identifier));
        }
        return joiner.toString();
    }

    private static String joinLiterals(List<Object> values) {
        StringJoiner joiner = new StringJoiner(", ");
        for (Object value : values) {
            joiner.add(sqlLiteral(value));
        }
        return joiner.toString();
    }

    private static String insertFragment(TDengineTableInfo tableInfo, TDengineRowData row) {
        return TDengineNameUtils.quoteIdentifier(tableInfo.getDatabaseName())
                + "."
                + TDengineNameUtils.quoteIdentifier(row.getSubtableName())
                + " USING "
                + tableInfo.qualifiedStableName()
                + " TAGS ("
                + joinLiterals(row.getTagValues())
                + ") ("
                + joinIdentifiers(row.getMetricColumnNames())
                + ") VALUES ("
                + joinLiterals(row.getMetricValues())
                + ")";
    }

    private static String toHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        char[] symbols = "0123456789ABCDEF".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xFF;
            hex[i * 2] = symbols[value >>> 4];
            hex[i * 2 + 1] = symbols[value & 0x0F];
        }
        return new String(hex);
    }

    public static List<TDengineRowData> singleton(TDengineRowData rowData) {
        List<TDengineRowData> rows = new ArrayList<>(1);
        rows.add(rowData);
        return rows;
    }

    public static boolean sameTarget(TDengineRowData left, TDengineRowData right) {
        return left.getTableInfo().getDatabaseName().equals(right.getTableInfo().getDatabaseName())
                && left.getTableInfo().getStableName().equals(right.getTableInfo().getStableName());
    }

    public static String describeTarget(TDengineTableInfo tableInfo) {
        return tableInfo.getDatabaseName() + "." + tableInfo.getStableName();
    }
}
