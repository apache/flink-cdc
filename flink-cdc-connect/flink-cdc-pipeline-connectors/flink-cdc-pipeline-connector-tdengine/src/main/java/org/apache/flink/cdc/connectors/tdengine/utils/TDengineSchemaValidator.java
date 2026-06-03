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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineClientWrapper;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Validates TDengine super table schemas against expected Flink CDC schemas. */
public final class TDengineSchemaValidator {

    private TDengineSchemaValidator() {}

    public static void validateStableSchema(
            TDengineClientWrapper client,
            TDengineTableInfo tableInfo,
            Schema schema,
            TDengineDataSinkConfig config)
            throws SQLException {
        Map<String, String> expectedColumns = expectedColumns(schema, config);
        Map<String, String> actualColumns = readStableColumns(client, tableInfo);
        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> expected : expectedColumns.entrySet()) {
            String actualType = actualColumns.get(expected.getKey());
            if (actualType == null) {
                mismatches.add("missing column `" + expected.getKey() + "`");
                continue;
            }
            if (!Objects.equals(expected.getValue(), actualType)) {
                mismatches.add(
                        "column `"
                                + expected.getKey()
                                + "` expected type "
                                + expected.getValue()
                                + " but was "
                                + actualType);
            }
        }
        for (String actualColumn : actualColumns.keySet()) {
            if (!expectedColumns.containsKey(actualColumn)) {
                mismatches.add("unexpected column `" + actualColumn + "`");
            }
        }
        if (!mismatches.isEmpty()) {
            throw new SQLException(
                    "TDengine stable "
                            + tableInfo.qualifiedStableName()
                            + " schema mismatch: "
                            + String.join("; ", mismatches));
        }
    }

    public static Map<String, String> expectedColumns(
            Schema schema, TDengineDataSinkConfig config) {
        Map<String, String> columns = new LinkedHashMap<>();
        String timestampField = config.getTimestampField();
        String subtableField = config.getSubtableField();
        columns.put(
                timestampField,
                normalizeType(
                        TDengineTypeUtils.toTDengineType(
                                TDengineSqlUtils.requireColumn(schema, timestampField).getType(),
                                true,
                                config)));
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()
                    || column.getName().equals(timestampField)
                    || column.getName().equals(subtableField)
                    || config.getTagFields().contains(column.getName())) {
                continue;
            }
            columns.put(
                    column.getName(),
                    normalizeType(
                            TDengineTypeUtils.toTDengineType(column.getType(), false, config)));
        }
        for (String tagField : config.getTagFields()) {
            Column column = TDengineSqlUtils.requireColumn(schema, tagField);
            columns.put(
                    tagField,
                    normalizeType(
                            TDengineTypeUtils.toTDengineType(column.getType(), false, config)));
        }
        return columns;
    }

    static Map<String, String> readStableColumns(
            TDengineClientWrapper client, TDengineTableInfo tableInfo) throws SQLException {
        Map<String, String> columns = new LinkedHashMap<>();
        for (TDengineColumnDescription column : client.describeStable(tableInfo)) {
            columns.put(column.getName(), normalizeType(column.getType()));
        }
        return columns;
    }

    static String normalizeType(String type) {
        if (type == null) {
            return "";
        }
        return type.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", "");
    }
}
