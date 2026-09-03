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

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses Canal {@code mysqlType} / row fields into a CDC {@link Schema}. */
class CanalJsonSchemaParser {

    private static final Pattern MYSQL_TYPE_PATTERN =
            Pattern.compile(
                    "^([A-Z]+)(?:\\s+UNSIGNED)?(?:\\s+ZEROFILL)?(?:\\((\\d+)(?:,\\s*(\\d+))?\\))?(?:\\s+UNSIGNED)?(?:\\s+ZEROFILL)?.*$");

    Schema parseSchema(JsonNode mysqlType, JsonNode sampleRow, List<String> primaryKeys) {
        Set<String> primaryKeySet = new HashSet<>(primaryKeys);
        Schema.Builder builder = Schema.newBuilder();
        if (mysqlType != null && mysqlType.isObject() && mysqlType.size() > 0) {
            Iterator<Map.Entry<String, JsonNode>> fields = mysqlType.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                DataType type = parseMysqlType(field.getValue().asText());
                builder.physicalColumn(
                        field.getKey(),
                        primaryKeySet.contains(field.getKey()) ? type.notNull() : type.nullable());
            }
        } else if (sampleRow != null && sampleRow.isObject()) {
            Iterator<String> names = sampleRow.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                DataType type = DataTypes.STRING();
                builder.physicalColumn(
                        name, primaryKeySet.contains(name) ? type.notNull() : type.nullable());
            }
        }
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }
        return builder.build();
    }

    List<String> parsePrimaryKeys(JsonNode pkNames) {
        List<String> result = new ArrayList<>();
        if (pkNames == null || !pkNames.isArray()) {
            return result;
        }
        for (JsonNode name : pkNames) {
            if (name != null && !name.isNull() && !name.asText().isEmpty()) {
                result.add(name.asText());
            }
        }
        return result;
    }

    DataType parseMysqlType(String mysqlType) {
        if (mysqlType == null || mysqlType.trim().isEmpty()) {
            return DataTypes.STRING();
        }
        String normalized = mysqlType.trim().toUpperCase();
        Matcher matcher = MYSQL_TYPE_PATTERN.matcher(normalized);
        String typeName = normalized;
        Integer length = null;
        Integer scale = null;
        if (matcher.matches()) {
            typeName = matcher.group(1);
            if (matcher.group(2) != null) {
                length = Integer.parseInt(matcher.group(2));
            }
            if (matcher.group(3) != null) {
                scale = Integer.parseInt(matcher.group(3));
            }
        }
        boolean unsigned = normalized.contains("UNSIGNED") || normalized.equals("SERIAL");
        switch (typeName) {
            case "BIT":
                if (length == null || length <= 1) {
                    return DataTypes.BOOLEAN();
                }
                return DataTypes.VARBINARY((length + 7) / 8);
            case "BOOL":
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
            case "SMALLINT":
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "YEAR":
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case "BIGINT":
            case "SERIAL":
                return unsigned ? DataTypes.DECIMAL(20, 0) : DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "REAL":
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
            case "FIXED":
                int precision = length == null ? 10 : Math.min(38, length);
                int decimalScale = scale == null ? 0 : Math.min(scale, precision);
                return DataTypes.DECIMAL(precision, decimalScale);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return length == null ? DataTypes.TIME(0) : DataTypes.TIME(Math.min(length, 9));
            case "DATETIME":
            case "TIMESTAMP":
                return length == null
                        ? DataTypes.TIMESTAMP(0)
                        : DataTypes.TIMESTAMP(Math.min(length, 9));
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return DataTypes.BYTES();
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "JSON":
            case "ENUM":
            case "SET":
            default:
                return DataTypes.STRING();
        }
    }
}
