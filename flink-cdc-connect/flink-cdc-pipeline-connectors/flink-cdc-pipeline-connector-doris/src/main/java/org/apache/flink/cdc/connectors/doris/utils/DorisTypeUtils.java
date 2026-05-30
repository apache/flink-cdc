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

package org.apache.flink.cdc.connectors.doris.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.rest.models.Field;

import java.util.Locale;

/** Shared Doris type-string helpers for DDL and diagnostics. */
public class DorisTypeUtils {

    private DorisTypeUtils() {}

    public static String toDorisTypeString(DataType dataType) {
        if (dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(dataType);
            return String.format("%s(%s)", "DATETIMEV2", Math.min(Math.max(precision, 0), 6));
        }
        return DorisTypeMapper.toDorisType(DataTypeUtils.toFlinkDataType(dataType));
    }

    public static String buildExistingFieldType(Field field) {
        String type = field.getType();
        if (type == null || type.trim().isEmpty()) {
            return "";
        }

        String trimmedType = type.trim();
        if (trimmedType.contains("(")) {
            return trimmedType;
        }

        String upperType = trimmedType.toUpperCase(Locale.ROOT);
        switch (upperType) {
            case "CHAR":
            case "VARCHAR":
                return field.getPrecision() > 0
                        ? String.format("%s(%s)", upperType, field.getPrecision())
                        : upperType;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
                return field.getPrecision() > 0
                        ? String.format(
                                "%s(%s,%s)", upperType, field.getPrecision(), field.getScale())
                        : upperType;
            case "DATETIME":
            case "DATETIMEV2":
                return String.format("%s(%s)", upperType, Math.max(field.getScale(), 0));
            default:
                return upperType;
        }
    }

    public static String normalizeDorisTypeDefinition(String typeDefinition) {
        if (typeDefinition == null || typeDefinition.trim().isEmpty()) {
            return "";
        }

        String normalized =
                typeDefinition
                        .toUpperCase(Locale.ROOT)
                        .replaceAll("\\s*,\\s*", ",")
                        .replaceAll("\\(\\s*", "(")
                        .replaceAll("\\s*\\)", ")")
                        .replaceAll("\\s+", " ")
                        .trim();
        normalized = normalized.replace("DATETIMEV2", "DATETIME");
        normalized = normalized.replace("DATEV2", "DATE");
        normalized = normalized.replace("DECIMALV3", "DECIMAL");
        normalized = normalized.replace("DECIMALV2", "DECIMAL");
        normalized = normalized.replace("TEXT", "STRING");
        return normalized;
    }
}
