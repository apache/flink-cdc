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

package org.apache.flink.cdc.connectors.oracle.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;
import oracle.jdbc.OracleTypes;

import java.sql.Types;
import java.util.Locale;

/** Utilities for converting from oracle types to {@link DataType}s. */
public class OracleTypeUtils {

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /** Returns a corresponding Flink cdc data type from a debezium {@link Column} . */
    private static DataType convertFromColumn(Column column) {
        String normalizedTypeName = normalizeTypeName(column.typeName());
        if (normalizedTypeName.contains("SDO_GEOMETRY")) {
            return DataTypes.STRING();
        }
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.NCHAR:
                return DataTypes.CHAR(column.length());
            case Types.VARCHAR:
            case Types.NVARCHAR:
                return DataTypes.VARCHAR(column.length());
            case Types.OTHER:
            case Types.STRUCT:
            case Types.CLOB:
            case OracleTypes.NCLOB:
            case Types.SQLXML:
                return DataTypes.STRING();
            case Types.BLOB:
            case OracleTypes.RAW:
            case OracleTypes.BFILE:
                return DataTypes.BYTES();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return DataTypes.INT();
            case Types.FLOAT:
            case Types.REAL:
            case OracleTypes.BINARY_FLOAT:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
            case OracleTypes.BINARY_DOUBLE:
                return DataTypes.DOUBLE();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return mappingNumberType(column.length(), column.scale().orElse(0));
            case Types.DATE:
                return DataTypes.TIMESTAMP();
            case Types.TIMESTAMP:
                return mappingTimestampType(normalizedTypeName, column.length());
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case OracleTypes.TIMESTAMPTZ:
                return mappingTimestampWithTimeZoneType(normalizedTypeName, column.length());
            case OracleTypes.TIMESTAMPLTZ:
                return mappingTimestampLtzType(normalizedTypeName, column.length());
            case OracleTypes.INTERVALYM:
            case OracleTypes.INTERVALDS:
            case Types.BIGINT:
                return DataTypes.BIGINT();
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support Oracle type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }

    private static DataType mappingNumberType(int precision, int scale) {
        if (precision == 1 && scale == 0) {
            return DataTypes.BOOLEAN();
        }

        if (precision <= 0) {
            int normalizedScale = Math.max(0, scale);
            return DataTypes.DECIMAL(38, Math.min(normalizedScale, 38));
        }

        if (scale <= 0) {
            int integerDigits = precision - scale;
            if (integerDigits <= 0) {
                return DataTypes.DECIMAL(precision, 0);
            }
            if (integerDigits < 3) {
                return DataTypes.TINYINT();
            }
            if (integerDigits < 5) {
                return DataTypes.SMALLINT();
            }
            if (integerDigits < 10) {
                return DataTypes.INT();
            }
            if (integerDigits < 19) {
                return DataTypes.BIGINT();
            }
            if (integerDigits <= 38) {
                return DataTypes.DECIMAL(integerDigits, 0);
            }
            return DataTypes.STRING();
        }

        if (precision > 38) {
            return DataTypes.STRING();
        }
        if (scale > precision) {
            return DataTypes.DECIMAL(precision, precision);
        }
        return DataTypes.DECIMAL(precision, scale);
    }

    private static DataType mappingTimestampType(String normalizedType, int fallbackPrecision) {
        Integer precision = resolveTemporalPrecision(normalizedType, fallbackPrecision);
        return precision == null ? DataTypes.TIMESTAMP() : DataTypes.TIMESTAMP(precision);
    }

    private static DataType mappingTimestampWithTimeZoneType(
            String normalizedType, int fallbackPrecision) {
        Integer precision = resolveTemporalPrecision(normalizedType, fallbackPrecision);
        return precision == null ? DataTypes.TIMESTAMP_TZ() : DataTypes.TIMESTAMP_TZ(precision);
    }

    private static DataType mappingTimestampLtzType(String normalizedType, int fallbackPrecision) {
        Integer precision = resolveTemporalPrecision(normalizedType, fallbackPrecision);
        return precision == null ? DataTypes.TIMESTAMP_LTZ() : DataTypes.TIMESTAMP_LTZ(precision);
    }

    private static Integer resolveTemporalPrecision(String normalizedType, int fallbackPrecision) {
        Integer precisionFromType = extractFirstBracketNumber(normalizedType);
        if (precisionFromType != null && precisionFromType >= 0 && precisionFromType <= 9) {
            return precisionFromType;
        }
        if (fallbackPrecision >= 0 && fallbackPrecision <= 9) {
            return fallbackPrecision;
        }
        return null;
    }

    private static Integer extractFirstBracketNumber(String input) {
        int start = input.indexOf("(");
        int end = input.indexOf(")", start + 1);
        if (start == -1 || end == -1 || end <= start + 1) {
            return null;
        }

        String content = input.substring(start + 1, end).trim();
        int comma = content.indexOf(',');
        if (comma >= 0) {
            content = content.substring(0, comma).trim();
        }
        if (!content.matches("\\d+")) {
            return null;
        }
        return Integer.parseInt(content);
    }

    private static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        return typeName.toUpperCase(Locale.ROOT).trim().replaceAll("\\s+", " ");
    }

    private OracleTypeUtils() {}
}
