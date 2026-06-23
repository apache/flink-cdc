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
import org.apache.flink.cdc.common.types.DecimalType;

import io.debezium.relational.Column;
import oracle.jdbc.OracleTypes;

import java.sql.Types;

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
        if (column.typeName().contains("SDO_GEOMETRY")) {
            return DataTypes.STRING();
        }
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.NCHAR:
                return DataTypes.CHAR(column.length());
            case Types.VARCHAR:
            case Types.NVARCHAR:
                return DataTypes.VARCHAR(column.length());
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
                {
                    // Bare NUMBER (scale unspecified): floating-point semantic,
                    // can store both integers and decimals up to 38 significant digits.
                    // Use DECIMAL(38, 19) as a balanced universal numeric type.
                    if (!column.scale().isPresent()) {
                        return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 19);
                    }

                    int precision = column.length();
                    int scale = column.scale().get();

                    // precision == 0 (e.g. NUMBER(*, s)) means unspecified.
                    int p = precision > 0 ? precision : DecimalType.MAX_PRECISION;

                    // scale < 0 or > 36: not safely representable as DECIMAL, downgrade to STRING.
                    if (scale < 0 || scale > 36) {
                        return DataTypes.STRING();
                    }

                    if (scale == 0) {
                        // Explicit integer: use BIGINT for p <= 18, DECIMAL otherwise.
                        return p <= 18 ? DataTypes.BIGINT() : DataTypes.DECIMAL(p, 0);
                    }

                    // 1 <= scale <= 36: standard decimal with fractional part.
                    return DataTypes.DECIMAL(p, scale);
                }
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case OracleTypes.TIMESTAMPTZ:
                String length = getFirstBracketContent(column.typeName());
                if (length != null) {
                    return DataTypes.TIMESTAMP_TZ(Integer.parseInt(length));
                } else {
                    if (column.length() >= 0) {
                        return DataTypes.TIMESTAMP_TZ(column.length());
                    }
                }
                return DataTypes.TIMESTAMP_TZ();
            case OracleTypes.TIMESTAMPLTZ:
                length = getFirstBracketContent(column.typeName());
                if (length != null) {
                    return DataTypes.TIMESTAMP_LTZ(Integer.parseInt(length));
                } else {
                    if (column.length() >= 0) {
                        return DataTypes.TIMESTAMP_LTZ(column.length());
                    }
                }
                return DataTypes.TIMESTAMP_TZ();
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

    private static String getFirstBracketContent(String input) {
        int start = input.indexOf("(");
        int end = input.indexOf(")");
        if (start != -1 && end != -1 && start < end) {
            return input.substring(start + 1, end);
        }
        return null;
    }

    private OracleTypeUtils() {}
}
