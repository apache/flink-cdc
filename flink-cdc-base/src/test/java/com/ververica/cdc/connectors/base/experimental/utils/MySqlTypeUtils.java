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

package com.ververica.cdc.connectors.base.experimental.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;

/** Utilities for converting from MySQL types to Flink types. */
public class MySqlTypeUtils {

    // ------ MySQL Type ------
    private static final String BIT = "BIT";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String BIGINT = "BIGINT";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String UNKNOWN = "UNKNOWN";

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true.
     */
    private static DataType convertFromColumn(Column column) {
        String typeName = column.typeName();
        switch (typeName) {
            case TINYINT:
                return column.length() == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DECIMAL:
                return DataTypes.DECIMAL(column.length(), column.scale().orElse(0));
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
            case TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
            case CHAR:
                return DataTypes.CHAR(column.length());
            case VARCHAR:
                return DataTypes.VARCHAR(column.length());
            case TEXT:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(column.length());
            case VARBINARY:
                return DataTypes.VARBINARY(column.length());
            case BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", typeName));
        }
    }
}
