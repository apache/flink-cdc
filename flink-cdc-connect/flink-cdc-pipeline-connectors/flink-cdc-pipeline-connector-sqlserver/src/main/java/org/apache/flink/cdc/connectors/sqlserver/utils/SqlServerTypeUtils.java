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

package org.apache.flink.cdc.connectors.sqlserver.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.relational.Column;

import java.sql.Types;

/** A utility class for converting SQL Server types to Flink CDC types. */
public class SqlServerTypeUtils {

    // SQL Server specific type names
    static final String UNIQUE_IDENTIFIER = "uniqueidentifier";
    static final String XML = "xml";
    static final String SQL_VARIANT = "sql_variant";
    static final String HIERARCHY_ID = "hierarchyid";
    static final String GEOMETRY = "geometry";
    static final String GEOGRAPHY = "geography";
    static final String MONEY = "money";
    static final String SMALL_MONEY = "smallmoney";
    static final String DATETIME_OFFSET = "datetimeoffset";
    static final String DATETIME2 = "datetime2";
    static final String DATETIME = "datetime";
    static final String SMALL_DATETIME = "smalldatetime";
    static final String IMAGE = "image";
    static final String TIMESTAMP = "timestamp";
    static final String ROW_VERSION = "rowversion";
    static final String TEXT = "text";
    static final String N_TEXT = "ntext";

    /** Returns a corresponding Flink CDC data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink CDC data type from a debezium {@link Column} with nullable
     * always be true.
     */
    private static DataType convertFromColumn(Column column) {
        int precision = column.length();
        int scale = column.scale().orElse(0);

        switch (column.jdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            case Types.TINYINT:
                // SQL Server TINYINT is unsigned 0-255, maps to SMALLINT
                return DataTypes.SMALLINT();
            case Types.SMALLINT:
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                return DataTypes.INT();
            case Types.BIGINT:
                return DataTypes.BIGINT();
            case Types.REAL:
                return DataTypes.FLOAT();
            case Types.FLOAT:
                return DataTypes.DOUBLE();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (precision > 0 && precision <= DecimalType.MAX_PRECISION) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
            case Types.CHAR:
            case Types.NCHAR:
                return DataTypes.CHAR(precision);
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (precision > 0) {
                    return DataTypes.VARCHAR(precision);
                }
                return DataTypes.STRING();
            case Types.CLOB:
            case Types.NCLOB:
                return DataTypes.STRING();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return DataTypes.BYTES();
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return DataTypes.TIME(Math.max(scale, 0));
            case Types.TIMESTAMP:
                return DataTypes.TIMESTAMP(scale > 0 ? scale : 6);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return DataTypes.TIMESTAMP_LTZ(scale > 0 ? scale : 6);
            case Types.STRUCT:
                // SQL Server specific types like unique identifier, xml, etc.
                String typeName = column.typeName();
                if (UNIQUE_IDENTIFIER.equalsIgnoreCase(typeName)) {
                    return DataTypes.STRING();
                }
                return DataTypes.STRING();
            default:
                // For unknown types, try to handle them as STRING
                String unknownTypeName = column.typeName();
                if (unknownTypeName != null) {
                    // Handle SQL Server specific types
                    switch (unknownTypeName.toLowerCase()) {
                        case UNIQUE_IDENTIFIER:
                        case XML:
                        case SQL_VARIANT:
                        case HIERARCHY_ID:
                        case GEOMETRY:
                        case GEOGRAPHY:
                            return DataTypes.STRING();
                        case MONEY:
                        case SMALL_MONEY:
                            return DataTypes.DECIMAL(10, 4);
                        case DATETIME_OFFSET:
                            return DataTypes.TIMESTAMP_LTZ(scale > 0 ? scale : 7);
                        case DATETIME2:
                            return DataTypes.TIMESTAMP(scale > 0 ? scale : 7);
                        case DATETIME:
                            return DataTypes.TIMESTAMP(3);
                        case SMALL_DATETIME:
                            return DataTypes.TIMESTAMP(0);
                        case IMAGE:
                        case TIMESTAMP:
                        case ROW_VERSION:
                            return DataTypes.BYTES();
                        case TEXT:
                        case N_TEXT:
                            return DataTypes.STRING();
                        default:
                            // Fall through to exception
                    }
                }
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SQL Server type '%s', JDBC type '%d' yet.",
                                column.typeName(), column.jdbcType()));
        }
    }
}
