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

package org.apache.flink.cdc.connectors.oceanbase.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;

import java.sql.Types;

/** Utilities for converting from OceanBase types to Flink types. */
public class OceanBaseTypeUtils {

    public static boolean isUnsignedColumn(Column column) {
        return column.typeName().toUpperCase().contains("UNSIGNED");
    }

    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    private static DataType convertFromColumn(Column column) {
        switch (column.jdbcType()) {
            case Types.BIT:
                if (column.length() > 1) {
                    return DataTypes.BYTES();
                }
                return DataTypes.BOOLEAN();
            case Types.TINYINT:
                if (column.length() == 1) {
                    DataTypes.BOOLEAN();
                }
                if (isUnsignedColumn(column)) {
                    return DataTypes.SMALLINT();
                }
                return DataTypes.TINYINT();
            case Types.SMALLINT:
                if (isUnsignedColumn(column)) {
                    return DataTypes.INT();
                }
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                if (!column.typeName().toUpperCase().startsWith("MEDIUMINT")
                        && isUnsignedColumn(column)) {
                    return DataTypes.BIGINT();
                }
                return DataTypes.INT();
            case Types.BIGINT:
                if (isUnsignedColumn(column)) {
                    return DataTypes.DECIMAL(20, 0);
                }
                return DataTypes.BIGINT();
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DataTypes.DECIMAL(column.length(), column.scale().orElse(0));
            case Types.REAL:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case Types.TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BINARY:
                return DataTypes.BINARY(column.length());
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return DataTypes.VARBINARY(column.length());
            case Types.BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support OceanBase type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }
}
