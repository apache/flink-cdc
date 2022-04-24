/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;
import oracle.jdbc.OracleTypes;

import java.sql.Types;

/** Utilities for converting from oracle types to Flink types. */
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

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true, copy from io.debezium.connector.oracle.OracleValueConverters#converte.
     */
    private static DataType convertFromColumn(Column column) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BLOB:
                return DataTypes.BYTES();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return DataTypes.INT();
            case Types.FLOAT:
            case Types.REAL:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DataTypes.DECIMAL(column.length(), column.scale().orElse(0));
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support Oracle type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }
}
