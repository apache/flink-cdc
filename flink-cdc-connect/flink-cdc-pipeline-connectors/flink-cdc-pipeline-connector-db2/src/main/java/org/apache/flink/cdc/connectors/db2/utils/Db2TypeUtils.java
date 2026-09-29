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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;

import java.sql.Types;

/** A utility class for converting Db2 types to Flink CDC types. */
public class Db2TypeUtils {

    // Db2 specific type names
    static final String DECFLOAT = "decfloat";
    static final String XML = "xml";

    private static final int DECFLOAT_PRECISION_16 = 16;
    private static final int DECFLOAT_PRECISION_34 = 34;

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
        String typeName = column.typeName();

        if (typeName != null) {
            switch (typeName.toLowerCase()) {
                case DECFLOAT:
                    // DECFLOAT(16) maps to DOUBLE, DECFLOAT(34) maps to DECIMAL(34, 0)
                    if (precision == DECFLOAT_PRECISION_34) {
                        return DataTypes.DECIMAL(DECFLOAT_PRECISION_34, 0);
                    }
                    return DataTypes.DOUBLE();
                case XML:
                    return DataTypes.STRING();
                default:
                    // Fall through to JDBC type handling.
            }
        }

        switch (column.jdbcType()) {
            case Types.CHAR:
                if (precision > 0) {
                    return DataTypes.CHAR(precision);
                }
                return DataTypes.STRING();
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (precision > 0) {
                    return DataTypes.VARCHAR(precision);
                }
                return DataTypes.STRING();
            case Types.SQLXML:
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return DataTypes.BYTES();
            case Types.TINYINT:
            case Types.SMALLINT:
                // Db2 SMALLINT is a 2-byte integer
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                return DataTypes.INT();
            case Types.BIGINT:
                return DataTypes.BIGINT();
            case Types.REAL:
                return DataTypes.FLOAT();
            case Types.FLOAT:
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (precision > 0) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(38, scale);
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIME:
                return DataTypes.TIME(Math.max(scale, 0));
            case Types.TIMESTAMP:
                return DataTypes.TIMESTAMP(column.scale().orElse(6));
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support Db2 type '%s', JDBC type '%d' yet.",
                                column.typeName(), column.jdbcType()));
        }
    }
}
