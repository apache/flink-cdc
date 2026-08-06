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
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.relational.Column;

import java.sql.Types;

/** A utility class for converting DB2 types to Flink CDC types. */
public class Db2TypeUtils {

    static final String XML = "xml";
    static final String DECFLOAT = "DECFLOAT";

    /** Returns a corresponding Flink CDC data type from a Debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink CDC data type from a Debezium {@link Column} that is always
     * nullable.
     */
    private static DataType convertFromColumn(Column column) {
        int precision = column.length();
        int scale = column.scale().orElse(0);

        switch (column.jdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            case Types.TINYINT:
                // DB2 TINYINT is unsigned 0-255, maps to SMALLINT
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
                return DataTypes.FLOAT();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (precision > 0 && precision <= DecimalType.MAX_PRECISION) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, scale);
            case Types.CHAR:
            case Types.NCHAR:
                return precision > 0 ? DataTypes.CHAR(precision) : DataTypes.STRING();
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
            case Types.SQLXML:
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
                return DataTypes.TIMESTAMP(timestampPrecision(column));
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return DataTypes.TIMESTAMP_LTZ(timestampPrecision(column));
            default:
                String unknownTypeName = column.typeName();
                if (XML.equalsIgnoreCase(unknownTypeName)) {
                    return DataTypes.STRING();
                }
                if (DECFLOAT.equalsIgnoreCase(unknownTypeName)) {
                    return DataTypes.DOUBLE();
                }
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support DB2 type '%s', JDBC type '%d' yet.",
                                column.typeName(), column.jdbcType()));
        }
    }

    private static int timestampPrecision(Column column) {
        int precision = column.length();
        if (precision < TimestampType.MIN_PRECISION) {
            return DataTypes.TIMESTAMP().getPrecision();
        }
        return Math.min(precision, TimestampType.MAX_PRECISION);
    }
}
