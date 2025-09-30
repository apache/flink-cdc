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

import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import io.debezium.relational.Column;

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

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true.
     */
    private static DataType convertFromColumn(Column column) {
        String type = column.typeName();
        DataType dataType;
        switch (type.toUpperCase()) {
            case "VARCHAR2":
            case "VARCHAR":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
                dataType = new VarCharType(column.length());
                break;
            case "BLOB":
            case "CLOB":
            case "TEXT":
                dataType = DataTypes.STRING();
                break;
            case "NUMBER":
                dataType =
                        column.length() == 0 || column.scale().get() <= 0
                                ? new BigIntType()
                                : new DecimalType(column.length(), column.scale().get());
                break;
            case "LONG":
                dataType = new BigIntType();
                break;
            case "DATE":
                dataType = new TimestampType(6);
                break;
            case "FLOAT":
            case "BINARY_FLOAT":
                dataType = new FloatType();
                break;
            case "BINARY_DOUBLE":
            case "DOUBLE":
                dataType = new DoubleType();
                break;
            case "TIMESTAMP(1)":
                dataType = new TimestampType(1);
                break;
            case "TIMESTAMP(2)":
                dataType = new TimestampType(2);
                break;
            case "TIMESTAMP(3)":
                dataType = new TimestampType(3);
                break;
            case "TIMESTAMP(4)":
                dataType = new TimestampType(4);
                break;
            case "TIMESTAMP(6)":
                dataType = new TimestampType(6);
                break;
            case "TIMESTAMP(9)":
                dataType = new TimestampType(9);
                break;
            case "TIMESTAMP(9) WITH TIME ZONE":
                dataType = new ZonedTimestampType(9);
                break;
            case "TIMESTAMP(6) WITH TIME ZONE":
                dataType = new ZonedTimestampType(6);
                break;
            case "TIMESTAMP(3) WITH TIME ZONE":
                dataType = new ZonedTimestampType(3);
                break;
            case "TIMESTAMP(13) WITH TIME ZONE":
                dataType = new ZonedTimestampType(13);
                break;
            case "TIMESTAMP(6) WITH LOCAL TIME ZONE":
                dataType = new LocalZonedTimestampType(6);
                break;
            case "INTERVAL YEAR(2) TO MONTH":
            case "INTERVAL DAY(3) TO SECOND(2)":
                dataType = new BigIntType();
                break;
            case "XMLTYPE":
                dataType = new VarCharType();
                break;
            default:
                throw new RuntimeException("Unsupported data type:" + type);
        }
        return dataType;
    }

    private OracleTypeUtils() {}
}
