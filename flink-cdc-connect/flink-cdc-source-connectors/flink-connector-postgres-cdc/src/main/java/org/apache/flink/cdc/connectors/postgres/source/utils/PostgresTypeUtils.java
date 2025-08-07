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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;

import java.util.Properties;

import static org.apache.flink.cdc.connectors.postgres.source.utils.DebeziumConfigUtils.DecimalHandlingMode;
import static org.apache.flink.cdc.connectors.postgres.source.utils.DebeziumConfigUtils.getDecimalHandlingMode;

/** A utility class for converting Postgres types to Flink types. */
public class PostgresTypeUtils {

    private static final String PG_SMALLSERIAL = "smallserial";
    private static final String PG_SERIAL = "serial";
    private static final String PG_BIGSERIAL = "bigserial";
    private static final String PG_BYTEA = "bytea";
    private static final String PG_BYTEA_ARRAY = "_bytea";
    private static final String PG_SMALLINT = "int2";
    private static final String PG_SMALLINT_ARRAY = "_int2";
    private static final String PG_INTEGER = "int4";
    private static final String PG_INTEGER_ARRAY = "_int4";
    private static final String PG_BIGINT = "int8";
    private static final String PG_BIGINT_ARRAY = "_int8";
    private static final String PG_REAL = "float4";
    private static final String PG_REAL_ARRAY = "_float4";
    private static final String PG_DOUBLE_PRECISION = "float8";
    private static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    private static final String PG_NUMERIC = "numeric";
    private static final String PG_NUMERIC_ARRAY = "_numeric";
    private static final String PG_BOOLEAN = "bool";
    private static final String PG_BOOLEAN_ARRAY = "_bool";
    private static final String PG_TIMESTAMP = "timestamp";
    private static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    private static final String PG_TIMESTAMPTZ = "timestamptz";
    private static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    private static final String PG_DATE = "date";
    private static final String PG_DATE_ARRAY = "_date";
    private static final String PG_TIME = "time";
    private static final String PG_TIME_ARRAY = "_time";
    private static final String PG_TEXT = "text";
    private static final String PG_TEXT_ARRAY = "_text";
    private static final String PG_CHAR = "bpchar";
    private static final String PG_CHAR_ARRAY = "_bpchar";
    private static final String PG_CHARACTER = "character";
    private static final String PG_CHARACTER_ARRAY = "_character";
    private static final String PG_CHARACTER_VARYING = "varchar";
    private static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    private static final String PG_UUID = "uuid";
    private static final String PG_BIT = "bit";
    private static final String PG_BIT_ARRAY = "_bit";
    private static final String PG_VARBIT = "varbit";
    private static final String PG_VARBIT_ARRAY = "_varbit";

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        return fromDbzColumn(column, null);
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with Debezium
     * properties.
     */
    public static DataType fromDbzColumn(Column column, Properties debeziumProperties) {
        DataType dataType = convertFromColumn(column, debeziumProperties);
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
    private static DataType convertFromColumn(Column column, Properties debeziumProperties) {
        DecimalHandlingMode decimalMode = getDecimalHandlingMode(debeziumProperties);
        String typeName = column.typeName();

        int precision = column.length();
        int scale = column.scale().orElse(0);

        switch (typeName) {
            case PG_BOOLEAN:
                return DataTypes.BOOLEAN();
            case PG_BOOLEAN_ARRAY:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case PG_BYTEA:
                return DataTypes.BYTES();
            case PG_BYTEA_ARRAY:
                return DataTypes.ARRAY(DataTypes.BYTES());
            case PG_SMALLINT:
            case PG_SMALLSERIAL:
                return DataTypes.SMALLINT();
            case PG_SMALLINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            case PG_INTEGER:
            case PG_SERIAL:
                return DataTypes.INT();
            case PG_INTEGER_ARRAY:
                return DataTypes.ARRAY(DataTypes.INT());
            case PG_BIGINT:
            case PG_BIGSERIAL:
                return DataTypes.BIGINT();
            case PG_BIGINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case PG_REAL:
                return DataTypes.FLOAT();
            case PG_REAL_ARRAY:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case PG_DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case PG_DOUBLE_PRECISION_ARRAY:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case PG_NUMERIC:
                return DebeziumConfigUtils.handleNumericTypeForTable(precision, scale, decimalMode);
            case PG_NUMERIC_ARRAY:
                return DataTypes.ARRAY(
                        DebeziumConfigUtils.handleNumericTypeForTable(
                                precision, scale, decimalMode));
            case PG_CHAR:
            case PG_CHARACTER:
                return DataTypes.CHAR(precision);
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
                return DataTypes.ARRAY(DataTypes.CHAR(precision));
            case PG_CHARACTER_VARYING:
                return DataTypes.VARCHAR(precision);
            case PG_CHARACTER_VARYING_ARRAY:
                return DataTypes.ARRAY(DataTypes.VARCHAR(precision));
            case PG_TEXT:
            case PG_UUID:
                return DataTypes.STRING();
            case PG_TEXT_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PG_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case PG_TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP(scale));
            case PG_TIMESTAMPTZ:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale);
            case PG_TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale));
            case PG_TIME:
                return DataTypes.TIME(scale);
            case PG_TIME_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIME(scale));
            case PG_DATE:
                return DataTypes.DATE();
            case PG_DATE_ARRAY:
                return DataTypes.ARRAY(DataTypes.DATE());
            case PG_BIT:
                // Fix for FLINK-35907: Handle bit(1) as BOOLEAN, bit(n) as BYTES
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.BYTES();
                }
            case PG_BIT_ARRAY:
                // For bit arrays, always use BYTES array
                return DataTypes.ARRAY(DataTypes.BYTES());
            case PG_VARBIT:
                // Variable-length bit strings always as BYTES
                return DataTypes.BYTES();
            case PG_VARBIT_ARRAY:
                return DataTypes.ARRAY(DataTypes.BYTES());
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Postgres type '%s' yet", typeName));
        }
    }
}
