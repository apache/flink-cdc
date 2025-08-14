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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.MONEY_FRACTION_DIGITS;

/** A utility class for converting Postgres types to Flink types. */
public class PostgresTypeUtils {

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(
            Column column, PostgresConnectorConfig dbzConfig, TypeRegistry typeRegistry) {
        DataType dataType = convertFromColumn(column, dbzConfig, typeRegistry);
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
    private static DataType convertFromColumn(
            Column column, PostgresConnectorConfig dbzConfig, TypeRegistry typeRegistry) {
        int nativeType = column.nativeType();

        int precision = column.length();
        int scale = column.scale().orElse(0);

        PostgresConnectorConfig.IntervalHandlingMode intervalHandlingMode =
                PostgresConnectorConfig.IntervalHandlingMode.parse(
                        dbzConfig
                                .getConfig()
                                .getString(PostgresConnectorConfig.INTERVAL_HANDLING_MODE));

        PostgresConnectorConfig.BinaryHandlingMode binaryHandlingMode =
                dbzConfig.binaryHandlingMode();

        TemporalPrecisionMode temporalPrecisionMode = dbzConfig.getTemporalPrecisionMode();

        JdbcValueConverters.DecimalMode decimalMode =
                dbzConfig.getDecimalMode() != null
                        ? dbzConfig.getDecimalMode()
                        : JdbcValueConverters.DecimalMode.PRECISE;

        PostgresConnectorConfig.HStoreHandlingMode hStoreHandlingMode =
                PostgresConnectorConfig.HStoreHandlingMode.parse(
                        dbzConfig
                                .getConfig()
                                .getString(PostgresConnectorConfig.HSTORE_HANDLING_MODE));

        switch (nativeType) {
            case PgOid.BOOL:
                return DataTypes.BOOLEAN();
            case PgOid.BIT:
            case PgOid.VARBIT:
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.BINARY(precision);
                }
            case PgOid.BOOL_ARRAY:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case PgOid.BYTEA:
                return handleBinaryWithBinaryMode(binaryHandlingMode);
            case PgOid.BYTEA_ARRAY:
                return DataTypes.ARRAY(handleBinaryWithBinaryMode(binaryHandlingMode));
            case PgOid.INT2:
                return DataTypes.SMALLINT();
            case PgOid.INT2_ARRAY:
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            case PgOid.INT4:
                return DataTypes.INT();
            case PgOid.INT4_ARRAY:
                return DataTypes.ARRAY(DataTypes.INT());
            case PgOid.INT8:
            case PgOid.OID:
                return DataTypes.BIGINT();
            case PgOid.INTERVAL:
                return handleIntervalWithIntervalHandlingMode(intervalHandlingMode);
            case PgOid.INTERVAL_ARRAY:
                return DataTypes.ARRAY(
                        handleIntervalWithIntervalHandlingMode(intervalHandlingMode));
            case PgOid.INT8_ARRAY:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case PgOid.FLOAT4:
                return DataTypes.FLOAT();
            case PgOid.FLOAT4_ARRAY:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case PgOid.FLOAT8:
                return DataTypes.DOUBLE();
            case PgOid.FLOAT8_ARRAY:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case PgOid.NUMERIC:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                return handleNumericWithDecimalMode(precision, scale, decimalMode);
            case PgOid.NUMERIC_ARRAY:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                return DataTypes.ARRAY(handleNumericWithDecimalMode(precision, scale, decimalMode));
            case PgOid.MONEY:
                return handleMoneyWithDecimalMode(
                        dbzConfig.getConfig().getInteger(MONEY_FRACTION_DIGITS), decimalMode);
            case PgOid.CHAR:
            case PgOid.BPCHAR:
                return DataTypes.CHAR(precision);
            case PgOid.CHAR_ARRAY:
            case PgOid.BPCHAR_ARRAY:
                return DataTypes.ARRAY(DataTypes.CHAR(precision));
            case PgOid.VARCHAR:
                return DataTypes.VARCHAR(precision);
            case PgOid.VARCHAR_ARRAY:
                return DataTypes.ARRAY(DataTypes.VARCHAR(precision));
            case PgOid.TEXT:
            case PgOid.POINT:
            case PgOid.UUID:
            case PgOid.JSON:
            case PgOid.JSONB:
            case PgOid.XML:
            case PgOid.INET_OID:
            case PgOid.CIDR_OID:
            case PgOid.MACADDR_OID:
            case PgOid.MACADDR8_OID:
            case PgOid.INT4RANGE_OID:
            case PgOid.NUM_RANGE_OID:
            case PgOid.INT8RANGE_OID:
            case PgOid.TSRANGE_OID:
            case PgOid.TSTZRANGE_OID:
            case PgOid.DATERANGE_OID:
                return DataTypes.STRING();
            case PgOid.TEXT_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PgOid.TIMESTAMP:
                return handleTimestampWithTemporalMode(temporalPrecisionMode, scale);
            case PgOid.TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(
                        handleTimestampWithTemporalMode(temporalPrecisionMode, scale));
            case PgOid.TIMESTAMPTZ:
                return new ZonedTimestampType(scale);
            case PgOid.TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(new ZonedTimestampType(scale));
            case PgOid.TIME:
                return handleTimeWithTemporalMode(temporalPrecisionMode, scale);
            case PgOid.TIME_ARRAY:
                return DataTypes.ARRAY(handleTimeWithTemporalMode(temporalPrecisionMode, scale));
            case PgOid.DATE:
                return handleDateWithTemporalMode(temporalPrecisionMode);
            case PgOid.DATE_ARRAY:
                return DataTypes.ARRAY(handleDateWithTemporalMode(temporalPrecisionMode));
            default:
                if (nativeType == typeRegistry.ltreeOid()) {
                    return DataTypes.STRING();
                } else if (nativeType == typeRegistry.geometryOid()) {
                    return DataTypes.STRING();
                } else if (nativeType == typeRegistry.geographyOid()) {
                    return DataTypes.STRING();
                } else if (nativeType == typeRegistry.citextOid()) {
                    return DataTypes.STRING();
                } else if (nativeType == typeRegistry.hstoreOid()) {
                    return handleHstoreWithHstoreMode(hStoreHandlingMode);
                } else if (nativeType == typeRegistry.ltreeArrayOid()) {
                    return DataTypes.ARRAY(DataTypes.STRING());
                } else if (nativeType == typeRegistry.geometryArrayOid()) {
                    return DataTypes.ARRAY(DataTypes.STRING());
                }
                final PostgresType resolvedType = typeRegistry.get(nativeType);
                if (resolvedType.isEnumType()) {
                    return DataTypes.STRING();
                }
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support Postgres type '%s', Postgres oid '%d' yet",
                                column.typeName(), column.nativeType()));
        }
    }

    public static DataType handleNumericWithDecimalMode(
            int precision, int scale, JdbcValueConverters.DecimalMode mode) {
        switch (mode) {
            case PRECISE:
                if (precision > DecimalType.DEFAULT_SCALE
                        && precision <= DecimalType.MAX_PRECISION) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException("Unknown decimal mode: " + mode);
        }
    }

    public static DataType handleBinaryWithBinaryMode(
            CommonConnectorConfig.BinaryHandlingMode mode) {
        switch (mode) {
            case BYTES:
                return DataTypes.BYTES();
            case BASE64:
            case HEX:
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException("Unknown binary mode: " + mode);
        }
    }

    public static DataType handleMoneyWithDecimalMode(
            int moneyFractionDigits, JdbcValueConverters.DecimalMode mode) {
        switch (mode) {
            case PRECISE:
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, moneyFractionDigits);
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException("Unknown decimal mode: " + mode);
        }
    }

    public static DataType handleIntervalWithIntervalHandlingMode(
            PostgresConnectorConfig.IntervalHandlingMode mode) {
        switch (mode) {
            case NUMERIC:
                return DataTypes.BIGINT();
            case STRING:
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException("Unknown interval mode: " + mode);
        }
    }

    public static DataType handleDateWithTemporalMode(TemporalPrecisionMode mode) {
        switch (mode) {
            case ADAPTIVE:
            case ADAPTIVE_TIME_MICROSECONDS:
            case CONNECT:
                return DataTypes.DATE();
            default:
                throw new IllegalArgumentException("Unknown temporal precision mode: " + mode);
        }
    }

    public static DataType handleTimeWithTemporalMode(TemporalPrecisionMode mode, int scale) {
        switch (mode) {
            case ADAPTIVE:
            case ADAPTIVE_TIME_MICROSECONDS:
            case CONNECT:
                return DataTypes.TIME(scale);
            default:
                throw new IllegalArgumentException("Unknown temporal precision mode: " + mode);
        }
    }

    public static DataType handleTimestampWithTemporalMode(TemporalPrecisionMode mode, int scale) {
        switch (mode) {
            case ADAPTIVE:
            case ADAPTIVE_TIME_MICROSECONDS:
            case CONNECT:
                return DataTypes.TIMESTAMP(scale);
            default:
                throw new IllegalArgumentException("Unknown temporal precision mode: " + mode);
        }
    }

    public static DataType handleHstoreWithHstoreMode(
            PostgresConnectorConfig.HStoreHandlingMode mode) {
        switch (mode) {
            case JSON:
                return DataTypes.STRING();
            case MAP:
                return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
            default:
                throw new IllegalArgumentException("Unknown hstore mode: " + mode);
        }
    }
}
