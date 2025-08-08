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

import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TypeRegistry;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.relational.Column;
import org.postgresql.core.Oid;

import java.util.Properties;

/** A utility class for converting Postgres types to Flink types. */
public class PostgresTypeUtils {

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column, Properties dbzProperties, TypeRegistry typeRegistry) {
        DataType dataType = convertFromColumn(column, dbzProperties, typeRegistry);
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
    private static DataType convertFromColumn(Column column, Properties dbzProperties, TypeRegistry typeRegistry) {
        int nativeType = column.nativeType();

        int precision = column.length();
        int scale = column.scale().orElse(0);

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
                return DataTypes.BYTES();
            case PgOid.BYTEA_ARRAY:
                return DataTypes.ARRAY(DataTypes.BYTES());
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
            case PgOid.INTERVAL:
                return DataTypes.BIGINT();
            case PgOid.INT8_ARRAY:
            case PgOid.INTERVAL_ARRAY:
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
                if (precision > 0) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 0);
            case PgOid.NUMERIC_ARRAY:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                if (precision > 0) {
                    return DataTypes.ARRAY(DataTypes.DECIMAL(precision, scale));
                }
                return DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 0));
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
                return DataTypes.STRING();
            case PgOid.TEXT_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PgOid.TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case PgOid.TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP(scale));
            case PgOid.TIMESTAMPTZ:
                return new ZonedTimestampType(scale);
            case PgOid.TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(new ZonedTimestampType(scale));
            case PgOid.TIME:
            case PgOid.TIMETZ:
                return DataTypes.TIME(scale);
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIME(scale));
            case PgOid.DATE:
                return DataTypes.DATE();
            case PgOid.DATE_ARRAY:
                return DataTypes.ARRAY(DataTypes.DATE());
            default:
                if (nativeType == typeRegistry.ltreeOid()) {
                    return DataTypes.STRING();
                }
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Postgres type '%s', Postgres oid '%d' yet", column.typeName(),column.nativeType()));
        }
    }
}
