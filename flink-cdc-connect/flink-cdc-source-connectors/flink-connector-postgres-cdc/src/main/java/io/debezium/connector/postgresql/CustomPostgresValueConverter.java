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

package io.debezium.connector.postgresql;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import org.apache.kafka.connect.data.Field;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.ZoneOffset;

/**
 * A {@link PostgresValueConverter} which is able to keep the date and time (wall clock) stored in
 * PostgreSQL for {@code timestamp} values, instead of letting the time zone of the JVM shift them.
 *
 * <p>This behavior is only enabled by {@link
 * PostgresSourceOptions#SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED}, otherwise the
 * conversion of the parent implementation is kept. Only {@code timestamp} (without time zone)
 * values are affected, timestamp values with time zone always use the parent implementation, and
 * the schema of the converted values is never changed. The constructor and factory mirror the
 * Debezium implementation and must remain aligned with it when the Debezium version changes.
 */
public class CustomPostgresValueConverter extends PostgresValueConverter {

    private final boolean wallClockConversionEnabled;

    protected CustomPostgresValueConverter(
            Charset databaseCharset,
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            ZoneOffset defaultOffset,
            BigIntUnsignedMode bigIntUnsignedMode,
            boolean includeUnknownDatatypes,
            TypeRegistry typeRegistry,
            PostgresConnectorConfig.HStoreHandlingMode hStoreMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode,
            PostgresConnectorConfig.IntervalHandlingMode intervalMode,
            byte[] toastPlaceholder,
            int moneyFractionDigits,
            boolean wallClockConversionEnabled) {
        super(
                databaseCharset,
                decimalMode,
                temporalPrecisionMode,
                defaultOffset,
                bigIntUnsignedMode,
                includeUnknownDatatypes,
                typeRegistry,
                hStoreMode,
                binaryMode,
                intervalMode,
                toastPlaceholder,
                moneyFractionDigits);
        this.wallClockConversionEnabled = wallClockConversionEnabled;
    }

    public static CustomPostgresValueConverter of(
            PostgresConnectorConfig connectorConfig,
            Charset databaseCharset,
            TypeRegistry typeRegistry) {
        Configuration config = connectorConfig.getConfig();
        boolean wallClockConversionEnabled =
                config.getBoolean(
                        PostgresSourceOptions.SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED
                                .key(),
                        PostgresSourceOptions.SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED
                                .defaultValue());
        return new CustomPostgresValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                connectorConfig.includeUnknownDatatypes(),
                typeRegistry,
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.intervalHandlingMode(),
                connectorConfig.getUnavailableValuePlaceholder(),
                connectorConfig.moneyFractionDigits(),
                wallClockConversionEnabled);
    }

    /**
     * Converts a PostgreSQL {@code timestamp} to {@link java.time.LocalDateTime} with the date and
     * time (wall clock) stored in the database, if {@link
     * PostgresSourceOptions#SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED} is enabled.
     *
     * <p>The parent implementation converts through {@link Timestamp#toInstant()}, which applies
     * the historical offsets of the JVM default time zone and may thus shift the date and time
     * fields of values before 1970-01-01. The column and field definitions are intentionally unused
     * because this conversion only depends on the JDBC value.
     */
    @Override
    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (!wallClockConversionEnabled) {
            // Keep the behavior of Debezium for backward compatibility.
            return super.convertTimestampToLocalDateTime(column, fieldDefn, data);
        }
        if (data == null) {
            return null;
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }
        final Timestamp timestamp = (Timestamp) data;

        if (POSITIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return POSITIVE_INFINITY_LOCAL_DATE_TIME;
        } else if (NEGATIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return NEGATIVE_INFINITY_LOCAL_DATE_TIME;
        }

        // Preserve the timestamp's wall-clock fields instead of applying historical zone offsets.
        return timestamp.toLocalDateTime();
    }
}
