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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import org.apache.kafka.connect.data.Field;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.ZoneOffset;

/**
 * A custom PostgresValueConverter that correctly handles timestamp conversion to LocalDateTime for
 * dates before 1970-01-01.
 */
public class CustomPostgresValueConverter extends PostgresValueConverter {
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
            int moneyFractionDigits) {
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
    }

    public static CustomPostgresValueConverter of(
            PostgresConnectorConfig connectorConfig,
            Charset databaseCharset,
            TypeRegistry typeRegistry) {
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
                connectorConfig.moneyFractionDigits());
    }

    @Override
    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
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

        return timestamp.toLocalDateTime();
    }
}
