/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium.converter;

import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Use the SPI way to process and convert Debezium data problems that example date type. Refs:
 * https://github.com/holmofy/debezium-datetime-converter
 *
 * <p>See more: https://debezium.io/documentation/reference/1.9/development/converters.html
 */
public class FlinkCDCFormatConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDCFormatConverter.class);
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timeFormatter;
    private DateTimeFormatter datetimeFormatter;
    private DateTimeFormatter timestampFormatter;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props,
                "format.timestamp",
                p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            LOG.error("The {} setting is illegal:{}.", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        switch (sqlType) {
            case "DATE":
                schemaBuilder =
                        SchemaBuilder.string()
                                .optional()
                                .name("com.ververica.cdc.debezium.date.string");
                converter = this::convertDate;
                break;
            case "TIME":
                schemaBuilder =
                        SchemaBuilder.string()
                                .optional()
                                .name("com.ververica.cdc.debezium.time.string");
                converter = this::convertTime;
                break;
            case "DATETIME":
                schemaBuilder =
                        SchemaBuilder.string()
                                .optional()
                                .name("com.ververica.cdc.debezium.datetime.string");
                converter = this::convertDateTime;
                break;
            case "TIMESTAMP":
                schemaBuilder =
                        SchemaBuilder.string()
                                .optional()
                                .name("com.ververica.cdc.debezium.timestamp.string");
                converter = this::convertTimestamp;
                break;
            default:
                break;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            LOG.info(
                    "Register converter for sqlType {} to schema {}",
                    sqlType,
                    schemaBuilder.name());
        }
    }

    private String convertDate(Object input) {
        if (input == null) {
            return null;
        } else {
            return dateFormatter.format(TemporalConversions.toLocalDate(input));
        }
    }

    private String convertTime(Object input) {
        if (input == null) {
            return null;
        } else {
            return timeFormatter.format(TemporalConversions.toLocalTime(input));
        }
    }

    private String convertDateTime(Object input) {
        if (input == null) {
            return null;
        } else {
            return datetimeFormatter.format(
                    TemporalConversions.toLocalDateTime(input, timestampZoneId));
        }
    }

    private String convertTimestamp(Object input) {
        if (input == null) {
            return null;
        } else {
            return timestampFormatter.format(
                    TemporalConversions.toLocalDateTime(input, timestampZoneId));
        }
    }
}
