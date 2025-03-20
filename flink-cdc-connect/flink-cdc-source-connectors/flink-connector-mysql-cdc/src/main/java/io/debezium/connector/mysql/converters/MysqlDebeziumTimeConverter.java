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

package io.debezium.connector.mysql.converters;

import org.apache.flink.cdc.debezium.utils.ConvertTimeBceUtil;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.Conversions;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Debezium converts the datetime type in MySQL into a UTC timestamp by default ({@link
 * io.debezium.time.Timestamp} ), The time zone is hard-coded and cannot be changed. causing
 * conversion errors part of the time Enable this converter to convert the four times "DATE",
 * "DATETIME", "TIME", and "TIMESTAMP" into the format corresponding to the configured time zone
 * (for example, yyyy-MM-dd)
 *
 * @see io.debezium.connector.mysql.converters.TinyIntOneToBooleanConverter
 */
public class MysqlDebeziumTimeConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger log = LoggerFactory.getLogger(MysqlDebeziumTimeConverter.class);

    private static boolean loggedUnknownTimestampClass = false;
    private static boolean loggedUnknownDateClass = false;
    private static boolean loggedUnknownTimeClass = false;
    private static boolean loggedUnknownTimestampWithTimeZoneClass = false;

    private final String DATE = "DATE";
    private final String DATETIME = "DATETIME";
    private final String TIME = "TIME";
    private final String TIMESTAMP = "TIMESTAMP";
    private final String[] DATE_TYPES = {DATE, DATETIME, TIME, TIMESTAMP};

    protected static final String DATE_FORMAT = "yyyy-MM-dd";
    protected static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected ZoneId zoneId;
    protected static final String DEFAULT_DATE_FORMAT_PATTERN = "1970-01-01 00:00:00";
    protected DateTimeFormatter dateFormatter;
    protected DateTimeFormatter timeFormatter;
    protected DateTimeFormatter datetimeFormatter;
    protected DateTimeFormatter timestampFormatter;
    protected String schemaNamePrefix;
    protected static DateTimeFormatter originalFormat =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected Boolean parseNullDefaultValue = true;

    @Override
    public void configure(Properties properties) {
        String dateFormat = properties.getProperty("format.date", DATE_FORMAT);
        String timeFormat = properties.getProperty("format.time", TIME_FORMAT);
        String datetimeFormat = properties.getProperty("format.datetime", DATETIME_FORMAT);
        String timestampFormat = properties.getProperty("format.timestamp", DATETIME_FORMAT);
        this.parseNullDefaultValue =
                Boolean.parseBoolean(
                        properties.getProperty("format.default.value.convert", "true"));
        String className = this.getClass().getName();
        this.schemaNamePrefix = properties.getProperty("schema.name.prefix", className + ".mysql");
        this.dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        this.datetimeFormatter = DateTimeFormatter.ofPattern(datetimeFormat);
        this.timestampFormatter = DateTimeFormatter.ofPattern(timestampFormat);
        this.zoneId =
                ZoneId.of(
                        properties.getProperty(
                                "format.timezone", ZoneId.systemDefault().toString()));
    }

    @Override
    public void converterFor(
            final RelationalColumn field, final ConverterRegistration<SchemaBuilder> registration) {
        if (Arrays.stream(DATE_TYPES).anyMatch(s -> s.equalsIgnoreCase(field.typeName()))) {
            registerDateConverter(field, registration);
        }
    }

    private void registerDateConverter(
            final RelationalColumn field, final ConverterRegistration<SchemaBuilder> registration) {
        String columnType = field.typeName().toUpperCase();
        String schemaName = this.schemaNamePrefix + "." + columnType.toLowerCase();
        registration.register(
                SchemaBuilder.string().name(schemaName).optional(),
                value -> {
                    try {
                        return convertDateObject(field, value, columnType);
                    } catch (Exception e) {
                        logConvertDateError(field, value);
                        throw new RuntimeException("MysqlDebeziumConverter error", e);
                    }
                });
    }

    private void logConvertDateError(RelationalColumn field, Object value) {
        String fieldName = field.name();
        String fieldType = field.typeName().toUpperCase();
        String defaultValue = "null";
        if (field.hasDefaultValue() && field.defaultValue() != null) {
            defaultValue = field.defaultValue().toString();
        }
        log.error(
                "Find schema need to change dateType, but failed. Field name:{}, field type:{}, "
                        + "field value:{}, field default value:{}",
                fieldName,
                fieldType,
                value == null ? "null" : value,
                defaultValue);
    }

    private Object convertDateObject(RelationalColumn field, Object value, String columnType) {
        if (value == null) {
            return convertDateDefaultValue(field);
        }
        switch (columnType.toUpperCase(Locale.ROOT)) {
            case "DATE":
                if (value instanceof Integer) {
                    return this.convertToDate(columnType, LocalDate.ofEpochDay((Integer) value));
                }
                return this.convertToDate(columnType, value);
            case "TIME":
                if (value instanceof Long) {
                    long l = Math.multiplyExact((Long) value, TimeUnit.MICROSECONDS.toNanos(1));
                    return this.convertToTime(columnType, LocalTime.ofNanoOfDay(l));
                }
                return this.convertToTime(columnType, value);
            case "DATETIME":
                if (value instanceof Long) {
                    if (getTimePrecision(field) <= 3) {
                        return this.convertToTimestamp(
                                columnType, Conversions.toInstantFromMillis((Long) value));
                    }
                    if (getTimePrecision(field) <= 6) {
                        return this.convertToTimestamp(
                                columnType, Conversions.toInstantFromMicros((Long) value));
                    }
                }
                return this.convertToTimestamp(columnType, value);
            case "TIMESTAMP":
                return this.convertToTimestampWithTimezone(columnType, value);
            default:
                throw new IllegalArgumentException(
                        "Unknown field type  " + columnType.toUpperCase(Locale.ROOT));
        }
    }

    private Object convertToTimestampWithTimezone(String columnType, Object timestamp) {
        // In snapshot mode, debezium produces a java.sql.Timestamp object for the TIMESTAMPTZ type.
        // Conceptually, a timestamp with timezone is an Instant. But t.toInstant() actually
        // mangles the value for ancient dates, because leap years weren't applied consistently in
        // ye olden days. Additionally, toInstant() (and toLocalDateTime()) actually lose the era
        // indicator,
        // so we can't rely on their getEra() methods.
        // So we have special handling for this case, which sidesteps the toInstant conversion.
        if (timestamp instanceof Timestamp) {
            Timestamp value = (Timestamp) timestamp;
            ZonedDateTime zonedDateTime = value.toInstant().atZone(zoneId);
            return ConvertTimeBceUtil.resolveEra(value, zonedDateTime.format(timestampFormatter));
        } else if (timestamp instanceof OffsetDateTime) {
            OffsetDateTime value =
                    ((OffsetDateTime) timestamp).toInstant().atZone(zoneId).toOffsetDateTime();
            return ConvertTimeBceUtil.resolveEra(
                    value.toLocalDate(), value.format(timestampFormatter));
        } else if (timestamp instanceof ZonedDateTime) {
            ZonedDateTime zonedDateTime = ((ZonedDateTime) timestamp).toInstant().atZone(zoneId);
            return ConvertTimeBceUtil.resolveEra(
                    zonedDateTime.toLocalDate(), zonedDateTime.format(timestampFormatter));
        } else if (timestamp instanceof Instant) {
            OffsetDateTime dateTime = OffsetDateTime.ofInstant((Instant) timestamp, zoneId);
            ZonedDateTime timestampZt = ZonedDateTime.from(dateTime);
            LocalDate localDate = timestampZt.toLocalDate();
            return ConvertTimeBceUtil.resolveEra(localDate, timestampZt.format(timestampFormatter));
        } else {
            if (!loggedUnknownTimestampWithTimeZoneClass) {
                printUnknownDateClassLogs(columnType, timestamp);
                loggedUnknownTimestampWithTimeZoneClass = true;
            }
            // If init 1970-01-01T00:00:00Zd need to change
            Instant instant = Instant.parse(timestamp.toString());
            OffsetDateTime dateTime = OffsetDateTime.ofInstant(instant, zoneId);
            ZonedDateTime timestampZt = ZonedDateTime.from(dateTime);
            LocalDate localDate = timestampZt.toLocalDate();
            return ConvertTimeBceUtil.resolveEra(localDate, timestampZt.format(timestampFormatter));
        }
    }

    private Object convertToTimestamp(String columnType, Object timestamp) {
        if (timestamp instanceof Timestamp) {
            // Snapshot mode
            LocalDateTime localDateTime = ((Timestamp) timestamp).toLocalDateTime();
            return ConvertTimeBceUtil.resolveEra(
                    ((Timestamp) timestamp), localDateTime.format(datetimeFormatter));
        } else if (timestamp instanceof Instant) {
            // Incremental mode
            Instant time = (Instant) timestamp;
            ZonedDateTime zonedDateTime = time.atZone(zoneId);
            return ConvertTimeBceUtil.resolveEra(
                    zonedDateTime.toLocalDate(),
                    time.atOffset(zonedDateTime.getOffset())
                            .toLocalDateTime()
                            .format(datetimeFormatter));
        } else if (timestamp instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) timestamp;
            LocalDate localDateTime = dateTime.toLocalDate();
            return ConvertTimeBceUtil.resolveEra(localDateTime, dateTime.format(datetimeFormatter));
        }
        if (!loggedUnknownTimestampClass) {
            printUnknownDateClassLogs(columnType, timestamp);
            loggedUnknownTimestampClass = true;
        }
        LocalDateTime localDateTime = LocalDateTime.parse(timestamp.toString());
        LocalDate localDate = localDateTime.toLocalDate();
        return ConvertTimeBceUtil.resolveEra(localDate, localDateTime.format(datetimeFormatter));
    }

    private Object convertToTime(String columnType, Object time) {
        if (time instanceof Time) {
            return formatTime(((Time) time).toLocalTime());
        } else if (time instanceof LocalTime) {
            return formatTime((LocalTime) time);
        } else if (time instanceof java.time.Duration) {
            long value = ((java.time.Duration) time).toNanos();
            if (value >= 0 && value < TimeUnit.DAYS.toNanos(1)) {
                return formatTime(LocalTime.ofNanoOfDay(value));
            } else {
                long updatedValue = Math.min(Math.abs(value), LocalTime.MAX.toNanoOfDay());
                log.debug(
                        "Time values must use number of nanoseconds greater than 0 and less than 86400000000000 but its {}, "
                                + "converting to {} ",
                        value,
                        updatedValue);
                return formatTime(LocalTime.ofNanoOfDay(updatedValue));
            }
        } else {
            if (!loggedUnknownTimeClass) {
                printUnknownDateClassLogs(columnType, time);
                loggedUnknownTimeClass = true;
            }

            String valueAsString = time.toString();
            if (valueAsString.startsWith("24")) {
                log.debug("Time value {} is above range, converting to 23:59:59", valueAsString);
                return LocalTime.MAX.toString();
            }
            return formatTime(LocalTime.parse(valueAsString));
        }
    }

    private String formatTime(LocalTime localTime) {
        return localTime.format(timeFormatter);
    }

    private int getTimePrecision(final RelationalColumn field) {
        return field.length().orElse(-1);
    }

    private String convertToDate(String columnType, Object date) {
        if (date instanceof Date) {
            // Snapshot mode
            LocalDate localDate = ((Date) date).toLocalDate();
            return ConvertTimeBceUtil.resolveEra(localDate, localDate.format(dateFormatter));
        } else if (date instanceof LocalDate) {
            // Incremental mode
            return dateFormatter.format((LocalDate) date);
        } else if (date instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) date);
        } else if (date instanceof Integer) {
            return LocalDate.ofEpochDay(((Integer) date).longValue()).format(dateFormatter);
        } else {
            if (!loggedUnknownDateClass) {
                printUnknownDateClassLogs(columnType, date);
                loggedUnknownDateClass = true;
            }
            LocalDate localDate = LocalDate.parse(date.toString());
            return ConvertTimeBceUtil.resolveEra(localDate, localDate.format(dateFormatter));
        }
    }

    public Object convertDateDefaultValue(RelationalColumn field) {
        if (field.isOptional()) {
            return null;
        } else if (field.hasDefaultValue()) {
            // There is an extreme case where the field defaultValue is 0, resulting in a Kafka
            // Schema mismatch
            if (parseNullDefaultValue) {
                LocalDateTime dateTime =
                        LocalDateTime.parse(DEFAULT_DATE_FORMAT_PATTERN, originalFormat);
                String columnType = field.typeName().toUpperCase();
                switch (columnType.toUpperCase(Locale.ROOT)) {
                    case DATE:
                        return dateTime.format(dateFormatter);
                    case DATETIME:
                        return dateTime.format(datetimeFormatter);
                    case TIME:
                        return dateTime.format(timeFormatter);
                    case TIMESTAMP:
                        return dateTime.format(timestampFormatter);
                }
            }
        }
        return null;
    }

    private static void printUnknownDateClassLogs(String type, Object value) {
        log.warn(
                "MySql Date Convert Database type : {} Unknown class for Date data type {}",
                type,
                value.getClass());
    }
}
