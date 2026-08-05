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

package org.apache.flink.cdc.runtime.functions.impl;

import org.apache.flink.cdc.common.utils.DateTimeUtils;
import org.apache.flink.cdc.common.utils.ThreadLocalCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.TimeZone;

/** Temporal built-in functions. */
public class TemporalFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(TemporalFunctions.class);

    private static final long MILLIS_PER_DAY = 24L * 60 * 60 * 1000;
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final ThreadLocalCache<String, DateTimeFormatter> FORMATTER_CACHE =
            ThreadLocalCache.of(DateTimeFormatter::ofPattern);

    public static Instant currentTimestamp(long epochTime) {
        return Instant.ofEpochMilli(epochTime);
    }

    // synonym of CURRENT_TIMESTAMP.
    public static Instant now(long epochTime) {
        return currentTimestamp(epochTime);
    }

    public static LocalDateTime localtimestamp(long epochTime, String timezone) {
        return currentTimestamp(epochTime).atZone(ZoneId.of(timezone)).toLocalDateTime();
    }

    public static LocalTime localtime(long epochTime, String timezone) {
        return localtimestamp(epochTime, timezone).toLocalTime();
    }

    // synonym of LOCALTIME.
    public static LocalTime currentTime(long epochTime, String timezone) {
        return localtimestamp(epochTime, timezone).toLocalTime();
    }

    public static LocalDate currentDate(long epochTime, String timezone) {
        return localtimestamp(epochTime, timezone).toLocalDate();
    }

    public static Long extract(String unit, TemporalAccessor temporal, String timezone) {
        if (temporal == null) {
            return null;
        }
        if (temporal instanceof Instant) {
            temporal = ((Instant) temporal).atZone(ZoneId.of(timezone));
        }
        switch (unit) {
            case "YEAR":
                return getTemporalField(temporal, ChronoField.YEAR, unit);
            case "QUARTER":
                return getTemporalField(temporal, IsoFields.QUARTER_OF_YEAR, unit);
            case "MONTH":
                return getTemporalField(temporal, ChronoField.MONTH_OF_YEAR, unit);
            case "WEEK":
                return getTemporalField(temporal, IsoFields.WEEK_OF_WEEK_BASED_YEAR, unit);
            case "DAY":
                return getTemporalField(temporal, ChronoField.DAY_OF_MONTH, unit);
            case "DOY":
                return getTemporalField(temporal, ChronoField.DAY_OF_YEAR, unit);
            case "DOW":
                // SQL DOW starts with Sunday as 1, while ISO starts with Monday as 1.
                return getTemporalField(temporal, ChronoField.DAY_OF_WEEK, unit) % 7 + 1;
            case "HOUR":
                return getTemporalField(temporal, ChronoField.HOUR_OF_DAY, unit);
            case "MINUTE":
                return getTemporalField(temporal, ChronoField.MINUTE_OF_HOUR, unit);
            case "SECOND":
                return getTemporalField(temporal, ChronoField.SECOND_OF_MINUTE, unit);
            default:
                throw new IllegalArgumentException("Unsupported EXTRACT unit: " + unit);
        }
    }

    private static long getTemporalField(
            TemporalAccessor temporal, java.time.temporal.TemporalField field, String unit) {
        if (temporal instanceof LocalDate
                && (field == ChronoField.HOUR_OF_DAY
                        || field == ChronoField.MINUTE_OF_HOUR
                        || field == ChronoField.SECOND_OF_MINUTE)) {
            return 0L;
        }
        if (!temporal.isSupported(field)) {
            throw new IllegalArgumentException(
                    String.format(
                            "EXTRACT unit %s cannot be applied to %s",
                            unit, temporal.getClass().getSimpleName()));
        }
        return temporal.getLong(field);
    }

    public static LocalDate temporalPlusMonths(LocalDate temporal, long months) {
        return temporal == null ? null : temporal.plusMonths(months);
    }

    public static LocalTime temporalPlusMonths(LocalTime temporal, long months) {
        // Year-month intervals do not affect TIME values.
        return temporal;
    }

    public static LocalDateTime temporalPlusMonths(LocalDateTime temporal, long months) {
        return temporal == null ? null : temporal.plusMonths(months);
    }

    public static Instant temporalPlusMonths(Instant temporal, long months) {
        // TIMESTAMP_LTZ month arithmetic follows UTC calendar semantics.
        return temporal == null
                ? null
                : temporal.atZone(ZoneId.of("UTC")).plusMonths(months).toInstant();
    }

    public static LocalDate temporalPlusMillis(LocalDate temporal, long millis) {
        // DATE arithmetic uses only the whole-day portion of a day-time interval.
        return temporal == null ? null : temporal.plusDays(millis / MILLIS_PER_DAY);
    }

    public static LocalTime temporalPlusMillis(LocalTime temporal, long millis) {
        return temporal == null ? null : temporal.plus(millis, ChronoUnit.MILLIS);
    }

    public static LocalDateTime temporalPlusMillis(LocalDateTime temporal, long millis) {
        return temporal == null ? null : temporal.plus(millis, ChronoUnit.MILLIS);
    }

    public static Instant temporalPlusMillis(Instant temporal, long millis) {
        return temporal == null ? null : temporal.plusMillis(millis);
    }

    public static String fromUnixtime(Integer seconds, String timezone) {
        if (seconds == null) {
            return null;
        }
        return fromUnixtime(seconds.longValue(), timezone);
    }

    public static String fromUnixtime(Integer seconds, String format, String timezone) {
        if (seconds == null) {
            return null;
        }
        return fromUnixtime(seconds.longValue(), format, timezone);
    }

    public static String fromUnixtime(Long seconds, String timezone) {
        if (seconds == null) {
            return null;
        }
        return DateTimeUtils.formatUnixTimestamp(seconds, TimeZone.getTimeZone(timezone));
    }

    public static String fromUnixtime(Long seconds, String format, String timezone) {
        if (seconds == null) {
            return null;
        }
        return DateTimeUtils.formatUnixTimestamp(seconds, format, TimeZone.getTimeZone(timezone));
    }

    public static long unixTimestamp(long epochTime, String timezone) {
        return epochTime / 1000;
    }

    public static long unixTimestamp(String dateTimeStr, long epochTime, String timezone) {
        return DateTimeUtils.unixTimestamp(dateTimeStr, TimeZone.getTimeZone(timezone));
    }

    public static long unixTimestamp(
            String dateTimeStr, String format, long epochTime, String timezone) {
        return DateTimeUtils.unixTimestamp(dateTimeStr, format, TimeZone.getTimeZone(timezone));
    }

    public static String dateFormat(Instant timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return DateTimeUtils.formatInstant(timestamp, format, TimeZone.getTimeZone(timezone));
    }

    public static String dateFormat(LocalDateTime timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return DateTimeUtils.formatInstant(
                timestamp.atZone(ZoneId.of(timezone)).toInstant(),
                format,
                TimeZone.getTimeZone(timezone));
    }

    public static String dateFormat(LocalDate date, String format, String timezone) {
        // `timezone` is ignored since DateData is time-zone insensitive
        if (date == null) {
            return null;
        }
        return date.format(FORMATTER_CACHE.get(format));
    }

    public static String dateFormat(LocalTime time, String format, String timezone) {
        // `timezone` is ignored since TimeData is time-zone insensitive
        if (time == null) {
            return null;
        }
        return time.format(FORMATTER_CACHE.get(format));
    }

    public static LocalDate toDate(String str) {
        return DateTimeUtils.parseDate(str, "yyyy-MM-dd");
    }

    public static LocalDate toDate(String str, String format) {
        return DateTimeUtils.parseDate(str, format);
    }

    public static LocalDate toDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return localDateTime.toLocalDate();
    }

    public static LocalDate toDate(ZonedDateTime zonedDateTime) {
        if (zonedDateTime == null) {
            return null;
        }
        return zonedDateTime.toLocalDate();
    }

    public static LocalDate toDate(Instant instant) {
        if (instant == null) {
            return null;
        }
        return LocalDateTime.ofInstant(instant, ZoneId.of("UTC")).toLocalDate();
    }

    public static LocalDateTime toTimestamp(String str, String timezone) {
        return toTimestamp(str, "yyyy-MM-dd HH:mm:ss", timezone);
    }

    public static LocalDateTime toTimestamp(String str, String format, String timezone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        try {
            return LocalDateTime.ofInstant(dateFormat.parse(str).toInstant(), ZoneId.of(timezone));
        } catch (ParseException e) {
            LOG.error("Unsupported date type convert: {}", str);
            throw new RuntimeException(e);
        }
    }

    public static Integer timestampDiff(
            String timeIntervalUnit,
            LocalDateTime fromTimestamp,
            LocalDateTime toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.atZone(ZoneId.of(timezone)).toInstant(),
                timezone,
                toTimestamp.atZone(ZoneId.of(timezone)).toInstant(),
                timezone);
    }

    public static Integer timestampDiff(
            String timeIntervalUnit, Instant fromTimestamp, Instant toTimestamp, String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit, fromTimestamp, "UTC", toTimestamp, "UTC");
    }

    public static Integer timestampDiff(
            String timeIntervalUnit,
            Instant fromTimestamp,
            LocalDateTime toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp,
                "UTC",
                toTimestamp.atZone(ZoneId.of(timezone)).toInstant(),
                timezone);
    }

    public static Integer timestampDiff(
            String timeIntervalUnit,
            LocalDateTime fromTimestamp,
            Instant toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.atZone(ZoneId.of(timezone)).toInstant(),
                timezone,
                toTimestamp,
                "UTC");
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            LocalDateTime fromTimestamp,
            LocalDateTime toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit, Instant fromTimestamp, Instant toTimestamp, String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            Instant fromTimestamp,
            LocalDateTime toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            LocalDateTime fromTimestamp,
            Instant toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static LocalDateTime timestampadd(
            String timeIntervalUnit, Integer interval, LocalDateTime timePoint, String timezone) {
        if (interval == null || timePoint == null) {
            return null;
        }
        return LocalDateTime.ofInstant(
                DateTimeUtils.timestampAdd(
                        timeIntervalUnit,
                        interval,
                        timePoint.atZone(ZoneId.of(timezone)).toInstant(),
                        timezone),
                ZoneId.of(timezone));
    }

    public static Instant timestampadd(
            String timeIntervalUnit, Integer interval, Instant timePoint, String timezone) {
        if (interval == null || timePoint == null) {
            return null;
        }
        return DateTimeUtils.timestampAdd(timeIntervalUnit, interval, timePoint, "UTC");
    }

    // TO_TIMESTAMP_LTZ function series
    public static Instant toTimestampLtz(Long epochMillis) {
        if (epochMillis == null) {
            return null;
        }
        return Instant.ofEpochMilli(epochMillis);
    }

    public static Instant toTimestampLtz(Long timestamp, Integer precision) {
        if (timestamp == null) {
            return null;
        }
        if (precision == 0) {
            return Instant.ofEpochSecond(timestamp);
        } else if (precision == 3) {
            return Instant.ofEpochMilli(timestamp);
        } else {
            throw new IllegalArgumentException(
                    "TO_TIMESTAMP_LTZ precision must be 0 or 3, but get: " + precision);
        }
    }

    public static Instant toTimestampLtz(String timestamp) {
        return toTimestampLtz(timestamp, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static Instant toTimestampLtz(String timestamp, String format) {
        return toTimestampLtz(timestamp, format, "UTC");
    }

    public static Instant toTimestampLtz(String timestamp, String format, String zoneId) {
        return LocalDateTime.parse(timestamp, FORMATTER_CACHE.get(format))
                .atZone(ZoneId.of(zoneId))
                .toInstant();
    }

    public static String dateFormatTz(LocalDateTime timestamp, String timezone) {
        return dateFormatTz(timestamp, DEFAULT_DATETIME_FORMAT, timezone);
    }

    public static String dateFormatTz(LocalDateTime timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return timestamp.atZone(ZoneId.of(timezone)).format(FORMATTER_CACHE.get(format));
    }

    public static String dateFormatTz(Instant timestamp, String timezone) {
        return dateFormatTz(timestamp, DEFAULT_DATETIME_FORMAT, timezone);
    }

    public static String dateFormatTz(Instant timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return timestamp.atZone(ZoneId.of(timezone)).format(FORMATTER_CACHE.get(format));
    }

    public static String dateAdd(LocalDateTime timestamp, int days, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return timestamp
                .toLocalDate()
                .plusDays(days)
                .format(FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT));
    }

    public static String dateAdd(Instant timestamp, int days, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return timestamp
                .atZone(ZoneId.of(timezone))
                .plusDays(days)
                .format(FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT));
    }

    public static String dateAdd(LocalDate timestamp, int days, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return timestamp.plusDays(days).format(FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT));
    }

    public static String dateAdd(String timestamp, int days, String timezone) {
        if (timestamp == null) {
            return null;
        }

        // yyyy-MM-dd
        if (timestamp.length() == 10) {
            return LocalDate.parse(timestamp, FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT))
                    .plusDays(days)
                    .format(FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT));
        }

        // yyyy-MM-dd HH:mm:ss
        if (timestamp.length() == 19) {
            return LocalDateTime.parse(timestamp, FORMATTER_CACHE.get(DEFAULT_DATETIME_FORMAT))
                    .toLocalDate()
                    .plusDays(days)
                    .format(FORMATTER_CACHE.get(DEFAULT_DATE_FORMAT));
        }

        throw new IllegalArgumentException("Unparseable timestamp: " + timestamp);
    }
}
