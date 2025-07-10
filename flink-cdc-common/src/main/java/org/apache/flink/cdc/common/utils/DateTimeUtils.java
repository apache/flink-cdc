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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.TimeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/** Utility functions for datetime types: date, time, timestamp. */
public class DateTimeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);

    /** The julian date of the epoch, 1970-01-01. */
    public static final int EPOCH_JULIAN = 2440588;

    /**
     * The number of milliseconds in a day.
     *
     * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
     */
    public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

    /** The SimpleDateFormat string for ISO dates, "yyyy-MM-dd". */
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

    /** The SimpleDateFormat string for ISO times, "HH:mm:ss". */
    private static final String TIME_FORMAT_STRING = "HH:mm:ss";

    /** The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss". */
    private static final String TIMESTAMP_FORMAT_STRING =
            DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

    /**
     * A ThreadLocal cache map for SimpleDateFormat, because SimpleDateFormat is not thread-safe.
     * (string_format) => formatter
     */
    private static final ThreadLocalCache<String, SimpleDateFormat> FORMATTER_CACHE =
            ThreadLocalCache.of(SimpleDateFormat::new);

    // --------------------------------------------------------------------------------------------
    // TIMESTAMP to  DATE/TIME utils
    // --------------------------------------------------------------------------------------------

    /**
     * Get date from a timestamp.
     *
     * @param ts the timestamp in milliseconds.
     * @return the date in days.
     */
    public static DateData timestampMillisToDate(long ts) {
        long days = ts / MILLIS_PER_DAY;
        if (days < 0) {
            days = days - 1;
        }
        return DateData.fromEpochDay((int) days);
    }

    /**
     * Get time from a timestamp.
     *
     * @param ts the timestamp in milliseconds.
     * @return the time in milliseconds.
     */
    public static TimeData timestampMillisToTime(long ts) {
        return TimeData.fromMillisOfDay((int) (ts % MILLIS_PER_DAY));
    }

    // --------------------------------------------------------------------------------------------
    // Parsing functions
    // --------------------------------------------------------------------------------------------
    /** Returns the epoch days since 1970-01-01. */
    public static int parseDate(String dateStr, String fromFormat) {
        // It is OK to use UTC, we just want get the epoch days
        // TODO  use offset, better performance
        long ts = internalParseTimestampMillis(dateStr, fromFormat, TimeZone.getTimeZone("UTC"));
        ZoneId zoneId = ZoneId.of("UTC");
        Instant instant = Instant.ofEpochMilli(ts);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        return ymdToUnixDate(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth());
    }

    public static DateData parseDate(String dateStr, String fromFormat, String timezone) {
        long ts = internalParseTimestampMillis(dateStr, fromFormat, TimeZone.getTimeZone(timezone));
        ZoneId zoneId = ZoneId.of(timezone);
        Instant instant = Instant.ofEpochMilli(ts);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        return DateData.fromLocalDate(zdt.toLocalDate());
    }

    private static long internalParseTimestampMillis(String dateStr, String format, TimeZone tz) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(tz);
        try {
            Date date = formatter.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            LOG.error(
                    String.format(
                            "Exception when parsing datetime string '%s' in format '%s', the default value Long.MIN_VALUE(-9223372036854775808) will be returned.",
                            dateStr, format),
                    e);
            return Long.MIN_VALUE;
        }
    }

    private static int ymdToUnixDate(int year, int month, int day) {
        final int julian = ymdToJulian(year, month, day);
        return julian - EPOCH_JULIAN;
    }

    private static int ymdToJulian(int year, int month, int day) {
        int a = (14 - month) / 12;
        int y = year + 4800 - a;
        int m = month + 12 * a - 3;
        return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    }

    // --------------------------------------------------------------------------------------------
    // UNIX TIME
    // --------------------------------------------------------------------------------------------

    /**
     * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string in the
     * "yyyy-MM-dd HH:mm:ss" format.
     */
    public static String formatUnixTimestamp(long unixTime, TimeZone timeZone) {
        return formatUnixTimestamp(unixTime, TIMESTAMP_FORMAT_STRING, timeZone);
    }

    /**
     * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string in the
     * given format.
     */
    public static String formatUnixTimestamp(long unixTime, String format, TimeZone timeZone) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(timeZone);
        Date date = new Date(unixTime * 1000);
        try {
            return formatter.format(date);
        } catch (Exception e) {
            LOG.error("Exception when formatting.", e);
            return null;
        }
    }

    /**
     * Returns the value of the argument as an unsigned integer in seconds since '1970-01-01
     * 00:00:00' UTC.
     */
    public static long unixTimestamp(String dateStr, TimeZone timeZone) {
        return unixTimestamp(dateStr, TIMESTAMP_FORMAT_STRING, timeZone);
    }

    /**
     * Returns the value of the argument as an unsigned integer in seconds since '1970-01-01
     * 00:00:00' UTC.
     */
    public static long unixTimestamp(String dateStr, String format, TimeZone timeZone) {
        long ts = internalParseTimestampMillis(dateStr, format, timeZone);
        if (ts == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        } else {
            // return the seconds
            return ts / 1000;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Format
    // --------------------------------------------------------------------------------------------

    public static String formatTimestampMillis(long ts, String format, TimeZone timeZone) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(timeZone);
        Date dateTime = new Date(ts);
        return formatter.format(dateTime);
    }

    // --------------------------------------------------------------------------------------------
    // Compare
    // --------------------------------------------------------------------------------------------

    public static Integer timestampDiff(
            String timeIntervalUnit,
            long fromDate,
            String fromTimezone,
            long toDate,
            String toTimezone) {
        Calendar from = Calendar.getInstance(TimeZone.getTimeZone(fromTimezone));
        from.setTime(new Date(fromDate));
        Calendar to = Calendar.getInstance(TimeZone.getTimeZone(toTimezone));
        to.setTime(new Date(toDate));
        long second = (to.getTimeInMillis() - from.getTimeInMillis()) / 1000;
        switch (timeIntervalUnit) {
            case "SECOND":
                if (second > Integer.MAX_VALUE) {
                    return null;
                }
                return (int) second;
            case "MINUTE":
                if (second > Integer.MAX_VALUE) {
                    return null;
                }
                return (int) second / 60;
            case "HOUR":
                if (second > Integer.MAX_VALUE) {
                    return null;
                }
                return (int) second / 3600;
            case "DAY":
                if (second > Integer.MAX_VALUE) {
                    return null;
                }
                return (int) second / (24 * 3600);
            case "MONTH":
                return to.get(Calendar.YEAR) * 12
                        + to.get(Calendar.MONTH)
                        - (from.get(Calendar.YEAR) * 12 + from.get(Calendar.MONTH));
            case "YEAR":
                return to.get(Calendar.YEAR) - from.get(Calendar.YEAR);
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported timestamp interval unit %s. Supported units are: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR",
                                timeIntervalUnit));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Add
    // --------------------------------------------------------------------------------------------

    public static long timestampAdd(
            String timeIntervalUnit, int interval, long timePoint, String timezone) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone(timezone));
        calendar.setTime(new Date(timePoint));
        int field;
        switch (timeIntervalUnit) {
            case "SECOND":
                field = Calendar.SECOND;
                break;
            case "MINUTE":
                field = Calendar.MINUTE;
                break;
            case "HOUR":
                field = Calendar.HOUR;
                break;
            case "DAY":
                field = Calendar.DATE;
                break;
            case "MONTH":
                field = Calendar.MONTH;
                break;
            case "YEAR":
                field = Calendar.YEAR;
                break;
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported timestamp interval unit %s. Supported units are: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR",
                                timeIntervalUnit));
        }
        calendar.add(field, interval);
        return calendar.getTimeInMillis();
    }
}
