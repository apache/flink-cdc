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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    public static int timestampMillisToDate(long ts) {
        int days = (int) (ts / MILLIS_PER_DAY);
        if (days < 0) {
            days = days - 1;
        }
        return days;
    }

    /**
     * Get time from a timestamp.
     *
     * @param ts the timestamp in milliseconds.
     * @return the time in milliseconds.
     */
    public static int timestampMillisToTime(long ts) {
        return (int) (ts % MILLIS_PER_DAY);
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

    public static int parseDate(String dateStr, String fromFormat, String timezone) {
        long ts = internalParseTimestampMillis(dateStr, fromFormat, TimeZone.getTimeZone(timezone));
        ZoneId zoneId = ZoneId.of(timezone);
        Instant instant = Instant.ofEpochMilli(ts);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        return ymdToUnixDate(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth());
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
                            "Exception when parsing datetime string '%s' in format '%s'",
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
    // Format
    // --------------------------------------------------------------------------------------------

    public static String formatTimestampMillis(long ts, String format, TimeZone timeZone) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(timeZone);
        Date dateTime = new Date(ts);
        return formatter.format(dateTime);
    }
}
