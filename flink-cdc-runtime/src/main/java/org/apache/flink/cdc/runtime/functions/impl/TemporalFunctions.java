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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.flink.cdc.common.utils.DateTimeUtils.timestampMillisToDate;
import static org.apache.flink.cdc.common.utils.DateTimeUtils.timestampMillisToTime;

/** Temporal built-in functions. */
public class TemporalFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(TemporalFunctions.class);

    public static LocalZonedTimestampData currentTimestamp(long epochTime) {
        return LocalZonedTimestampData.fromEpochMillis(epochTime);
    }

    // synonym with currentTimestamp
    public static LocalZonedTimestampData now(long epochTime) {
        return LocalZonedTimestampData.fromEpochMillis(epochTime);
    }

    public static TimestampData localtimestamp(long epochTime, String timezone) {
        return TimestampData.fromLocalDateTime(
                Instant.ofEpochMilli(epochTime).atZone(ZoneId.of(timezone)).toLocalDateTime());
    }

    public static TimeData localtime(long epochTime, String timezone) {
        return timestampMillisToTime(localtimestamp(epochTime, timezone).getMillisecond());
    }

    public static TimeData currentTime(long epochTime, String timezone) {
        // the time value of currentTimestamp under given session time zone
        return timestampMillisToTime(localtimestamp(epochTime, timezone).getMillisecond());
    }

    public static DateData currentDate(long epochTime, String timezone) {
        // the date value of currentTimestamp under given session time zone
        return timestampMillisToDate(localtimestamp(epochTime, timezone).getMillisecond());
    }

    public static String fromUnixtime(long seconds, String timezone) {
        return DateTimeUtils.formatUnixTimestamp(seconds, TimeZone.getTimeZone(timezone));
    }

    public static String fromUnixtime(long seconds, String format, String timezone) {
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

    public static String dateFormat(TimestampData timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        // `timezone` is ignored since TimestampData is time-zone insensitive
        return DateTimeUtils.formatTimestampMillis(
                timestamp.getMillisecond(), format, TimeZone.getTimeZone("UTC"));
    }

    public static String dateFormat(
            LocalZonedTimestampData timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        return DateTimeUtils.formatTimestampMillis(
                timestamp.getEpochMillisecond(), format, TimeZone.getTimeZone(timezone));
    }

    public static DateData toDate(String str, String timezone) {
        return toDate(str, "yyyy-MM-dd", timezone);
    }

    public static DateData toDate(String str, String format, String timezone) {
        return DateTimeUtils.parseDate(str, format, timezone);
    }

    public static TimestampData toTimestamp(String str, String timezone) {
        return toTimestamp(str, "yyyy-MM-dd HH:mm:ss", timezone);
    }

    public static TimestampData toTimestamp(String str, String format, String timezone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        try {
            return TimestampData.fromMillis(dateFormat.parse(str).getTime());
        } catch (ParseException e) {
            LOG.error("Unsupported date type convert: {}", str);
            throw new RuntimeException(e);
        }
    }

    // Be compatible with the existing definition of Function TIMESTAMP_DIFF
    public static Integer timestampDiff(
            String timeIntervalUnit,
            LocalZonedTimestampData fromTimestamp,
            LocalZonedTimestampData toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.getEpochMillisecond(),
                timezone,
                toTimestamp.getEpochMillisecond(),
                timezone);
    }

    // Be compatible with the existing definition of Function TIMESTAMP_DIFF
    public static Integer timestampDiff(
            String timeIntervalUnit,
            TimestampData fromTimestamp,
            TimestampData toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.getMillisecond(),
                "UTC",
                toTimestamp.getMillisecond(),
                "UTC");
    }

    // Be compatible with the existing definition of Function TIMESTAMP_DIFF
    public static Integer timestampDiff(
            String timeIntervalUnit,
            TimestampData fromTimestamp,
            LocalZonedTimestampData toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.getMillisecond(),
                "UTC",
                toTimestamp.getEpochMillisecond(),
                timezone);
    }

    // Be compatible with the existing definition of Function TIMESTAMP_DIFF
    public static Integer timestampDiff(
            String timeIntervalUnit,
            LocalZonedTimestampData fromTimestamp,
            TimestampData toTimestamp,
            String timezone) {
        if (fromTimestamp == null || toTimestamp == null) {
            return null;
        }
        return DateTimeUtils.timestampDiff(
                timeIntervalUnit,
                fromTimestamp.getEpochMillisecond(),
                timezone,
                toTimestamp.getMillisecond(),
                "UTC");
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            LocalZonedTimestampData fromTimestamp,
            LocalZonedTimestampData toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            TimestampData fromTimestamp,
            TimestampData toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            TimestampData fromTimestamp,
            LocalZonedTimestampData toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static Integer timestampdiff(
            String timeIntervalUnit,
            LocalZonedTimestampData fromTimestamp,
            TimestampData toTimestamp,
            String timezone) {
        return timestampDiff(timeIntervalUnit, fromTimestamp, toTimestamp, timezone);
    }

    public static LocalZonedTimestampData timestampadd(
            String timeIntervalUnit,
            Integer interval,
            LocalZonedTimestampData timePoint,
            String timezone) {
        if (interval == null || timePoint == null) {
            return null;
        }
        return LocalZonedTimestampData.fromEpochMillis(
                DateTimeUtils.timestampAdd(
                        timeIntervalUnit, interval, timePoint.getEpochMillisecond(), timezone));
    }

    public static TimestampData timestampadd(
            String timeIntervalUnit, Integer interval, TimestampData timePoint, String timezone) {
        if (interval == null || timePoint == null) {
            return null;
        }
        return TimestampData.fromMillis(
                DateTimeUtils.timestampAdd(
                        timeIntervalUnit, interval, timePoint.getMillisecond(), "UTC"));
    }
}
