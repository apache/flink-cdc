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

package org.apache.flink.cdc.runtime.functions;

import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** System function utils to support the call of flink cdc pipeline transform. */
public class SystemFunctionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SystemFunctionUtils.class);

    public static int localtime(long epochTime, String timezone) {
        return DateTimeUtils.timestampMillisToTime(epochTime);
    }

    public static TimestampData localtimestamp(long epochTime, String timezone) {
        return TimestampData.fromMillis(epochTime);
    }

    // synonym: localtime
    public static int currentTime(long epochTime, String timezone) {
        return localtime(epochTime, timezone);
    }

    public static int currentDate(long epochTime, String timezone) {
        return DateTimeUtils.timestampMillisToDate(epochTime);
    }

    public static TimestampData currentTimestamp(long epochTime, String timezone) {
        return TimestampData.fromMillis(
                epochTime + TimeZone.getTimeZone(timezone).getOffset(epochTime));
    }

    public static LocalZonedTimestampData now(long epochTime, String timezone) {
        return LocalZonedTimestampData.fromEpochMillis(epochTime);
    }

    public static String dateFormat(LocalZonedTimestampData timestamp, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(timestamp.getEpochMillisecond()));
    }

    public static String dateFormat(TimestampData timestamp, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(timestamp.getMillisecond()));
    }

    public static String dateFormat(ZonedTimestampData timestamp, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(timestamp.getMillisecond()));
    }

    public static int toDate(String str) {
        return toDate(str, "yyyy-MM-dd");
    }

    public static int toDate(String str, String format) {
        return DateTimeUtils.parseDate(str, format);
    }

    public static TimestampData toTimestamp(String str) {
        return toTimestamp(str, "yyyy-MM-dd HH:mm:ss");
    }

    public static TimestampData toTimestamp(String str, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            return TimestampData.fromMillis(dateFormat.parse(str).getTime());
        } catch (ParseException e) {
            LOG.error("Unsupported date type convert: {}", str);
            throw new RuntimeException(e);
        }
    }

    public static int timestampDiff(
            String symbol,
            LocalZonedTimestampData fromTimestamp,
            LocalZonedTimestampData toTimestamp) {
        return timestampDiff(
                symbol, fromTimestamp.getEpochMillisecond(), toTimestamp.getEpochMillisecond());
    }

    public static int timestampDiff(
            String symbol, TimestampData fromTimestamp, TimestampData toTimestamp) {
        return timestampDiff(symbol, fromTimestamp.getMillisecond(), toTimestamp.getMillisecond());
    }

    public static int timestampDiff(
            String symbol, ZonedTimestampData fromTimestamp, ZonedTimestampData toTimestamp) {
        return timestampDiff(symbol, fromTimestamp.getMillisecond(), toTimestamp.getMillisecond());
    }

    public static int timestampDiff(String symbol, long fromDate, long toDate) {
        Calendar from = Calendar.getInstance();
        from.setTime(new Date(fromDate));
        Calendar to = Calendar.getInstance();
        to.setTime(new Date(toDate));
        Long second = (to.getTimeInMillis() - from.getTimeInMillis()) / 1000;
        switch (symbol) {
            case "SECOND":
                return second.intValue();
            case "MINUTE":
                return second.intValue() / 60;
            case "HOUR":
                return second.intValue() / 3600;
            case "DAY":
                return second.intValue() / (24 * 3600);
            case "MONTH":
                return to.get(Calendar.YEAR) * 12
                        + to.get(Calendar.MONDAY)
                        - (from.get(Calendar.YEAR) * 12 + from.get(Calendar.MONDAY));
            case "YEAR":
                return to.get(Calendar.YEAR) - from.get(Calendar.YEAR);
            default:
                LOG.error("Unsupported timestamp diff: {}", symbol);
                throw new RuntimeException("Unsupported timestamp diff: " + symbol);
        }
    }

    public static boolean betweenAsymmetric(String value, String minValue, String maxValue) {
        if (value == null) {
            return false;
        }
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }

    public static boolean betweenAsymmetric(Short value, short minValue, short maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Integer value, int minValue, int maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Long value, long minValue, long maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Float value, float minValue, float maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Double value, double minValue, double maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        if (value == null) {
            return false;
        }
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }

    public static boolean notBetweenAsymmetric(String value, String minValue, String maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Short value, short minValue, short maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Integer value, int minValue, int maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Long value, long minValue, long maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Float value, float minValue, float maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Double value, double minValue, double maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean in(String value, String... str) {
        return Arrays.stream(str).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Short value, Short... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Integer value, Integer... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Long value, Long... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Float value, Float... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Double value, Double... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(BigDecimal value, BigDecimal... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean notIn(String value, String... values) {
        return !in(value, values);
    }

    public static boolean notIn(Short value, Short... values) {
        return !in(value, values);
    }

    public static boolean notIn(Integer value, Integer... values) {
        return !in(value, values);
    }

    public static boolean notIn(Long value, Long... values) {
        return !in(value, values);
    }

    public static boolean notIn(Float value, Float... values) {
        return !in(value, values);
    }

    public static boolean notIn(Double value, Double... values) {
        return !in(value, values);
    }

    public static boolean notIn(BigDecimal value, BigDecimal... values) {
        return !in(value, values);
    }

    public static int charLength(String str) {
        return str.length();
    }

    public static String trim(String symbol, String target, String str) {
        return str.trim();
    }

    /**
     * Returns a string resulting from replacing all substrings that match the regular expression
     * with replacement.
     */
    public static String regexpReplace(String str, String regex, String replacement) {
        if (str == null || regex == null || replacement == null) {
            return null;
        }
        try {
            return str.replaceAll(regex, Matcher.quoteReplacement(replacement));
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Exception in regexpReplace('%s', '%s', '%s')",
                            str, regex, replacement),
                    e);
            // return null if exception in regex replace
            return null;
        }
    }

    public static String concat(String... str) {
        return String.join("", str);
    }

    public static boolean like(String str, String regex) {
        return Pattern.compile(regex).matcher(str).find();
    }

    public static boolean notLike(String str, String regex) {
        return !like(str, regex);
    }

    public static String substr(String str, int beginIndex) {
        return str.substring(beginIndex);
    }

    public static String substr(String str, int beginIndex, int length) {
        return str.substring(beginIndex, beginIndex + length);
    }

    public static String upper(String str) {
        return str.toUpperCase();
    }

    public static String lower(String str) {
        return str.toLowerCase();
    }

    /** SQL <code>ABS</code> operator applied to byte values. */
    public static byte abs(byte b0) {
        return (byte) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to short values. */
    public static short abs(short b0) {
        return (short) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to int values. */
    public static int abs(int b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to long values. */
    public static long abs(long b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to float values. */
    public static float abs(float b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to double values. */
    public static double abs(double b0) {
        return Math.abs(b0);
    }

    public static double floor(double b0) {
        return Math.floor(b0);
    }

    public static float floor(float b0) {
        return (float) Math.floor(b0);
    }

    /** SQL <code>FLOOR</code> operator applied to int values. */
    public static int floor(int b0, int b1) {
        int r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    /** SQL <code>FLOOR</code> operator applied to long values. */
    public static long floor(long b0, long b1) {
        long r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    public static double ceil(double b0) {
        return Math.ceil(b0);
    }

    public static float ceil(float b0) {
        return (float) Math.ceil(b0);
    }

    /** SQL <code>CEIL</code> operator applied to int values. */
    public static int ceil(int b0, int b1) {
        int r = b0 % b1;
        if (r > 0) {
            r -= b1;
        }
        return b0 - r;
    }

    /** SQL <code>CEIL</code> operator applied to long values. */
    public static long ceil(long b0, long b1) {
        return floor(b0 + b1 - 1, b1);
    }

    // SQL ROUND
    /** SQL <code>ROUND</code> operator applied to byte values. */
    public static byte round(byte b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to byte values. */
    public static byte round(byte b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).byteValue();
    }

    /** SQL <code>ROUND</code> operator applied to short values. */
    public static short round(short b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to short values. */
    public static short round(short b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).shortValue();
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int round(int b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int round(int b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).intValue();
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long round(long b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long round(long b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).longValue();
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal round(BigDecimal b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal round(BigDecimal b0, int b1) {
        return b0.movePointRight(b1).setScale(0, RoundingMode.HALF_UP).movePointLeft(b1);
    }

    /** SQL <code>ROUND</code> operator applied to float values. */
    public static float round(float b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to float values. */
    public static float round(float b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).floatValue();
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double round(double b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double round(double b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String uuid(byte[] b) {
        return UUID.nameUUIDFromBytes(b).toString();
    }

    public static boolean valueEquals(Object object1, Object object2) {
        return (object1 != null && object2 != null) && object1.equals(object2);
    }

    public static Object coalesce(Object... objects) {
        for (Object item : objects) {
            if (item != null) {
                return item;
            }
        }
        return null;
    }
}
