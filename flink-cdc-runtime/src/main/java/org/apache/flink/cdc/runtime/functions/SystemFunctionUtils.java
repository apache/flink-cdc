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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.common.utils.DateTimeUtils.timestampMillisToDate;
import static org.apache.flink.cdc.common.utils.DateTimeUtils.timestampMillisToTime;

/**
 * System function utils to support the call of flink cdc pipeline transform. <br>
 * {@code castToXxx}-series function returns `null` when conversion is not viable.
 */
public class SystemFunctionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SystemFunctionUtils.class);

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

    public static boolean betweenAsymmetric(
            DecimalData value, DecimalData minValue, DecimalData maxValue) {
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

    public static boolean notBetweenAsymmetric(
            DecimalData value, DecimalData minValue, DecimalData maxValue) {
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

    public static boolean in(DecimalData value, DecimalData... values) {
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

    public static boolean notIn(DecimalData value, DecimalData... values) {
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
        return substring(str, beginIndex);
    }

    public static String substr(String str, int beginIndex, int length) {
        return substring(str, beginIndex, length);
    }

    public static String substring(String str, int beginIndex) {
        return substring(str, beginIndex, Integer.MAX_VALUE);
    }

    public static String substring(String str, int beginIndex, int length) {
        if (length < 0) {
            LOG.error(
                    "length of 'substring(str, beginIndex, length)' must be >= 0 and Int type, but length = {}",
                    length);
            throw new RuntimeException(
                    "length of 'substring(str, beginIndex, length)' must be >= 0 and Int type, but length = "
                            + length);
        }
        if (length > Integer.MAX_VALUE || beginIndex > Integer.MAX_VALUE) {
            LOG.error(
                    "length or start of 'substring(str, beginIndex, length)' must be Int type, but length = {}, beginIndex = {}",
                    beginIndex,
                    length);
            throw new RuntimeException(
                    "length or start of 'substring(str, beginIndex, length)' must be Int type, but length = "
                            + beginIndex
                            + ", beginIndex = "
                            + length);
        }
        if (str.isEmpty()) {
            return "";
        }

        int startPos;
        int endPos;

        if (beginIndex > 0) {
            startPos = beginIndex - 1;
            if (startPos >= str.length()) {
                return "";
            }
        } else if (beginIndex < 0) {
            startPos = str.length() + beginIndex;
            if (startPos < 0) {
                return "";
            }
        } else {
            startPos = 0;
        }

        if ((str.length() - startPos) < length) {
            endPos = str.length();
        } else {
            endPos = startPos + length;
        }
        return str.substring(startPos, endPos);
    }

    public static String upper(String str) {
        return str.toUpperCase();
    }

    public static String lower(String str) {
        return str.toLowerCase();
    }

    /** SQL <code>ABS</code> operator applied to byte values. */
    public static Byte abs(Byte value) {
        if (value == null) {
            return null;
        }
        return (byte) Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to short values. */
    public static Short abs(Short value) {
        if (value == null) {
            return null;
        }
        return (short) Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to int values. */
    public static Integer abs(Integer value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to long values. */
    public static Long abs(Long value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to float values. */
    public static Float abs(Float value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to double values. */
    public static Double abs(Double value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    /** SQL <code>ABS</code> operator applied to decimal values. */
    public static DecimalData abs(DecimalData value) {
        if (value == null) {
            return null;
        }
        return DecimalData.fromBigDecimal(
                BigDecimal.valueOf(Math.abs(value.toBigDecimal().doubleValue())),
                value.precision(),
                value.scale());
    }

    public static Byte floor(Byte value) {
        return value;
    }

    public static Short floor(Short value) {
        return value;
    }

    public static Integer floor(Integer value) {
        return value;
    }

    public static Long floor(Long value) {
        return value;
    }

    public static Double floor(Double value) {
        if (value == null) {
            return null;
        }
        return Math.floor(value);
    }

    public static Float floor(Float value) {
        if (value == null) {
            return null;
        }
        return (float) Math.floor(value);
    }

    public static DecimalData floor(DecimalData value) {
        if (value == null) {
            return null;
        }
        return DecimalData.fromBigDecimal(
                BigDecimal.valueOf(Math.floor(value.toBigDecimal().doubleValue())),
                value.precision(),
                0);
    }

    public static Byte ceil(Byte value) {
        return value;
    }

    public static Short ceil(Short value) {
        return value;
    }

    public static Integer ceil(Integer value) {
        return value;
    }

    public static Long ceil(Long value) {
        return value;
    }

    public static Double ceil(Double value) {
        if (value == null) {
            return null;
        }
        return Math.ceil(value);
    }

    public static Float ceil(Float value) {
        if (value == null) {
            return null;
        }
        return (float) Math.ceil(value);
    }

    public static DecimalData ceil(DecimalData value) {
        if (value == null) {
            return null;
        }
        return DecimalData.fromBigDecimal(
                BigDecimal.valueOf(Math.ceil(value.toBigDecimal().doubleValue())),
                value.precision(),
                0);
    }

    // SQL ROUND
    /** SQL <code>ROUND</code> operator applied to byte values. */
    public static Byte round(Byte value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(BigDecimal.valueOf(value), pointOffset).byteValue();
    }

    /** SQL <code>ROUND</code> operator applied to short values. */
    public static Short round(Short value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(BigDecimal.valueOf(value), pointOffset).shortValue();
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static Integer round(Integer value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(BigDecimal.valueOf(value), pointOffset).intValue();
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static Long round(Long value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(BigDecimal.valueOf(value), pointOffset).longValue();
    }

    /** SQL <code>ROUND</code> operator applied to DecimalData values. */
    public static DecimalData round(DecimalData value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return DecimalData.fromBigDecimal(
                value.toBigDecimal()
                        .movePointRight(pointOffset)
                        .setScale(0, RoundingMode.HALF_UP)
                        .movePointLeft(pointOffset),
                value.precision(),
                pointOffset);
    }

    /** SQL <code>ROUND</code> operator applied to float values. */
    public static Float round(Float value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(new BigDecimal(value.toString()), pointOffset).floatValue();
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static Double round(Double value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return round(BigDecimal.valueOf(value), pointOffset).doubleValue();
    }

    private static BigDecimal round(BigDecimal value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return value.movePointRight(pointOffset)
                .setScale(0, RoundingMode.HALF_UP)
                .movePointLeft(pointOffset);
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

    public static String castToString(Object object) {
        if (object == null) {
            return null;
        }
        return object.toString();
    }

    public static Boolean castToBoolean(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Boolean) {
            return (Boolean) object;
        } else if (object instanceof Byte) {
            return !object.equals((byte) 0);
        } else if (object instanceof Short) {
            return !object.equals((short) 0);
        } else if (object instanceof Integer) {
            return !object.equals(0);
        } else if (object instanceof Long) {
            return !object.equals(0L);
        } else if (object instanceof Float) {
            return !object.equals(0f);
        } else if (object instanceof Double) {
            return !object.equals(0d);
        } else if (object instanceof BigDecimal) {
            return ((BigDecimal) object).compareTo(BigDecimal.ZERO) != 0;
        } else if (object instanceof DecimalData) {
            return ((DecimalData) object).compareTo(DecimalData.zero(1, 0)) != 0;
        }
        return Boolean.valueOf(castToString(object));
    }

    public static Byte castToByte(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (byte) ((Boolean) object ? 1 : 0);
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).byteValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().byteValue();
        }
        if (object instanceof Double) {
            return ((Double) object).byteValue();
        }
        if (object instanceof Float) {
            return ((Float) object).byteValue();
        }
        String stringRep = castToString(object);
        try {
            return Byte.valueOf(stringRep);
        } catch (NumberFormatException e) {
            // Ignore this exception because it could still represent a valid floating point number,
            // but could not be accepted by Byte#valueOf.
        }
        try {
            return Double.valueOf(stringRep).byteValue();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static Short castToShort(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (short) ((Boolean) object ? 1 : 0);
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).shortValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().shortValue();
        }
        if (object instanceof Double) {
            return ((Double) object).shortValue();
        }
        if (object instanceof Float) {
            return ((Float) object).shortValue();
        }
        String stringRep = castToString(object);
        try {
            return Short.valueOf(stringRep);
        } catch (NumberFormatException e) {
            // Ignore this exception because it could still represent a valid floating point number,
            // but could not be accepted by Short#valueOf.
        }
        try {
            return Double.valueOf(stringRep).shortValue();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static Integer castToInteger(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (Boolean) object ? 1 : 0;
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).intValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().intValue();
        }
        if (object instanceof Double) {
            return ((Double) object).intValue();
        }
        if (object instanceof Float) {
            return ((Float) object).intValue();
        }
        String stringRep = castToString(object);
        try {
            return Integer.valueOf(stringRep);
        } catch (NumberFormatException e) {
            // Ignore this exception because it could still represent a valid floating point number,
            // but could not be accepted by Integer#valueOf.
        }
        try {
            return Double.valueOf(stringRep).intValue();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static Long castToLong(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (Boolean) object ? 1L : 0L;
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).longValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().longValue();
        }
        if (object instanceof Double) {
            return ((Double) object).longValue();
        }
        if (object instanceof Float) {
            return ((Float) object).longValue();
        }
        String stringRep = castToString(object);
        try {
            return Long.valueOf(stringRep);
        } catch (NumberFormatException e) {
            // Ignore this exception because it could still represent a valid floating point number,
            // but could not be accepted by Long#valueOf.
        }
        try {
            return Double.valueOf(stringRep).longValue();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static Float castToFloat(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (Boolean) object ? 1f : 0f;
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).floatValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().floatValue();
        }
        if (object instanceof Double) {
            return ((Double) object).floatValue();
        }
        if (object instanceof Float) {
            return (Float) object;
        }
        try {
            return Float.valueOf(castObjectIntoString(object));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static Double castToDouble(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            return (Boolean) object ? 1d : 0d;
        }
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).doubleValue();
        }
        if (object instanceof DecimalData) {
            return ((DecimalData) object).toBigDecimal().doubleValue();
        }
        if (object instanceof Double) {
            return (Double) object;
        }
        if (object instanceof Float) {
            return ((Float) object).doubleValue();
        }
        try {
            return Double.valueOf(castObjectIntoString(object));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static BigDecimal castToBigDecimal(Object object, int precision, int scale) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            object = (Boolean) object ? 1 : 0;
        }

        BigDecimal bigDecimal;
        try {
            bigDecimal = new BigDecimal(castObjectIntoString(object), new MathContext(precision));
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
        } catch (NumberFormatException ignored) {
            return null;
        }

        // If the precision overflows, null will be returned. Otherwise, we may accidentally emit a
        // non-serializable object into the pipeline that breaks downstream.
        if (bigDecimal.precision() > precision) {
            return null;
        }
        return bigDecimal;
    }

    public static DecimalData castToDecimalData(Object object, int precision, int scale) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean) {
            object = (Boolean) object ? 1 : 0;
        }

        BigDecimal bigDecimal;
        try {
            bigDecimal = new BigDecimal(castObjectIntoString(object), new MathContext(precision));
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
        } catch (NumberFormatException ignored) {
            return null;
        }

        // If the precision overflows, null will be returned. Otherwise, we may accidentally emit a
        // non-serializable object into the pipeline that breaks downstream.
        if (bigDecimal.precision() > precision) {
            return null;
        }
        return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
    }

    public static TimestampData castToTimestamp(Object object, String timezone) {
        if (object == null) {
            return null;
        }
        if (object instanceof LocalZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((LocalZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else if (object instanceof ZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((ZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(castObjectIntoString(object)));
        }
    }

    private static String castObjectIntoString(Object object) {
        if (object instanceof Boolean) {
            return Boolean.valueOf(castToString(object)) ? "1" : "0";
        }
        return String.valueOf(object);
    }

    private static int universalCompares(Object lhs, Object rhs) {
        Class<?> leftClass = lhs.getClass();
        Class<?> rightClass = rhs.getClass();
        if (leftClass.equals(rightClass) && lhs instanceof Comparable) {
            return ((Comparable) lhs).compareTo(rhs);
        } else if (lhs instanceof Number && rhs instanceof Number) {
            return Double.compare(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
        } else {
            throw new RuntimeException(
                    "Comparison of unsupported data types: "
                            + leftClass.getName()
                            + " and "
                            + rightClass.getName());
        }
    }

    public static boolean greaterThan(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return false;
        }
        return universalCompares(lhs, rhs) > 0;
    }

    public static boolean greaterThanOrEqual(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return false;
        }
        return universalCompares(lhs, rhs) >= 0;
    }

    public static boolean lessThan(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return false;
        }
        return universalCompares(lhs, rhs) < 0;
    }

    public static boolean lessThanOrEqual(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return false;
        }
        return universalCompares(lhs, rhs) <= 0;
    }
}
