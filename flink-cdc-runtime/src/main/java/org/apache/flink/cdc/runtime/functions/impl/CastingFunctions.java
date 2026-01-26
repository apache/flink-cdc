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

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** Casting built-in functions. */
public class CastingFunctions {

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
}
