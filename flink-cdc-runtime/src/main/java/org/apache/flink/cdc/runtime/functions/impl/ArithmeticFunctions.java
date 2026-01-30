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

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Arithmetic built-in functions. */
public class ArithmeticFunctions {

    // Add operator
    public static int plus(byte lhs, byte rhs) {
        return lhs + rhs;
    }

    public static int plus(short lhs, short rhs) {
        return lhs + rhs;
    }

    public static int plus(int lhs, int rhs) {
        return lhs + rhs;
    }

    public static long plus(long lhs, long rhs) {
        return lhs + rhs;
    }

    public static float plus(float lhs, float rhs) {
        return lhs + rhs;
    }

    public static double plus(double lhs, double rhs) {
        return lhs + rhs;
    }

    public static BigDecimal plus(BigDecimal lhs, BigDecimal rhs) {
        return lhs.add(rhs);
    }

    // Subtract operator
    public static int minus(byte lhs, byte rhs) {
        return lhs - rhs;
    }

    public static int minus(short lhs, short rhs) {
        return lhs - rhs;
    }

    public static int minus(int lhs, int rhs) {
        return lhs - rhs;
    }

    public static long minus(long lhs, long rhs) {
        return lhs - rhs;
    }

    public static float minus(float lhs, float rhs) {
        return lhs - rhs;
    }

    public static double minus(double lhs, double rhs) {
        return lhs - rhs;
    }

    public static BigDecimal minus(BigDecimal lhs, BigDecimal rhs) {
        return lhs.subtract(rhs);
    }

    // Multiply operator
    public static int times(byte lhs, byte rhs) {
        return lhs * rhs;
    }

    public static int times(short lhs, short rhs) {
        return lhs * rhs;
    }

    public static int times(int lhs, int rhs) {
        return lhs * rhs;
    }

    public static long times(long lhs, long rhs) {
        return lhs * rhs;
    }

    public static float times(float lhs, float rhs) {
        return lhs * rhs;
    }

    public static double times(double lhs, double rhs) {
        return lhs * rhs;
    }

    public static BigDecimal times(BigDecimal lhs, BigDecimal rhs) {
        return lhs.multiply(rhs);
    }

    // Divides operator
    public static int divides(byte lhs, byte rhs) {
        return lhs / rhs;
    }

    public static int divides(short lhs, short rhs) {
        return lhs / rhs;
    }

    public static int divides(int lhs, int rhs) {
        return lhs / rhs;
    }

    public static long divides(long lhs, long rhs) {
        return lhs / rhs;
    }

    public static float divides(float lhs, float rhs) {
        return lhs / rhs;
    }

    public static double divides(double lhs, double rhs) {
        return lhs / rhs;
    }

    public static BigDecimal divides(BigDecimal lhs, BigDecimal rhs) {
        return lhs.divide(rhs, RoundingMode.DOWN);
    }

    // Mods operator
    public static int mods(byte lhs, byte rhs) {
        return lhs % rhs;
    }

    public static int mods(short lhs, short rhs) {
        return lhs % rhs;
    }

    public static int mods(int lhs, int rhs) {
        return lhs % rhs;
    }

    public static int mods(long lhs, long rhs) {
        return (int) (lhs % rhs);
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
    public static BigDecimal abs(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.abs();
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

    public static BigDecimal floor(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.setScale(0, RoundingMode.FLOOR);
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

    public static BigDecimal ceil(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.setScale(0, RoundingMode.CEILING);
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

    public static BigDecimal round(BigDecimal value, int pointOffset) {
        if (value == null) {
            return null;
        }
        return value.setScale(pointOffset, RoundingMode.HALF_UP);
    }
}
