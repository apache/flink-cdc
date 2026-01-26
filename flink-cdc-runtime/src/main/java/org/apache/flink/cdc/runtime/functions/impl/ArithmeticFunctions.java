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

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Arithmetic built-in functions. */
public class ArithmeticFunctions {

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
}
