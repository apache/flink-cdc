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
import java.util.Arrays;

/** Comparison built-in functions. */
public class ComparisonFunctions {

    public static boolean valueEquals(Object object1, Object object2) {
        return (object1 != null && object2 != null) && object1.equals(object2);
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

    public static boolean betweenAsymmetric(String value, String minValue, String maxValue) {
        if (value == null) {
            return false;
        }
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }

    public static boolean betweenAsymmetric(Byte value, byte minValue, byte maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
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

    public static boolean notBetweenAsymmetric(Byte value, byte minValue, byte maxValue) {
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
        return Arrays.asList(str).contains(value);
    }

    public static boolean in(Byte value, Byte... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(Short value, Short... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(Integer value, Integer... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(Long value, Long... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(Float value, Float... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(Double value, Double... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean in(BigDecimal value, BigDecimal... values) {
        return Arrays.asList(values).contains(value);
    }

    public static boolean notIn(String value, String... values) {
        return !in(value, values);
    }

    public static boolean notIn(Byte value, Byte... values) {
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
}
