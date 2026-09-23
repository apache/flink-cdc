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

import org.apache.flink.cdc.common.types.variant.Variant;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Built-in functions for collection and struct data types.
 *
 * <p>These functions support accessing elements from collections (ARRAY, MAP), structured data
 * types (ROW), and semi-structured data types (VARIANT).
 */
public class StructFunctions {

    /** Creates an ARRAY value. */
    public static List<Object> array(Object... elements) {
        List<Object> result = new ArrayList<>(elements.length);
        for (Object element : elements) {
            result.add(element);
        }
        return result;
    }

    /** Creates a MAP value from alternating keys and values. */
    public static Map<Object, Object> map(Object... keyValues) {
        if (keyValues.length == 0 || keyValues.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "MAP requires at least one key-value pair and an even number of arguments.");
        }
        Map<Object, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            result.put(keyValues[i], keyValues[i + 1]);
        }
        return result;
    }

    /** Creates a ROW value. */
    public static List<Object> row(Object... fields) {
        List<Object> result = new ArrayList<>(fields.length);
        for (Object field : fields) {
            result.add(field);
        }
        return result;
    }

    /** Returns the number of elements in an ARRAY. */
    public static Integer cardinality(List<?> array) {
        return array == null ? null : array.size();
    }

    /** Returns the number of entries in a MAP. */
    public static Integer cardinality(Map<?, ?> map) {
        return map == null ? null : map.size();
    }

    /** Returns whether an ARRAY contains the given value. */
    public static Boolean arrayContains(List<?> array, Object value) {
        if (array == null) {
            return null;
        }
        for (Object element : array) {
            if (valueEquals(element, value)) {
                return true;
            }
        }
        return false;
    }

    /** Returns the 1-based position of a value in an ARRAY, or 0 if it is not found. */
    public static Integer arrayPosition(List<?> array, Object value) {
        if (array == null || value == null) {
            return null;
        }
        for (int i = 0; i < array.size(); i++) {
            if (valueEquals(array.get(i), value)) {
                return i + 1;
            }
        }
        return 0;
    }

    /** Returns the only element of an ARRAY. */
    public static <T> T element(List<T> array) {
        if (array == null || array.isEmpty()) {
            return null;
        }
        if (array.size() > 1) {
            throw new IllegalArgumentException("Array has more than one element.");
        }
        return array.get(0);
    }

    private static boolean valueEquals(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number && right instanceof Number) {
            return numberEquals((Number) left, (Number) right);
        }
        if (left instanceof List<?> && right instanceof List<?>) {
            List<?> leftList = (List<?>) left;
            List<?> rightList = (List<?>) right;
            if (leftList.size() != rightList.size()) {
                return false;
            }
            for (int i = 0; i < leftList.size(); i++) {
                if (!valueEquals(leftList.get(i), rightList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof Map<?, ?> && right instanceof Map<?, ?>) {
            return mapEquals((Map<?, ?>) left, (Map<?, ?>) right);
        }
        if (left.getClass().isArray() && right.getClass().isArray()) {
            int length = Array.getLength(left);
            if (length != Array.getLength(right)) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (!valueEquals(Array.get(left, i), Array.get(right, i))) {
                    return false;
                }
            }
            return true;
        }
        return left.equals(right);
    }

    private static boolean numberEquals(Number left, Number right) {
        if (left instanceof Float
                || left instanceof Double
                || right instanceof Float
                || right instanceof Double) {
            return Double.compare(left.doubleValue(), right.doubleValue()) == 0;
        }
        return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
    }

    private static boolean mapEquals(Map<?, ?> left, Map<?, ?> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (Map.Entry<?, ?> leftEntry : left.entrySet()) {
            boolean found = false;
            for (Map.Entry<?, ?> rightEntry : right.entrySet()) {
                if (valueEquals(leftEntry.getKey(), rightEntry.getKey())) {
                    if (!valueEquals(leftEntry.getValue(), rightEntry.getValue())) {
                        return false;
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    /**
     * Accesses an element from an ARRAY by index (1-based, SQL standard).
     *
     * <p>array[1] returns the first element.
     *
     * @param <T> the element type of the array
     * @param array the array to access
     * @param index the 1-based index
     * @return the element at the specified index, or null if index is out of bounds
     */
    public static <T> T itemAccess(List<T> array, Integer index) {
        if (array == null || index == null) {
            return null;
        }
        // Convert 1-based index to 0-based (SQL standard uses 1-based indexing)
        int zeroBasedIndex = index - 1;
        if (zeroBasedIndex < 0 || zeroBasedIndex >= array.size()) {
            return null;
        }
        return array.get(zeroBasedIndex);
    }

    /**
     * Accesses a value from a MAP by key.
     *
     * <p>map['key'] returns the value for 'key'.
     *
     * @param <K> the key type of the map
     * @param <V> the value type of the map
     * @param map the map to access
     * @param key the key to look up
     * @return the value for the specified key, or null if not found
     */
    public static <K, V> V itemAccess(Map<K, V> map, K key) {
        if (map == null || key == null) {
            return null;
        }
        return map.get(key);
    }

    /**
     * Accesses an element from a VARIANT array by index (1-based, SQL standard).
     *
     * <p>variant[1] returns the first element.
     *
     * @param variant the variant (must be an array) to access
     * @param index the 1-based index
     * @return the element at the specified index as a Variant, or null if the variant is not an
     *     array or index is out of bounds
     */
    public static Variant itemAccess(Variant variant, Integer index) {
        if (variant == null || index == null) {
            return null;
        }
        if (!variant.isArray()) {
            return null;
        }
        // Convert 1-based index to 0-based (SQL standard uses 1-based indexing)
        int zeroBasedIndex = index - 1;
        if (zeroBasedIndex < 0 || zeroBasedIndex >= variant.arraySize()) {
            return null;
        }
        return variant.getElement(zeroBasedIndex);
    }

    /**
     * Accesses a field from a VARIANT object by field name.
     *
     * <p>variant['fieldName'] returns the value of the specified field.
     *
     * @param variant the variant (must be an object) to access
     * @param fieldName the name of the field to look up
     * @return the field value as a Variant, or null if the variant is not an object or field is not
     *     found
     */
    public static Variant itemAccess(Variant variant, String fieldName) {
        if (variant == null || fieldName == null) {
            return null;
        }
        if (!variant.isObject()) {
            return null;
        }
        return variant.getField(fieldName);
    }
}
