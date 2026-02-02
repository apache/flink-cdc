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

import java.util.List;
import java.util.Map;

/** Struct built-in functions. */
public class StructFunctions {

    /**
     * Accesses an element from an ARRAY or MAP by index or key.
     *
     * <p>For ARRAY: Uses 1-based index (SQL standard). array[1] returns the first element.
     *
     * <p>For MAP: Uses key to access the value. map['key'] returns the value for 'key'.
     */
    public static Object itemAccess(Object collection, Object indexOrKey) {
        if (collection == null || indexOrKey == null) {
            return null;
        }

        Object result;
        if (collection instanceof List) {
            result = arrayElement((List) collection, indexOrKey);
        } else if (collection instanceof Map) {
            result = mapValue((Map<?, ?>) collection, indexOrKey);
        } else {
            throw new IllegalArgumentException(
                    "itemAccess only supports List or Map, but got: "
                            + collection.getClass().getName());
        }
        return result;
    }

    /**
     * Gets an element from an Object array by index (1-based, SQL standard). This overload handles
     * arrays that have been converted from ArrayData to Object[] by DataTypeConverter.
     *
     * @param array the Object array to access
     * @param index the 1-based index
     * @return the element at the specified index, or null if index is out of bounds
     */
    public static Object arrayElement(List array, Object index) {
        if (array == null || index == null) {
            return null;
        }

        int idx;
        if (index instanceof Number) {
            idx = ((Number) index).intValue();
        } else {
            idx = Integer.parseInt(index.toString());
        }

        // Convert 1-based index to 0-based (SQL standard uses 1-based indexing)
        int zeroBasedIndex = idx - 1;

        // Check bounds
        if (zeroBasedIndex < 0 || zeroBasedIndex >= array.size()) {
            return null;
        }

        return array.get(zeroBasedIndex);
    }

    /**
     * Gets a value from a Map by key.
     *
     * @param map the Map to access
     * @param key the key to look up
     * @return the value for the specified key, or null if not found
     */
    public static Object mapValue(Map<?, ?> map, Object key) {
        if (map == null || key == null) {
            return null;
        }

        return map.get(key);
    }
}
