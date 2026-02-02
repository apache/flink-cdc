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

/**
 * Built-in functions for collection and struct data types.
 *
 * <p>These functions support accessing elements from collections (ARRAY, MAP) and structured data
 * types (ROW).
 */
public class StructFunctions {

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
}
