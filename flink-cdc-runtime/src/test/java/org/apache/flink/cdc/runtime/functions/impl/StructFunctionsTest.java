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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link StructFunctions}. */
class StructFunctionsTest {

    @Test
    void testItemAccessWithList() {
        List<String> list = Arrays.asList("one", "two", "three");

        // SQL uses 1-based indexing
        assertThat(StructFunctions.itemAccess(list, 1)).isEqualTo("one");
        assertThat(StructFunctions.itemAccess(list, 2)).isEqualTo("two");
        assertThat(StructFunctions.itemAccess(list, 3)).isEqualTo("three");
    }

    @Test
    void testItemAccessWithMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);

        assertThat(StructFunctions.itemAccess(map, "a")).isEqualTo(1);
        assertThat(StructFunctions.itemAccess(map, "b")).isEqualTo(2);
        assertThat(StructFunctions.itemAccess(map, "c")).isEqualTo(3);
    }

    @Test
    void testItemAccessWithNull() {
        List<String> list = Arrays.asList("one", "two", "three");
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);

        // Null collection returns null
        assertThat(StructFunctions.itemAccess(null, 1)).isNull();
        assertThat(StructFunctions.itemAccess(null, "a")).isNull();

        // Null index/key returns null
        assertThat(StructFunctions.itemAccess(list, null)).isNull();
        assertThat(StructFunctions.itemAccess(map, null)).isNull();
    }

    @Test
    void testItemAccessWithInvalidType() {
        assertThatThrownBy(() -> StructFunctions.itemAccess("not a collection", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("itemAccess only supports List or Map");
    }

    @Test
    void testArrayElementWithNormalIndex() {
        List<Integer> array = Arrays.asList(10, 20, 30, 40, 50);

        // SQL uses 1-based indexing
        assertThat(StructFunctions.arrayElement(array, 1)).isEqualTo(10);
        assertThat(StructFunctions.arrayElement(array, 3)).isEqualTo(30);
        assertThat(StructFunctions.arrayElement(array, 5)).isEqualTo(50);
    }

    @Test
    void testArrayElementWithOutOfBoundsIndex() {
        List<Integer> array = Arrays.asList(10, 20, 30);

        // Out of bounds returns null (not throw exception)
        assertThat(StructFunctions.arrayElement(array, 0)).isNull(); // Index 0 is invalid in SQL
        assertThat(StructFunctions.arrayElement(array, 4)).isNull();
        assertThat(StructFunctions.arrayElement(array, -1)).isNull();
        assertThat(StructFunctions.arrayElement(array, 100)).isNull();
    }

    @Test
    void testArrayElementWithNull() {
        List<Integer> array = Arrays.asList(10, 20, 30);

        assertThat(StructFunctions.arrayElement(null, 1)).isNull();
        assertThat(StructFunctions.arrayElement(array, null)).isNull();
    }

    @Test
    void testArrayElementWithStringIndex() {
        List<Integer> array = Arrays.asList(10, 20, 30);

        // Index can be string that can be parsed to integer
        assertThat(StructFunctions.arrayElement(array, "1")).isEqualTo(10);
        assertThat(StructFunctions.arrayElement(array, "2")).isEqualTo(20);
    }

    @Test
    void testMapValue() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        assertThat(StructFunctions.mapValue(map, 1)).isEqualTo("one");
        assertThat(StructFunctions.mapValue(map, 2)).isEqualTo("two");
        assertThat(StructFunctions.mapValue(map, 3)).isEqualTo("three");
    }

    @Test
    void testMapValueWithAbsentKey() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");

        assertThat(StructFunctions.mapValue(map, 999)).isNull();
    }

    @Test
    void testMapValueWithNull() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");

        assertThat(StructFunctions.mapValue(null, 1)).isNull();
        assertThat(StructFunctions.mapValue(map, null)).isNull();
    }
}
