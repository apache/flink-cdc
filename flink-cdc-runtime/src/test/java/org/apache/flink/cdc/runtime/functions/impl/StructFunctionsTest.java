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
import org.apache.flink.cdc.common.types.variant.VariantBuilder;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StructFunctions}. */
class StructFunctionsTest {

    // ========================================
    // List (ARRAY) Access Tests
    // ========================================
    @Nested
    class ListAccessTests {

        @Test
        void testNormalAccess() {
            List<String> list = Arrays.asList("one", "two", "three");

            // SQL uses 1-based indexing
            assertThat(StructFunctions.itemAccess(list, 1)).isEqualTo("one");
            assertThat(StructFunctions.itemAccess(list, 2)).isEqualTo("two");
            assertThat(StructFunctions.itemAccess(list, 3)).isEqualTo("three");
        }

        @Test
        void testOutOfBoundsAccess() {
            List<Integer> list = Arrays.asList(10, 20, 30);

            // Index 0 is invalid in SQL (1-based indexing)
            assertThat(StructFunctions.itemAccess(list, 0)).isNull();
            // Negative index
            assertThat(StructFunctions.itemAccess(list, -1)).isNull();
            // Index beyond size
            assertThat(StructFunctions.itemAccess(list, 4)).isNull();
            assertThat(StructFunctions.itemAccess(list, 100)).isNull();
        }

        @Test
        void testNullHandling() {
            List<String> list = Arrays.asList("one", "two", "three");

            // Null list returns null
            assertThat(StructFunctions.itemAccess((List<String>) null, 1)).isNull();
            // Null index returns null
            assertThat(StructFunctions.itemAccess(list, null)).isNull();
        }

        @Test
        void testEmptyList() {
            List<String> emptyList = Collections.emptyList();

            assertThat(StructFunctions.itemAccess(emptyList, 1)).isNull();
        }

        @Test
        void testListWithNullElement() {
            List<String> listWithNull = new ArrayList<>();
            listWithNull.add("first");
            listWithNull.add(null);
            listWithNull.add("third");

            assertThat(StructFunctions.itemAccess(listWithNull, 1)).isEqualTo("first");
            assertThat(StructFunctions.itemAccess(listWithNull, 2)).isNull();
            assertThat(StructFunctions.itemAccess(listWithNull, 3)).isEqualTo("third");
        }
    }

    // ========================================
    // Map Access Tests
    // ========================================
    @Nested
    class MapAccessTests {

        @Test
        void testNormalAccessWithStringKey() {
            Map<String, Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 3);

            assertThat(StructFunctions.itemAccess(map, "a")).isEqualTo(1);
            assertThat(StructFunctions.itemAccess(map, "b")).isEqualTo(2);
            assertThat(StructFunctions.itemAccess(map, "c")).isEqualTo(3);
        }

        @Test
        void testNormalAccessWithIntegerKey() {
            Map<Integer, String> map = new HashMap<>();
            map.put(1, "one");
            map.put(2, "two");
            map.put(3, "three");

            assertThat(StructFunctions.itemAccess(map, 1)).isEqualTo("one");
            assertThat(StructFunctions.itemAccess(map, 2)).isEqualTo("two");
            assertThat(StructFunctions.itemAccess(map, 3)).isEqualTo("three");
        }

        @Test
        void testMissingKey() {
            Map<String, Integer> map = new HashMap<>();
            map.put("exists", 1);

            assertThat(StructFunctions.itemAccess(map, "nonexistent")).isNull();
        }

        @Test
        void testNullHandling() {
            Map<String, Integer> map = new HashMap<>();
            map.put("a", 1);

            // Null map returns null
            assertThat(StructFunctions.itemAccess((Map<String, Integer>) null, "a")).isNull();
            // Null key returns null
            assertThat(StructFunctions.itemAccess(map, null)).isNull();
        }

        @Test
        void testEmptyMap() {
            Map<String, Integer> emptyMap = Collections.emptyMap();

            assertThat(StructFunctions.itemAccess(emptyMap, "any")).isNull();
        }

        @Test
        void testMapWithNullValue() {
            Map<String, String> mapWithNullValue = new HashMap<>();
            mapWithNullValue.put("key1", "value1");
            mapWithNullValue.put("key2", null);

            assertThat(StructFunctions.itemAccess(mapWithNullValue, "key1")).isEqualTo("value1");
            assertThat(StructFunctions.itemAccess(mapWithNullValue, "key2")).isNull();
        }
    }

    // ========================================
    // Variant Access Tests
    // ========================================
    @Nested
    class VariantAccessTests {

        private final VariantBuilder builder = Variant.newBuilder();

        @Test
        void testVariantArrayAccessByIndex() {
            // Build a variant array: [1, 2, 3]
            Variant arrayVariant =
                    builder.array()
                            .add(builder.of(1))
                            .add(builder.of(2))
                            .add(builder.of(3))
                            .build();

            // SQL uses 1-based indexing
            Variant first = StructFunctions.itemAccess(arrayVariant, 1);
            assertThat(first).isNotNull();
            assertThat(first.getInt()).isEqualTo(1);

            Variant second = StructFunctions.itemAccess(arrayVariant, 2);
            assertThat(second).isNotNull();
            assertThat(second.getInt()).isEqualTo(2);

            Variant third = StructFunctions.itemAccess(arrayVariant, 3);
            assertThat(third).isNotNull();
            assertThat(third.getInt()).isEqualTo(3);
        }

        @Test
        void testVariantArrayOutOfBoundsAccess() {
            Variant arrayVariant =
                    builder.array()
                            .add(builder.of(10))
                            .add(builder.of(20))
                            .add(builder.of(30))
                            .build();

            // Index 0 is invalid in SQL (1-based indexing)
            assertThat(StructFunctions.itemAccess(arrayVariant, 0)).isNull();
            // Negative index
            assertThat(StructFunctions.itemAccess(arrayVariant, -1)).isNull();
            // Index beyond size
            assertThat(StructFunctions.itemAccess(arrayVariant, 4)).isNull();
            assertThat(StructFunctions.itemAccess(arrayVariant, 100)).isNull();
        }

        @Test
        void testVariantObjectAccessByFieldName() {
            // Build a variant object: {"name": "Alice", "age": 30}
            Variant objectVariant =
                    builder.object()
                            .add("name", builder.of("Alice"))
                            .add("age", builder.of(30))
                            .build();

            Variant name = StructFunctions.itemAccess(objectVariant, "name");
            assertThat(name).isNotNull();
            assertThat(name.getString()).isEqualTo("Alice");

            Variant age = StructFunctions.itemAccess(objectVariant, "age");
            assertThat(age).isNotNull();
            assertThat(age.getInt()).isEqualTo(30);
        }

        @Test
        void testVariantObjectMissingField() {
            Variant objectVariant = builder.object().add("exists", builder.of("value")).build();

            assertThat(StructFunctions.itemAccess(objectVariant, "nonexistent")).isNull();
        }

        @Test
        void testVariantNullHandling() {
            Variant arrayVariant = builder.array().add(builder.of(1)).build();
            Variant objectVariant = builder.object().add("key", builder.of("value")).build();

            // Null variant returns null
            assertThat(StructFunctions.itemAccess((Variant) null, 1)).isNull();
            assertThat(StructFunctions.itemAccess((Variant) null, "key")).isNull();

            // Null index returns null
            assertThat(StructFunctions.itemAccess(arrayVariant, (Integer) null)).isNull();

            // Null field name returns null
            assertThat(StructFunctions.itemAccess(objectVariant, (String) null)).isNull();
        }

        @Test
        void testVariantTypeMismatch() {
            Variant arrayVariant = builder.array().add(builder.of(1)).build();
            Variant objectVariant = builder.object().add("key", builder.of("value")).build();

            // Accessing array with string key returns null
            assertThat(StructFunctions.itemAccess(arrayVariant, "key")).isNull();

            // Accessing object with integer index returns null
            assertThat(StructFunctions.itemAccess(objectVariant, 1)).isNull();
        }

        @Test
        void testNestedVariantAccess() {
            // Build a nested variant: {"data": [1, {"nested": "value"}]}
            Variant nestedVariant =
                    builder.object()
                            .add(
                                    "data",
                                    builder.array()
                                            .add(builder.of(1))
                                            .add(
                                                    builder.object()
                                                            .add("nested", builder.of("value"))
                                                            .build())
                                            .build())
                            .build();

            // Access "data" field
            Variant data = StructFunctions.itemAccess(nestedVariant, "data");
            assertThat(data).isNotNull();
            assertThat(data.isArray()).isTrue();

            // Access second element of the array (index 2 in 1-based SQL standard)
            Variant secondElement = StructFunctions.itemAccess(data, 2);
            assertThat(secondElement).isNotNull();
            assertThat(secondElement.isObject()).isTrue();

            // Access "nested" field
            Variant nestedValue = StructFunctions.itemAccess(secondElement, "nested");
            assertThat(nestedValue).isNotNull();
            assertThat(nestedValue.getString()).isEqualTo("value");
        }

        @Test
        void testPrimitiveVariantAccess() {
            // Primitive variants are neither arrays nor objects
            Variant intVariant = builder.of(42);
            Variant stringVariant = builder.of("hello");

            // Accessing primitive variant with index returns null
            assertThat(StructFunctions.itemAccess(intVariant, 1)).isNull();
            assertThat(StructFunctions.itemAccess(stringVariant, 1)).isNull();

            // Accessing primitive variant with field name returns null
            assertThat(StructFunctions.itemAccess(intVariant, "key")).isNull();
            assertThat(StructFunctions.itemAccess(stringVariant, "key")).isNull();
        }
    }
}
