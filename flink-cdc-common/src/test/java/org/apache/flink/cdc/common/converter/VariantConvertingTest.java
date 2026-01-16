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

package org.apache.flink.cdc.common.converter;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.variant.BinaryVariantBuilder;
import org.apache.flink.cdc.common.types.variant.Variant;
import org.apache.flink.cdc.common.types.variant.VariantBuilder;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Converter tests for {@link org.apache.flink.cdc.common.types.VariantType}. */
class VariantConvertingTest {

    private static final BinaryVariantBuilder BUILDER = new BinaryVariantBuilder();
    private static final VariantBuilder.VariantArrayBuilder ARRAY_BUILDER =
            new BinaryVariantBuilder.VariantArrayBuilder();
    private static final BinaryVariantBuilder.VariantObjectBuilder OBJECT_BUILDER =
            new BinaryVariantBuilder.VariantObjectBuilder(true);

    private static final Variant[] TEST_VARIANTS = {
        BUILDER.of(true),
        BUILDER.of((byte) 2),
        BUILDER.of((short) 3),
        BUILDER.of(5),
        BUILDER.of((long) 7),
        BUILDER.of("11"),
        BUILDER.of((double) 13),
        BUILDER.of((float) 17),
        BUILDER.of("19".getBytes()),
        BUILDER.of(new BigDecimal("23")),
        BUILDER.of(Instant.ofEpochMilli(29)),
        BUILDER.of(LocalDate.ofEpochDay(31)),
        BUILDER.of(LocalDateTime.ofEpochSecond(37, 37, ZoneOffset.UTC)),
        BUILDER.ofNull(),
        ARRAY_BUILDER
                .add(BUILDER.of(true))
                .add(BUILDER.of((byte) 2))
                .add(BUILDER.of((short) 3))
                .add(BUILDER.of(5))
                .add(BUILDER.of((long) 7))
                .add(BUILDER.of("11"))
                .add(BUILDER.of((double) 13))
                .add(BUILDER.of((float) 17))
                .add(BUILDER.of("19".getBytes()))
                .add(BUILDER.of(new BigDecimal("23")))
                .add(BUILDER.of(Instant.ofEpochMilli(29)))
                .add(BUILDER.of(LocalDate.ofEpochDay(31)))
                .add(BUILDER.of(LocalDateTime.ofEpochSecond(37, 37, ZoneOffset.UTC)))
                .add(BUILDER.ofNull())
                .build(),
        OBJECT_BUILDER
                .add("col_bool", BUILDER.of(true))
                .add("col_tinyint", BUILDER.of((byte) 2))
                .add("col_shortint", BUILDER.of((short) 3))
                .add("col_int", BUILDER.of(5))
                .add("col_bigint", BUILDER.of((long) 7))
                .add("col_string", BUILDER.of("11"))
                .add("col_double", BUILDER.of((double) 13))
                .add("col_float", BUILDER.of((float) 17))
                .add("col_bytes", BUILDER.of("19".getBytes()))
                .add("col_decimal", BUILDER.of(new BigDecimal("23")))
                .add("col_timestamp", BUILDER.of(Instant.ofEpochMilli(29)))
                .add("col_date", BUILDER.of(LocalDate.ofEpochDay(31)))
                .add(
                        "col_datetime",
                        BUILDER.of(LocalDateTime.ofEpochSecond(37, 37, ZoneOffset.UTC)))
                .add("col_null", BUILDER.ofNull())
                .build()
    };

    @Test
    void testConvertingFromVariant() {
        assertThat(Stream.of(TEST_VARIANTS))
                .map(o -> InternalObjectConverter.convertToInternal(o, DataTypes.VARIANT()))
                .containsExactly(TEST_VARIANTS);
    }

    @Test
    void testConvertingToVariant() {
        assertThat(Stream.of(TEST_VARIANTS))
                .map(o -> JavaObjectConverter.convertToJava(o, DataTypes.VARIANT()))
                .containsExactly(TEST_VARIANTS);
    }

    @Test
    void testVariantTypeCoercion() {
        List<BinaryStringData> expectedStringResult =
                Stream.of(
                                "true",
                                "2",
                                "3",
                                "5",
                                "7",
                                "\"11\"",
                                "13.0",
                                "17.0",
                                "\"MTk=\"",
                                "23",
                                "\"1970-01-01T00:00:00.029+00:00\"",
                                "\"1970-02-01\"",
                                "\"1970-01-01T00:00:37\"",
                                "null",
                                "[true,2,3,5,7,\"11\",13.0,17.0,\"MTk=\",23,\"1970-01-01T00:00:00.029+00:00\",\"1970-02-01\",\"1970-01-01T00:00:37\",null]",
                                "{\"col_bigint\":7,\"col_bool\":true,\"col_bytes\":\"MTk=\",\"col_date\":\"1970-02-01\",\"col_datetime\":\"1970-01-01T00:00:37\",\"col_decimal\":23,\"col_double\":13.0,\"col_float\":17.0,\"col_int\":5,\"col_null\":null,\"col_shortint\":3,\"col_string\":\"11\",\"col_timestamp\":\"1970-01-01T00:00:00.029+00:00\",\"col_tinyint\":2}")
                        .map(BinaryStringData::new)
                        .collect(Collectors.toList());

        assertThat(Stream.of(TEST_VARIANTS))
                .map(
                        variant ->
                                SchemaMergingUtils.coerceObject(
                                        "UTC", variant, DataTypes.VARIANT(), DataTypes.STRING()))
                .containsExactlyElementsOf(expectedStringResult);
    }
}
