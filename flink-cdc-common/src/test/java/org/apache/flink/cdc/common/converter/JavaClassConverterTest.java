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

import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test cases for {@link JavaClassConverter}. */
class JavaClassConverterTest {

    @Test
    void testConvertingFullTypes() {
        assertThat(
                        Stream.of(
                                DataTypes.BOOLEAN(),
                                DataTypes.BYTES(),
                                DataTypes.BINARY(10),
                                DataTypes.VARBINARY(10),
                                DataTypes.CHAR(10),
                                DataTypes.VARCHAR(10),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE(),
                                DataTypes.FLOAT(),
                                DataTypes.DECIMAL(6, 3),
                                DataTypes.DATE(),
                                DataTypes.TIME(),
                                DataTypes.TIME(6),
                                DataTypes.TIMESTAMP(),
                                DataTypes.TIMESTAMP(6),
                                DataTypes.TIMESTAMP_LTZ(),
                                DataTypes.TIMESTAMP_LTZ(6),
                                DataTypes.TIMESTAMP_TZ(),
                                DataTypes.TIMESTAMP_TZ(6),
                                DataTypes.ARRAY(DataTypes.BIGINT()),
                                DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.STRING()),
                                DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.STRING(), "desc")),
                                DataTypes.ROW(DataTypes.SMALLINT(), DataTypes.STRING()),
                                DataTypes.VARIANT()))
                .map(JavaClassConverter::toJavaClass)
                .map(Class::getCanonicalName)
                .containsExactly(
                        "java.lang.Boolean",
                        "byte[]",
                        "byte[]",
                        "byte[]",
                        "java.lang.String",
                        "java.lang.String",
                        "java.lang.String",
                        "java.lang.Integer",
                        "java.lang.Byte",
                        "java.lang.Short",
                        "java.lang.Long",
                        "java.lang.Double",
                        "java.lang.Float",
                        "java.math.BigDecimal",
                        "java.time.LocalDate",
                        "java.time.LocalTime",
                        "java.time.LocalTime",
                        "java.time.LocalDateTime",
                        "java.time.LocalDateTime",
                        "java.time.Instant",
                        "java.time.Instant",
                        "java.time.ZonedDateTime",
                        "java.time.ZonedDateTime",
                        "java.util.List",
                        "java.util.Map",
                        "java.util.List",
                        "java.util.List",
                        "org.apache.flink.cdc.common.types.variant.Variant");
    }
}
