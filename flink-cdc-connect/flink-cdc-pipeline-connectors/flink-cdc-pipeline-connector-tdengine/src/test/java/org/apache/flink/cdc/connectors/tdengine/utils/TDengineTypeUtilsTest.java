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

package org.apache.flink.cdc.connectors.tdengine.utils;

import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link TDengineTypeUtils}. */
class TDengineTypeUtilsTest {

    @Test
    void testMapsPrimitiveTypes() {
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.BOOLEAN(), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("BOOL");
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.INT(), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("INT");
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.DOUBLE(), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("DOUBLE");
    }

    @Test
    void testUsesSourceStringAndBinaryLengths() {
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.VARCHAR(32), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("NCHAR(32)");
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.VARBINARY(64), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("VARBINARY(64)");
    }

    @Test
    void testCapsStringLengthByConfiguredDefault() {
        Assertions.assertThat(
                        TDengineTypeUtils.toTDengineType(
                                DataTypes.VARCHAR(1024), false, TDengineTestUtils.defaultConfig()))
                .isEqualTo("NCHAR(256)");
    }

    @Test
    void testRejectsInvalidTimestampType() {
        Assertions.assertThatThrownBy(
                        () ->
                                TDengineTypeUtils.toTDengineType(
                                        DataTypes.DOUBLE(),
                                        true,
                                        TDengineTestUtils.defaultConfig()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("timestamp.field");
    }
}
