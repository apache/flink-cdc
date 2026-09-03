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

package org.apache.flink.cdc.connectors.oracle.source.utils;

import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OracleTypeUtils}. */
class OracleTypeUtilsTest {

    @Test
    void testNormalizeDecimalPrecisionAndScale() {
        DecimalType precisionUnset =
                (DecimalType) OracleTypeUtils.fromDbzColumn(decimalColumn(0, 0)).getLogicalType();
        assertThat(precisionUnset.getPrecision()).isEqualTo(38);
        assertThat(precisionUnset.getScale()).isEqualTo(0);

        DecimalType precisionTooLarge =
                (DecimalType) OracleTypeUtils.fromDbzColumn(decimalColumn(50, 0)).getLogicalType();
        assertThat(precisionTooLarge.getPrecision()).isEqualTo(38);
        assertThat(precisionTooLarge.getScale()).isEqualTo(0);

        DecimalType negativeScale =
                (DecimalType)
                        OracleTypeUtils.fromDbzColumn(decimalColumn(20, -127)).getLogicalType();
        assertThat(negativeScale.getPrecision()).isEqualTo(20);
        assertThat(negativeScale.getScale()).isEqualTo(0);

        DecimalType scaleGreaterThanPrecision =
                (DecimalType) OracleTypeUtils.fromDbzColumn(decimalColumn(10, 20)).getLogicalType();
        assertThat(scaleGreaterThanPrecision.getPrecision()).isEqualTo(10);
        assertThat(scaleGreaterThanPrecision.getScale()).isEqualTo(10);
    }

    private static Column decimalColumn(int precision, int scale) {
        return Column.editor()
                .name("ID")
                .jdbcType(Types.DECIMAL)
                .length(precision)
                .scale(scale)
                .optional(false)
                .create();
    }
}
