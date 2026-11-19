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

package org.apache.flink.cdc.connectors.oracle.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleTypeUtils}. */
class OracleTypeUtilsTest {

    // --- Bare NUMBER (scale unspecified) tests ---

    @Test
    void testBareNumberShouldBeDecimal3819() {
        // NUMBER (scale unspecified) → DECIMAL(38, 19)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(0)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 19));
    }

    @Test
    void testBareNumberNotNullShouldBeDecimal3819NotNull() {
        // NOT NULL NUMBER → DECIMAL(38, 19).notNull()
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(0)
                        .optional(false)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 19).notNull());
    }

    @Test
    void testNumberPrecision10ScaleAbsentShouldBeDecimal3819() {
        // NUMBER(10) with no scale info → DECIMAL(38, 19) (treated as bare NUMBER)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(10)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 19));
    }

    // --- Explicit integer (scale = 0) tests ---

    @Test
    void testNumberPrecision1ShouldBeBigint() {
        // NUMBER(1, 0) → BIGINT (precision <= 18)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(1)
                        .scale(0)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumberPrecision18ShouldBeBigint() {
        // NUMBER(18, 0) → BIGINT (boundary: precision = 18)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(18)
                        .scale(0)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumberPrecision19ShouldBeDecimal() {
        // NUMBER(19, 0) → DECIMAL(19, 0) (boundary: precision = 19)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(19)
                        .scale(0)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(19, 0));
    }

    @Test
    void testNumberPrecision38ShouldBeDecimal() {
        // NUMBER(38, 0) → DECIMAL(38, 0)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(38)
                        .scale(0)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(38, 0));
    }

    @Test
    void testNumberPrecision19NotNullShouldBeDecimalNotNull() {
        // NOT NULL NUMBER(19, 0) → DECIMAL(19, 0).notNull()
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(19)
                        .scale(0)
                        .optional(false)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(19, 0).notNull());
    }

    // --- Decimal (scale > 0) tests ---

    @Test
    void testNumberWithScaleShouldBeDecimal() {
        // NUMBER(10, 6) → DECIMAL(10, 6)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(10)
                        .scale(6)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(10, 6));
    }

    @Test
    void testNumberWithScaleBoundaryShouldBeDecimal() {
        // NUMBER(38, 36) → DECIMAL(38, 36) (scale at boundary)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(38)
                        .scale(36)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(38, 36));
    }

    // --- NUMBER(*, s) (precision == 0, scale > 0) tests ---

    @Test
    void testNumberStarPrecisionShouldBeDecimal38() {
        // NUMBER(*, 2) → DECIMAL(38, 2) (precision == 0 defaults to MAX_PRECISION)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(0)
                        .scale(2)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 2));
    }

    @Test
    void testNumberStarPrecisionNotNullShouldBeDecimal38NotNull() {
        // NOT NULL NUMBER(*, 4) → DECIMAL(38, 4).notNull()
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(0)
                        .scale(4)
                        .optional(false)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 4).notNull());
    }

    // --- Scale < 0 (negative scale) downgrade tests ---

    @Test
    void testNegativeScaleShouldBeBigint() {
        // NUMBER(10, -2) → BIGINT (negative scale, p+|s|=12 fits in BIGINT)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(10)
                        .scale(-2)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNegativeScaleStarPrecisionShouldBeString() {
        // NUMBER(*, -3) → STRING (negative scale downgraded)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(0)
                        .scale(-3)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    // --- Scale > 36 downgrade tests ---

    @Test
    void testScaleGreaterThan36ShouldBeString() {
        // NUMBER(10, 37) → STRING (scale > 36 downgraded)
        Column column =
                Column.editor()
                        .name("col")
                        .type("NUMBER")
                        .jdbcType(Types.NUMERIC)
                        .length(10)
                        .scale(37)
                        .optional(true)
                        .create();
        DataType result = OracleTypeUtils.fromDbzColumn(column);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }
}
