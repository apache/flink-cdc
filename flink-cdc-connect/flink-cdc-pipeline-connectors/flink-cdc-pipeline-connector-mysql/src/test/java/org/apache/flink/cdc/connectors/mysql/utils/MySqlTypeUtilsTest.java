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

package org.apache.flink.cdc.connectors.mysql.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit tests for {@link MySqlTypeUtils}. */
public class MySqlTypeUtilsTest {
    private static final boolean TINY_INT_1_IS_BIT = true;

    /** Build a nullable column with the given MySQL type name and default length/scale. */
    private static Column column(String typeName) {
        return column(typeName, 10, 0);
    }

    /** Build a nullable column with the given MySQL type name, length and scale. */
    private static Column column(String typeName, int length, int scale) {
        return Column.editor()
                .name("test_col")
                .type(typeName)
                .length(length)
                .scale(scale)
                .optional(true)
                .create();
    }

    private static DataType fromColumn(String typeName) {
        return MySqlTypeUtils.fromDbzColumn(column(typeName), TINY_INT_1_IS_BIT);
    }

    private static DataType fromColumn(String typeName, int length, int scale) {
        return MySqlTypeUtils.fromDbzColumn(column(typeName, length, scale), TINY_INT_1_IS_BIT);
    }

    // ---- ZEROFILL (without UNSIGNED) should map the same as UNSIGNED ZEROFILL ----

    @Test
    public void testTinyIntZerofill() {
        assertThat(fromColumn("TINYINT ZEROFILL")).isEqualTo(DataTypes.SMALLINT());
    }

    @Test
    public void testSmallIntZerofill() {
        assertThat(fromColumn("SMALLINT ZEROFILL")).isEqualTo(DataTypes.INT());
    }

    @Test
    public void testMediumIntZerofill() {
        assertThat(fromColumn("MEDIUMINT ZEROFILL")).isEqualTo(DataTypes.INT());
    }

    @Test
    public void testIntZerofill() {
        // This is the type that triggered the original UnsupportedOperationException.
        assertThat(fromColumn("INT ZEROFILL")).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testIntegerZerofill() {
        assertThat(fromColumn("INTEGER ZEROFILL")).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testBigIntZerofill() {
        assertThat(fromColumn("BIGINT ZEROFILL")).isEqualTo(DataTypes.DECIMAL(20, 0));
    }

    @Test
    public void testRealZerofill() {
        assertThat(fromColumn("REAL ZEROFILL")).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testFloatZerofillUnspecifiedLength() {
        // FLOAT without explicit length -> FLOAT
        Column col =
                Column.editor()
                        .name("test_col")
                        .type("FLOAT ZEROFILL")
                        .length(-1)
                        .optional(true)
                        .create();
        assertThat(MySqlTypeUtils.fromDbzColumn(col, TINY_INT_1_IS_BIT))
                .isEqualTo(DataTypes.FLOAT());
    }

    @Test
    public void testFloatZerofillWithLength() {
        // FLOAT with explicit length -> treated as DOUBLE
        assertThat(fromColumn("FLOAT ZEROFILL", 10, 2)).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testDoubleZerofill() {
        assertThat(fromColumn("DOUBLE ZEROFILL")).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testDoublePrecisionZerofill() {
        assertThat(fromColumn("DOUBLE PRECISION ZEROFILL")).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testNumericZerofill() {
        assertThat(fromColumn("NUMERIC ZEROFILL", 10, 0)).isEqualTo(DataTypes.DECIMAL(10, 0));
    }

    @Test
    public void testFixedZerofill() {
        assertThat(fromColumn("FIXED ZEROFILL", 10, 2)).isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    @Test
    public void testDecimalZerofill() {
        assertThat(fromColumn("DECIMAL ZEROFILL", 10, 2)).isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    // ---- ZEROFILL maps identically to UNSIGNED ZEROFILL ----

    @Test
    public void testIntZerofillEqualsIntUnsignedZerofill() {
        assertThat(fromColumn("INT ZEROFILL")).isEqualTo(fromColumn("INT UNSIGNED ZEROFILL"));
    }

    @Test
    public void testBigIntZerofillEqualsBigIntUnsignedZerofill() {
        assertThat(fromColumn("BIGINT ZEROFILL")).isEqualTo(fromColumn("BIGINT UNSIGNED ZEROFILL"));
    }

    @Test
    public void testDecimalZerofillEqualsDecimalUnsignedZerofill() {
        assertThat(fromColumn("DECIMAL ZEROFILL", 10, 2))
                .isEqualTo(fromColumn("DECIMAL UNSIGNED ZEROFILL", 10, 2));
    }

    // ---- Non-null column should produce NOT NULL type ----

    @Test
    public void testIntZerofillNotNull() {
        Column col =
                Column.editor()
                        .name("test_col")
                        .type("INT ZEROFILL")
                        .length(10)
                        .scale(0)
                        .optional(false)
                        .create();
        assertThat(MySqlTypeUtils.fromDbzColumn(col, TINY_INT_1_IS_BIT))
                .isEqualTo(DataTypes.BIGINT().notNull());
    }
}
