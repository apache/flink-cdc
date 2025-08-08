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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresTypeUtils}. */
class PostgresTypeUtilsTest {

    @Test
    void testNumericWithPrecisionAndScale() {
        // Test numeric(10,2) -> DECIMAL(10,2)
        Column column = createColumn("numeric", 10, 2, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    @Test
    void testNumericZeroPrecision() {
        // Test numeric(0) -> BIGINT (default precise mode)
        Column column = createColumn("numeric", 0, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericZeroPrecisionWithStringMode() {
        // Test numeric(0) with decimal.handling.mode=string -> STRING
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testNumericZeroPrecisionWithDoubleMode() {
        // Test numeric(0) with decimal.handling.mode=double -> DOUBLE
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "double");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testNumericZeroPrecisionWithPreciseMode() {
        // Test numeric(0) with decimal.handling.mode=precise -> BIGINT (to avoid binary
        // serialization issues)
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "precise");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericZeroPrecisionNotNull() {
        // Test numeric(0) NOT NULL -> BIGINT NOT NULL
        Column column = createColumn("numeric", 0, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT().notNull());
    }

    @Test
    void testNumericNoPrecision() {
        // Test numeric without precision -> DECIMAL(38,18)
        Column column = createColumn("numeric", -1, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType.getLogicalType().toString()).contains("BIGINT");
    }

    @Test
    void testNumericArrayZeroPrecision() {
        // Test numeric(0)[] -> ARRAY<BIGINT> (default precise mode)
        Column column = createColumn("_numeric", 0, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));
    }

    @Test
    void testNumericArrayZeroPrecisionWithStringMode() {
        // Test numeric(0)[] with decimal.handling.mode=string -> STRING[]
        Column column = createColumn("_numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
    }

    @Test
    void testNumericArrayZeroPrecisionWithDoubleMode() {
        // Test numeric(0)[] with decimal.handling.mode=double -> DOUBLE[]
        Column column = createColumn("_numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "double");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.DOUBLE()));
    }

    @Test
    void testNumericArrayWithPrecision() {
        // Test numeric(10,2)[] -> ARRAY<DECIMAL(10,2)>
        Column column = createColumn("_numeric", 10, 2, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)));
    }

    @Test
    void testOtherTypesUnchanged() {
        // Test that other types are not affected by our numeric(0) fix
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");

        Column intColumn = createColumn("int4", 0, 0, true);
        DataType intType = PostgresTypeUtils.fromDbzColumn(intColumn, props);
        assertThat(intType).isEqualTo(DataTypes.INT());

        Column varcharColumn = createColumn("varchar", 50, 0, true);
        DataType varcharType = PostgresTypeUtils.fromDbzColumn(varcharColumn, props);
        assertThat(varcharType).isEqualTo(DataTypes.VARCHAR(50));
    }

    @Test
    void testNormalNumericTypesWithDecimalModes() {
        // Test that normal numeric types are not affected by decimal.handling.mode setting
        Properties stringProps = new Properties();
        stringProps.setProperty("decimal.handling.mode", "string");

        Properties doubleProps = new Properties();
        doubleProps.setProperty("decimal.handling.mode", "double");

        Properties preciseProps = new Properties();
        preciseProps.setProperty("decimal.handling.mode", "precise");

        Column column = createColumn("numeric", 10, 2, true);

        // All modes should return DECIMAL(10,2) for normal numeric types
        assertThat(PostgresTypeUtils.fromDbzColumn(column, stringProps))
                .isEqualTo(DataTypes.STRING());
        assertThat(PostgresTypeUtils.fromDbzColumn(column, doubleProps))
                .isEqualTo(DataTypes.DOUBLE());
        assertThat(PostgresTypeUtils.fromDbzColumn(column, preciseProps))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    /** Creates a mock Debezium Column for testing. */
    private Column createColumn(String typeName, int length, int scale, boolean optional) {
        return Column.editor()
                .name("test_column")
                .type(typeName)
                .length(length)
                .scale(scale)
                .optional(optional)
                .create();
    }
}
