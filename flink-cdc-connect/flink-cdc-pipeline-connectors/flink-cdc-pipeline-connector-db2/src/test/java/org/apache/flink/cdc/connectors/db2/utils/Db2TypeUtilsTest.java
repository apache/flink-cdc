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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Db2TypeUtils}. */
class Db2TypeUtilsTest {

    @Test
    void testDecFloatMapsToDoubleOrDecimal() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.OTHER, "DECFLOAT", 16, null)))
                .isEqualTo(DataTypes.DOUBLE());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.OTHER, "DECFLOAT", 34, null)))
                .isEqualTo(DataTypes.DECIMAL(34, 0));
    }

    @Test
    void testXmlMapsToString() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.SQLXML, "XML", 0, null)))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void testCharAndVarcharPreserveLength() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.CHAR, "CHAR", 10, null)))
                .isEqualTo(DataTypes.CHAR(10));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.VARCHAR, "VARCHAR", 128, null)))
                .isEqualTo(DataTypes.VARCHAR(128));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.CHAR, "CHAR", 0, null)))
                .isEqualTo(DataTypes.STRING());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.VARCHAR, "VARCHAR", 0, null)))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void testNumericTypes() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.SMALLINT, "SMALLINT", 0, null)))
                .isEqualTo(DataTypes.SMALLINT());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.INTEGER, "INTEGER", 0, null)))
                .isEqualTo(DataTypes.INT());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.BIGINT, "BIGINT", 0, null)))
                .isEqualTo(DataTypes.BIGINT());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.REAL, "REAL", 0, null)))
                .isEqualTo(DataTypes.FLOAT());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.DOUBLE, "DOUBLE", 0, null)))
                .isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testDecimalPreservesPrecisionAndScale() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.DECIMAL, "DECIMAL", 10, 2)))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.NUMERIC, "NUMERIC", 31, 0)))
                .isEqualTo(DataTypes.DECIMAL(31, 0));
    }

    @Test
    void testTemporalTypes() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.DATE, "DATE", 0, null)))
                .isEqualTo(DataTypes.DATE());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIME, "TIME", 0, null)))
                .isEqualTo(DataTypes.TIME(0));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIMESTAMP, "TIMESTAMP", 0, 6)))
                .isEqualTo(DataTypes.TIMESTAMP(6));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIMESTAMP, "TIMESTAMP", 0, null)))
                .isEqualTo(DataTypes.TIMESTAMP(6));
    }

    @Test
    void testBinaryTypes() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.BLOB, "BLOB", 0, null)))
                .isEqualTo(DataTypes.BYTES());
        assertThat(
                        Db2TypeUtils.fromDbzColumn(
                                column(Types.BINARY, "CHAR () FOR BIT DATA", 8, null)))
                .isEqualTo(DataTypes.BYTES());
        assertThat(
                        Db2TypeUtils.fromDbzColumn(
                                column(Types.VARBINARY, "VARCHAR () FOR BIT DATA", 8, null)))
                .isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testNotNullableColumn() {
        Column notNullColumn =
                Column.editor()
                        .name("c")
                        .jdbcType(Types.INTEGER)
                        .type("INTEGER")
                        .optional(false)
                        .create();
        assertThat(Db2TypeUtils.fromDbzColumn(notNullColumn)).isEqualTo(DataTypes.INT().notNull());
    }

    @Test
    void testUnsupportedType() {
        assertThatThrownBy(
                        () ->
                                Db2TypeUtils.fromDbzColumn(
                                        column(Types.JAVA_OBJECT, "unknown_type", 0, null)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("unknown_type");
    }

    private static Column column(int jdbcType, String typeName, int length, Integer scale) {
        ColumnEditor editor =
                Column.editor()
                        .name("c")
                        .jdbcType(jdbcType)
                        .type(typeName)
                        .length(length)
                        .optional(true);
        if (scale != null) {
            editor.scale(scale);
        }
        return editor.create();
    }
}
