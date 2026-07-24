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

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class Db2TypeUtilsTest {

    @Test
    void testCharacterAndLargeObjectTypes() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.CHAR, "CHAR", 5, 0, true)))
                .isEqualTo(DataTypes.CHAR(5));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.VARCHAR, "VARCHAR", 16, 0, true)))
                .isEqualTo(DataTypes.VARCHAR(16));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.CLOB, "CLOB", 0, 0, true)))
                .isEqualTo(DataTypes.STRING());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.BLOB, "BLOB", 0, 0, true)))
                .isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testNumericAndTemporalTypes() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.SMALLINT, "SMALLINT", 0, 0, true)))
                .isEqualTo(DataTypes.SMALLINT());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.INTEGER, "INTEGER", 0, 0, false)))
                .isEqualTo(DataTypes.INT().notNull());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.OTHER, "DECFLOAT", 16, 0, true)))
                .isEqualTo(DataTypes.DOUBLE());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.DECIMAL, "DECIMAL", 12, 3, true)))
                .isEqualTo(DataTypes.DECIMAL(12, 3));
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIMESTAMP, "TIMESTAMP", 3, 0, true)))
                .isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testTimestampPrecisionDefaultsAndClampsFromLength() {
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIMESTAMP, "TIMESTAMP", -1, 0, true)))
                .isEqualTo(DataTypes.TIMESTAMP());
        assertThat(Db2TypeUtils.fromDbzColumn(column(Types.TIMESTAMP, "TIMESTAMP", 12, 0, true)))
                .isEqualTo(DataTypes.TIMESTAMP(9));
    }

    private static io.debezium.relational.Column column(
            int jdbcType, String typeName, int length, int scale, boolean optional) {
        return io.debezium.relational.Column.editor()
                .name("C")
                .jdbcType(jdbcType)
                .type(typeName, typeName)
                .length(length)
                .scale(scale)
                .optional(optional)
                .create();
    }
}
