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

package org.apache.flink.cdc.connectors.jdbc.mysql.type;

import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcColumn;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test class for {@link MySqlTypeTransformer}. */
class MySqlTypeTransformerTest {
    public static final int POWER_2_8 = (int) Math.pow(2, 8);
    public static final int POWER_2_16 = (int) Math.pow(2, 16);
    public static final int POWER_2_32 = (int) Math.pow(2, 32);
    public static final int POWER_2_24 = (int) Math.pow(2, 24);

    @Test
    void testTinyintType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        TinyIntType charType = new TinyIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("TINYINT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("TINYINT");
    }

    @Test
    void testSmallIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        SmallIntType charType = new SmallIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("SMALLINT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("SMALLINT");
    }

    @Test
    void testIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        IntType charType = new IntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("INT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("INT");
    }

    @Test
    void testBigIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        BigIntType charType = new BigIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("BIGINT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("BIGINT");
    }

    @Test
    void testFloatType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        FloatType charType = new FloatType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("FLOAT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("FLOAT");
    }

    @Test
    void testDoubleType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        DoubleType charType = new DoubleType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("DOUBLE");
        assertThat(jdbcColumn.getDataType()).isEqualTo("DOUBLE");
    }

    @Test
    void testDecimalType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        DecimalType charType = new DecimalType(10, 2);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("DECIMAL(10, 2)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("DECIMAL");
        assertThat(jdbcColumn.getLength()).isEqualTo(10);
        assertThat(jdbcColumn.getScale()).isEqualTo(2);
    }

    @Test
    void testVisitCharType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        CharType charType = new CharType(10);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("CHAR(10)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("CHAR");
        assertThat(jdbcColumn.getLength()).isEqualTo(10);
    }

    // Add similar tests for other types, e.g., VarCharType, BooleanType, DecimalType, etc.

    // Example test for BooleanType
    @Test
    void testVisitBooleanType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        BooleanType booleanType = new BooleanType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(booleanType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("TINYINT(1)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("TINYINT");
    }

    @Test
    void testVisitVarCharTypeLengthLessThan256() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarCharType varCharType = new VarCharType(false, 100);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varCharType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("VARCHAR(100)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("VARCHAR");
        assertThat(jdbcColumn.isNullable()).isFalse();
    }

    @Test
    void testVisitVarCharTypeLengthLessThan65536() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarCharType varCharType = new VarCharType(true, POWER_2_8 + 1);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varCharType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("VARCHAR(257)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("VARCHAR");
        assertThat(jdbcColumn.isNullable()).isTrue();
    }

    @Test
    void testVisitVarCharTypeLengthLessThan16777216() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarCharType varCharType = new VarCharType(false, POWER_2_16 + 1);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varCharType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("MEDIUMTEXT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("MEDIUMTEXT");
        assertThat(jdbcColumn.isNullable()).isFalse();
    }

    @Test
    void testVisitVarCharTypeLengthGreaterThanOrEqualTo16777216() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarCharType varCharType = new VarCharType(true, POWER_2_24 + 1);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varCharType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("LONGTEXT");
        assertThat(jdbcColumn.getDataType()).isEqualTo("LONGTEXT");
        assertThat(jdbcColumn.isNullable()).isTrue();
    }

    @Test
    void testVisitVarBinaryTypeLengthLessThan65536() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarBinaryType varBinaryType = new VarBinaryType(true, POWER_2_16 - 1);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varBinaryType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("VARBINARY(65535)");
        assertThat(jdbcColumn.getDataType()).isEqualTo("VARBINARY");
        assertThat(jdbcColumn.isNullable()).isTrue();
    }

    @Test
    void testVisitVarBinaryTypeLengthLessThan16777216() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarBinaryType varBinaryType = new VarBinaryType(false, POWER_2_16);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varBinaryType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("MEDIUMBLOB");
        assertThat(jdbcColumn.getDataType()).isEqualTo("MEDIUMBLOB");
        assertThat(jdbcColumn.isNullable()).isFalse();
    }

    @Test
    void testVisitVarBinaryTypeLengthLessThan4294967296() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        VarBinaryType varBinaryType = new VarBinaryType(true, POWER_2_24);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(varBinaryType);

        JdbcColumn jdbcColumn = builder.build();
        assertThat(jdbcColumn.getColumnType()).isEqualTo("LONGBLOB");
        assertThat(jdbcColumn.getDataType()).isEqualTo("LONGBLOB");
        assertThat(jdbcColumn.isNullable()).isTrue();
    }
}
