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

package org.apache.flink.cdc.connectors.mysql.sink.type;

import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.connectors.jdbc.catalog.JdbcColumn;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MySqlTypeTransformerTest {
    @Test
    void testTinyintType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        TinyIntType charType = new TinyIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("TINYINT", jdbcColumn.getColumnType());
        assertEquals("TINYINT", jdbcColumn.getDataType());
    }

    @Test
    void testSmallIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        SmallIntType charType = new SmallIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("SMALLINT", jdbcColumn.getColumnType());
        assertEquals("SMALLINT", jdbcColumn.getDataType());
    }

    @Test
    void testIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        IntType charType = new IntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("INT", jdbcColumn.getColumnType());
        assertEquals("INT", jdbcColumn.getDataType());
    }

    @Test
    void testBigIntType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        BigIntType charType = new BigIntType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("BIGINT", jdbcColumn.getColumnType());
        assertEquals("BIGINT", jdbcColumn.getDataType());
    }

    @Test
    void testFloatType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        FloatType charType = new FloatType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("FLOAT", jdbcColumn.getColumnType());
        assertEquals("FLOAT", jdbcColumn.getDataType());
    }

    @Test
    void testDoubleType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        DoubleType charType = new DoubleType();
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("DOUBLE", jdbcColumn.getColumnType());
        assertEquals("DOUBLE", jdbcColumn.getDataType());
    }

    @Test
    void testDecimalType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        DecimalType charType = new DecimalType(10, 2);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("DECIMAL(10, 2)", jdbcColumn.getColumnType());
        assertEquals("DECIMAL", jdbcColumn.getDataType());
        assertEquals(10, jdbcColumn.getLength());
        assertEquals(2, jdbcColumn.getScale());
    }

    @Test
    void testVisitCharType() {
        JdbcColumn.Builder builder = new JdbcColumn.Builder();
        CharType charType = new CharType(10);
        MySqlTypeTransformer transformer = new MySqlTypeTransformer(builder);
        transformer.visit(charType);

        JdbcColumn jdbcColumn = builder.build();
        assertEquals("CHAR(10)", jdbcColumn.getColumnType());
        assertEquals("CHAR", jdbcColumn.getDataType());
        assertEquals(10, jdbcColumn.getLength());
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
        assertEquals("TINYINT(1)", jdbcColumn.getColumnType());
        assertEquals("TINYINT", jdbcColumn.getDataType());
    }
}
