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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.iceberg.expressions.Literal;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergTypeUtils}. */
public class IcebergTypeUtilsTest {

    @Test
    public void testParseDefaultValueNull() {
        assertThat(IcebergTypeUtils.parseDefaultValue(null, DataTypes.STRING())).isNull();
    }

    @Test
    public void testParseDefaultValueString() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("hello", DataTypes.STRING());
        assertThat(result).isNotNull();
        assertThat(result.value().toString()).isEqualTo("hello");
    }

    @Test
    public void testParseDefaultValueVarchar() {
        Literal<?> result =
                IcebergTypeUtils.parseDefaultValue("John Smith", DataTypes.VARCHAR(255));
        assertThat(result).isNotNull();
        assertThat(result.value().toString()).isEqualTo("John Smith");
    }

    @Test
    public void testParseDefaultValueBoolean() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("true", DataTypes.BOOLEAN());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(true);

        result = IcebergTypeUtils.parseDefaultValue("false", DataTypes.BOOLEAN());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(false);
    }

    @Test
    public void testParseDefaultValueBooleanInvalid() {
        // Non-canonical boolean values should return null
        assertThat(IcebergTypeUtils.parseDefaultValue("1", DataTypes.BOOLEAN())).isNull();
        assertThat(IcebergTypeUtils.parseDefaultValue("yes", DataTypes.BOOLEAN())).isNull();
        assertThat(IcebergTypeUtils.parseDefaultValue("garbage", DataTypes.BOOLEAN())).isNull();
    }

    @Test
    public void testParseDefaultValueInteger() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("42", DataTypes.INT());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(42);
    }

    @Test
    public void testParseDefaultValueTinyInt() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("1", DataTypes.TINYINT());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(1);
    }

    @Test
    public void testParseDefaultValueSmallInt() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("100", DataTypes.SMALLINT());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(100);
    }

    @Test
    public void testParseDefaultValueBigInt() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("9999999999", DataTypes.BIGINT());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(9999999999L);
    }

    @Test
    public void testParseDefaultValueFloat() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("1.5", DataTypes.FLOAT());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(1.5f);
    }

    @Test
    public void testParseDefaultValueDouble() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("3.14", DataTypes.DOUBLE());
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(3.14);
    }

    @Test
    public void testParseDefaultValueDecimal() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("1.23", DataTypes.DECIMAL(10, 2));
        assertThat(result).isNotNull();
        assertThat(result.value()).isEqualTo(new BigDecimal("1.23"));
    }

    @Test
    public void testParseDefaultValueUnsupportedType() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("data", DataTypes.BYTES());
        assertThat(result).isNull();
    }

    @Test
    public void testParseDefaultValueInvalidNumber() {
        Literal<?> result = IcebergTypeUtils.parseDefaultValue("not_a_number", DataTypes.INT());
        assertThat(result).isNull();
    }

    @Test
    public void testParseDefaultValueFunctionExpression() {
        // Function expressions like AUTO_DECREMENT() cannot be parsed as numbers
        Literal<?> result =
                IcebergTypeUtils.parseDefaultValue("AUTO_DECREMENT()", DataTypes.BIGINT());
        assertThat(result).isNull();
    }
}
