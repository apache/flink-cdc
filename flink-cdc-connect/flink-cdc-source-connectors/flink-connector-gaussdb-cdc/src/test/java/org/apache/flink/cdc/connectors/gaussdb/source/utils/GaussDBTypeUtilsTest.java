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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GaussDBTypeUtilsTest {

    @Test
    void convertNullAndBlankTypeNameToNullType() {
        assertThat(GaussDBTypeUtils.convertGaussDBType(null).getLogicalType().getTypeRoot())
                .isEqualTo(LogicalTypeRoot.NULL);
        assertThat(GaussDBTypeUtils.convertGaussDBType(" ").getLogicalType().getTypeRoot())
                .isEqualTo(LogicalTypeRoot.NULL);
    }

    @Test
    void convertsAllPrdDefinedTypes() {
        assertRoot("INTEGER", LogicalTypeRoot.INTEGER);
        assertRoot("INT4", LogicalTypeRoot.INTEGER);
        assertRoot("pg_catalog.int4", LogicalTypeRoot.INTEGER);

        assertRoot("BIGINT", LogicalTypeRoot.BIGINT);
        assertRoot("INT8", LogicalTypeRoot.BIGINT);

        assertRoot("SMALLINT", LogicalTypeRoot.SMALLINT);
        assertRoot("INT2", LogicalTypeRoot.SMALLINT);

        assertDecimal("DECIMAL(10,2)", 10, 2);
        assertDecimal("NUMERIC(18)", 18, 0);
        assertThat(GaussDBTypeUtils.convertGaussDBType("NUMERIC").getLogicalType().getTypeRoot())
                .isEqualTo(LogicalTypeRoot.DECIMAL);

        assertRoot("REAL", LogicalTypeRoot.FLOAT);
        assertRoot("FLOAT4", LogicalTypeRoot.FLOAT);

        assertRoot("DOUBLE PRECISION", LogicalTypeRoot.DOUBLE);
        assertRoot("FLOAT8", LogicalTypeRoot.DOUBLE);

        assertRoot("VARCHAR(255)", LogicalTypeRoot.VARCHAR);
        assertRoot("CHARACTER VARYING(255)", LogicalTypeRoot.VARCHAR);
        assertRoot("TEXT", LogicalTypeRoot.VARCHAR);

        assertRoot("BOOLEAN", LogicalTypeRoot.BOOLEAN);
        assertRoot("DATE", LogicalTypeRoot.DATE);
        assertRoot("TIME", LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
        assertRoot("TIMESTAMP", LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        assertRoot("TIMESTAMPTZ", LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        assertRoot("timestamp(3) with time zone", LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        assertRoot("BYTEA", LogicalTypeRoot.VARBINARY);

        assertRoot("JSON", LogicalTypeRoot.VARCHAR);
        assertRoot("JSONB", LogicalTypeRoot.VARCHAR);
    }

    private static void assertRoot(String typeName, LogicalTypeRoot expectedRoot) {
        DataType dataType = GaussDBTypeUtils.convertGaussDBType(typeName);
        LogicalType logicalType = dataType.getLogicalType();
        assertThat(logicalType.getTypeRoot()).isEqualTo(expectedRoot);
    }

    private static void assertDecimal(String typeName, int expectedPrecision, int expectedScale) {
        DataType dataType = GaussDBTypeUtils.convertGaussDBType(typeName);
        assertThat(dataType.getLogicalType()).isInstanceOf(DecimalType.class);
        DecimalType decimalType = (DecimalType) dataType.getLogicalType();
        assertThat(decimalType.getPrecision()).isEqualTo(expectedPrecision);
        assertThat(decimalType.getScale()).isEqualTo(expectedScale);
    }
}
