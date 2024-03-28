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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.VarCharType;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link
 * org.apache.flink.cdc.connectors.starrocks.sink.StarRocksUtils.CdcDataTypeTransformer}.
 */
public class CdcDataTypeTransformerTest {

    @Test
    public void testCharType() {
        // map to char of StarRocks if CDC length <= StarRocksUtils.MAX_CHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_char").setOrdinalPosition(0);
        new CharType(1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        assertEquals("small_char", smallLengthColumn.getColumnName());
        assertEquals(0, smallLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.CHAR, smallLengthColumn.getDataType());
        assertEquals(Integer.valueOf(3), smallLengthColumn.getColumnSize().orElse(null));
        assertTrue(smallLengthColumn.isNullable());

        // map to varchar of StarRocks if CDC length > StarRocksUtils.MAX_CHAR_SIZE
        StarRocksColumn.Builder largeLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("large_char").setOrdinalPosition(1);
        new CharType(StarRocksUtils.MAX_CHAR_SIZE)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, largeLengthBuilder));
        StarRocksColumn largeLengthColumn = largeLengthBuilder.build();
        assertEquals("large_char", largeLengthColumn.getColumnName());
        assertEquals(1, largeLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, largeLengthColumn.getDataType());
        assertEquals(
                Integer.valueOf(StarRocksUtils.MAX_CHAR_SIZE * 3),
                largeLengthColumn.getColumnSize().orElse(null));
        assertTrue(largeLengthColumn.isNullable());
    }

    @Test
    public void testCharTypeForPrimaryKey() {
        // map to varchar of StarRocks if column is primary key
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new CharType(1).accept(new StarRocksUtils.CdcDataTypeTransformer(true, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        assertEquals("primary_key", smallLengthColumn.getColumnName());
        assertEquals(0, smallLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, smallLengthColumn.getDataType());
        assertEquals(Integer.valueOf(3), smallLengthColumn.getColumnSize().orElse(null));
        assertTrue(smallLengthColumn.isNullable());
    }

    @Test
    public void testDecimalForPrimaryKey() {
        // map to Int of StarRocks if primary key column with precision < 10 and scale = 0
        StarRocksColumn.Builder intLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new DecimalType(9, 0)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, intLengthBuilder));
        StarRocksColumn intLengthColumn = intLengthBuilder.build();
        assertEquals("primary_key", intLengthColumn.getColumnName());
        assertEquals(0, intLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.INT, intLengthColumn.getDataType());
        assertTrue(intLengthColumn.isNullable());

        // map to BigInt of StarRocks if primary key column with precision < 19 and scale = 0
        StarRocksColumn.Builder bigIntLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new DecimalType(10, 0)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, bigIntLengthBuilder));
        StarRocksColumn bigIntLengthColumn = bigIntLengthBuilder.build();
        assertEquals("primary_key", bigIntLengthColumn.getColumnName());
        assertEquals(0, bigIntLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.BIGINT, bigIntLengthColumn.getDataType());
        assertTrue(bigIntLengthColumn.isNullable());

        // map to LARGEINT of StarRocks if primary key column with precision < 18 and scale = 0
        StarRocksColumn.Builder unsignedBigIntLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new DecimalType(20, 0)
                .accept(
                        new StarRocksUtils.CdcDataTypeTransformer(
                                true, unsignedBigIntLengthBuilder));
        StarRocksColumn unsignedBigIntLengthColumn = unsignedBigIntLengthBuilder.build();
        assertEquals("primary_key", unsignedBigIntLengthColumn.getColumnName());
        assertEquals(0, unsignedBigIntLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.LARGEINT, unsignedBigIntLengthColumn.getDataType());
        assertTrue(unsignedBigIntLengthColumn.isNullable());

        // map to Varchar of StarRocks if primary key column with precision >= 38 and scale = 0
        StarRocksColumn.Builder outOfBoundLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new DecimalType(38, 0)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, outOfBoundLengthBuilder));
        StarRocksColumn outOfBoundColumn = outOfBoundLengthBuilder.build();
        assertEquals("primary_key", outOfBoundColumn.getColumnName());
        assertEquals(0, outOfBoundColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, outOfBoundColumn.getDataType());
        assertEquals(Integer.valueOf(38), outOfBoundColumn.getColumnSize().orElse(null));
        assertTrue(unsignedBigIntLengthColumn.isNullable());

        // map to Varchar of StarRocks if primary key column with scale > 0
        StarRocksColumn.Builder scaleBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new DecimalType(12, 1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, scaleBuilder));
        StarRocksColumn scaleBoundColumn = scaleBuilder.build();
        assertEquals("primary_key", scaleBoundColumn.getColumnName());
        assertEquals(0, scaleBoundColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, scaleBoundColumn.getDataType());
        assertEquals(Integer.valueOf(12), scaleBoundColumn.getColumnSize().orElse(null));
        assertTrue(unsignedBigIntLengthColumn.isNullable());
    }

    @Test
    public void testVarCharType() {
        // the length fo StarRocks should be 3 times as that of CDC if CDC length * 3 <=
        // StarRocksUtils.MAX_VARCHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_varchar").setOrdinalPosition(0);
        new VarCharType(3)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        assertEquals("small_varchar", smallLengthColumn.getColumnName());
        assertEquals(0, smallLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, smallLengthColumn.getDataType());
        assertEquals(Integer.valueOf(9), smallLengthColumn.getColumnSize().orElse(null));
        assertTrue(smallLengthColumn.isNullable());

        // the length fo StarRocks should be StarRocksUtils.MAX_VARCHAR_SIZE if CDC length * 3 >
        // StarRocksUtils.MAX_VARCHAR_SIZE
        StarRocksColumn.Builder largeLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("large_varchar").setOrdinalPosition(1);
        new CharType(StarRocksUtils.MAX_VARCHAR_SIZE + 1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, largeLengthBuilder));
        StarRocksColumn largeLengthColumn = largeLengthBuilder.build();
        assertEquals("large_varchar", largeLengthColumn.getColumnName());
        assertEquals(1, largeLengthColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, largeLengthColumn.getDataType());
        assertEquals(
                Integer.valueOf(StarRocksUtils.MAX_VARCHAR_SIZE),
                largeLengthColumn.getColumnSize().orElse(null));
        assertTrue(largeLengthColumn.isNullable());
    }
}
