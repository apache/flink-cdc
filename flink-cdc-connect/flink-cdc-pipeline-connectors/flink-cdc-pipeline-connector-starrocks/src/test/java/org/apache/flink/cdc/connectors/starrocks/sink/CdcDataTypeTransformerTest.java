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
        // Map to DECIMAL of StarRocks if column is DECIMAL type and not primary key.
        StarRocksColumn.Builder noPrimaryKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("no_primary_key").setOrdinalPosition(0);
        new DecimalType(20, 1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, noPrimaryKeyBuilder));
        StarRocksColumn noPrimaryKeyColumn = noPrimaryKeyBuilder.build();
        assertEquals("no_primary_key", noPrimaryKeyColumn.getColumnName());
        assertEquals(0, noPrimaryKeyColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.DECIMAL, noPrimaryKeyColumn.getDataType());
        assertEquals(Integer.valueOf(20), noPrimaryKeyColumn.getColumnSize().orElse(null));
        assertEquals(Integer.valueOf(1), noPrimaryKeyColumn.getDecimalDigits().get());
        assertTrue(noPrimaryKeyColumn.isNullable());

        // Map to VARCHAR of StarRocks if column is DECIMAL type and primary key.
        StarRocksColumn.Builder primaryKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(1);
        new DecimalType(20, 1)
                .notNull()
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, primaryKeyBuilder));
        StarRocksColumn primaryKeyColumn = primaryKeyBuilder.build();
        assertEquals("primary_key", primaryKeyColumn.getColumnName());
        assertEquals(1, primaryKeyColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, primaryKeyColumn.getDataType());
        assertEquals(Integer.valueOf(22), primaryKeyColumn.getColumnSize().orElse(null));
        assertTrue(!primaryKeyColumn.isNullable());

        // Map to VARCHAR of StarRocks if column is DECIMAL type and primary key
        // DECIMAL(20,0) is common in cdc pipeline, for example, the upstream cdc source is unsigned
        // BIGINT.
        StarRocksColumn.Builder unsignedBigIntKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(1);
        new DecimalType(20, 0)
                .notNull()
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, unsignedBigIntKeyBuilder));
        StarRocksColumn unsignedBigIntColumn = unsignedBigIntKeyBuilder.build();
        assertEquals("primary_key", unsignedBigIntColumn.getColumnName());
        assertEquals(1, unsignedBigIntColumn.getOrdinalPosition());
        assertEquals(StarRocksUtils.VARCHAR, unsignedBigIntColumn.getDataType());
        assertEquals(Integer.valueOf(21), unsignedBigIntColumn.getColumnSize().orElse(null));
        assertTrue(!unsignedBigIntColumn.isNullable());
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
