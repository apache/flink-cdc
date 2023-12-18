/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververical.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link StarRocksUtils.CdcDataTypeTransformer}. */
public class CdcDataTypeTransformerTest {

    @Test
    public void testCharType() {
        // map to char of StarRocks if CDC length <= StarRocksUtils.MAX_CHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_char").setOrdinalPosition(0);
        new CharType(1).accept(new StarRocksUtils.CdcDataTypeTransformer(smallLengthBuilder));
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
                .accept(new StarRocksUtils.CdcDataTypeTransformer(largeLengthBuilder));
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
    public void testVarCharType() {
        // the length fo StarRocks should be 3 times as that of CDC if CDC length * 3 <=
        // StarRocksUtils.MAX_VARCHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_varchar").setOrdinalPosition(0);
        new VarCharType(3).accept(new StarRocksUtils.CdcDataTypeTransformer(smallLengthBuilder));
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
                .accept(new StarRocksUtils.CdcDataTypeTransformer(largeLengthBuilder));
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
