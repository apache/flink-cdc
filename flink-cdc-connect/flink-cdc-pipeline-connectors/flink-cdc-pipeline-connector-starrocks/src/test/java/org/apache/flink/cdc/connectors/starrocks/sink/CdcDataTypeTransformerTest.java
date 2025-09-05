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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link
 * org.apache.flink.cdc.connectors.starrocks.sink.StarRocksUtils.CdcDataTypeTransformer}.
 */
class CdcDataTypeTransformerTest {

    @Test
    void testCharType() {
        // map to char of StarRocks if CDC length <= StarRocksUtils.MAX_CHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_char").setOrdinalPosition(0);
        new CharType(1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        Assertions.assertThat(smallLengthColumn.getColumnName()).isEqualTo("small_char");
        Assertions.assertThat(smallLengthColumn.getOrdinalPosition()).isZero();
        Assertions.assertThat(smallLengthColumn.getDataType()).isEqualTo(StarRocksUtils.CHAR);
        Assertions.assertThat(smallLengthColumn.getColumnSize()).hasValue(3);
        Assertions.assertThat(smallLengthColumn.isNullable()).isTrue();

        // map to varchar of StarRocks if CDC length > StarRocksUtils.MAX_CHAR_SIZE
        StarRocksColumn.Builder largeLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("large_char").setOrdinalPosition(1);
        new CharType(StarRocksUtils.MAX_CHAR_SIZE)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, largeLengthBuilder));
        StarRocksColumn largeLengthColumn = largeLengthBuilder.build();
        Assertions.assertThat(largeLengthColumn.getColumnName()).isEqualTo("large_char");
        Assertions.assertThat(largeLengthColumn.getOrdinalPosition()).isOne();
        Assertions.assertThat(largeLengthColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(largeLengthColumn.getColumnSize())
                .hasValue(StarRocksUtils.MAX_CHAR_SIZE * 3);
        Assertions.assertThat(largeLengthColumn.isNullable()).isTrue();
    }

    @Test
    void testCharTypeForPrimaryKey() {
        // map to varchar of StarRocks if column is primary key
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(0);
        new CharType(1).accept(new StarRocksUtils.CdcDataTypeTransformer(true, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        Assertions.assertThat(smallLengthColumn.getColumnName()).isEqualTo("primary_key");
        Assertions.assertThat(smallLengthColumn.getOrdinalPosition()).isZero();
        Assertions.assertThat(smallLengthColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(smallLengthColumn.getColumnSize()).hasValue(3);
        Assertions.assertThat(smallLengthColumn.isNullable()).isTrue();
    }

    @Test
    void testDecimalForPrimaryKey() {
        // Map to DECIMAL of StarRocks if column is DECIMAL type and not primary key.
        StarRocksColumn.Builder noPrimaryKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("no_primary_key").setOrdinalPosition(0);
        new DecimalType(20, 1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, noPrimaryKeyBuilder));
        StarRocksColumn noPrimaryKeyColumn = noPrimaryKeyBuilder.build();
        Assertions.assertThat(noPrimaryKeyColumn.getColumnName()).isEqualTo("no_primary_key");
        Assertions.assertThat(noPrimaryKeyColumn.getOrdinalPosition()).isZero();
        Assertions.assertThat(noPrimaryKeyColumn.getDataType()).isEqualTo(StarRocksUtils.DECIMAL);
        Assertions.assertThat(noPrimaryKeyColumn.getColumnSize()).hasValue(20);
        Assertions.assertThat(noPrimaryKeyColumn.getDecimalDigits()).contains(1);
        Assertions.assertThat(noPrimaryKeyColumn.isNullable()).isTrue();

        // Map to VARCHAR of StarRocks if column is DECIMAL type and primary key.
        StarRocksColumn.Builder primaryKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(1);
        new DecimalType(20, 1)
                .notNull()
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, primaryKeyBuilder));
        StarRocksColumn primaryKeyColumn = primaryKeyBuilder.build();
        Assertions.assertThat(primaryKeyColumn.getColumnName()).isEqualTo("primary_key");
        Assertions.assertThat(primaryKeyColumn.getOrdinalPosition()).isOne();
        Assertions.assertThat(primaryKeyColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(primaryKeyColumn.getColumnSize()).hasValue(22);
        Assertions.assertThat(primaryKeyColumn.isNullable()).isFalse();

        // Map to VARCHAR of StarRocks if column is DECIMAL type and primary key
        // DECIMAL(20,0) is common in cdc pipeline, for example, the upstream cdc source is unsigned
        // BIGINT.
        StarRocksColumn.Builder unsignedBigIntKeyBuilder =
                new StarRocksColumn.Builder().setColumnName("primary_key").setOrdinalPosition(1);
        new DecimalType(20, 0)
                .notNull()
                .accept(new StarRocksUtils.CdcDataTypeTransformer(true, unsignedBigIntKeyBuilder));
        StarRocksColumn unsignedBigIntColumn = unsignedBigIntKeyBuilder.build();
        Assertions.assertThat(unsignedBigIntColumn.getColumnName()).isEqualTo("primary_key");
        Assertions.assertThat(unsignedBigIntColumn.getOrdinalPosition()).isOne();
        Assertions.assertThat(unsignedBigIntColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(unsignedBigIntColumn.getColumnSize()).hasValue(21);
        Assertions.assertThat(unsignedBigIntColumn.isNullable()).isFalse();
    }

    @Test
    void testVarCharType() {
        // the length fo StarRocks should be 3 times as that of CDC if CDC length * 3 <=
        // StarRocksUtils.MAX_VARCHAR_SIZE
        StarRocksColumn.Builder smallLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("small_varchar").setOrdinalPosition(0);
        new VarCharType(3)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, smallLengthBuilder));
        StarRocksColumn smallLengthColumn = smallLengthBuilder.build();
        Assertions.assertThat(smallLengthColumn.getColumnName()).isEqualTo("small_varchar");
        Assertions.assertThat(smallLengthColumn.getOrdinalPosition()).isZero();
        Assertions.assertThat(smallLengthColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(smallLengthColumn.getColumnSize()).hasValue(9);
        Assertions.assertThat(smallLengthColumn.isNullable()).isTrue();

        // the length fo StarRocks should be StarRocksUtils.MAX_VARCHAR_SIZE if CDC length * 3 >
        // StarRocksUtils.MAX_VARCHAR_SIZE
        StarRocksColumn.Builder largeLengthBuilder =
                new StarRocksColumn.Builder().setColumnName("large_varchar").setOrdinalPosition(1);
        new CharType(StarRocksUtils.MAX_VARCHAR_SIZE + 1)
                .accept(new StarRocksUtils.CdcDataTypeTransformer(false, largeLengthBuilder));
        StarRocksColumn largeLengthColumn = largeLengthBuilder.build();
        Assertions.assertThat(largeLengthColumn.getColumnName()).isEqualTo("large_varchar");
        Assertions.assertThat(largeLengthColumn.getOrdinalPosition()).isOne();
        Assertions.assertThat(largeLengthColumn.getDataType()).isEqualTo(StarRocksUtils.VARCHAR);
        Assertions.assertThat(largeLengthColumn.getColumnSize())
                .hasValue(StarRocksUtils.MAX_VARCHAR_SIZE);
        Assertions.assertThat(largeLengthColumn.isNullable()).isTrue();
    }
}
