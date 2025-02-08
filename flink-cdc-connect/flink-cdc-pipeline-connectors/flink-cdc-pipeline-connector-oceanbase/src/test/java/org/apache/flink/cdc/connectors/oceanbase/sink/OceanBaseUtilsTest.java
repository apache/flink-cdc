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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link OceanBaseUtils}. */
public class OceanBaseUtilsTest {

    @Test
    public void testToOceanBaseTable() {
        TableId tableId = TableId.parse("test.tbl1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col_id", new IntType(false))
                        .physicalColumn("col_bool", DataTypes.BOOLEAN())
                        .physicalColumn("col_bit_1", DataTypes.BOOLEAN())
                        .physicalColumn("col_bit_64", DataTypes.BINARY(8))
                        .physicalColumn("col_tinyint_1", DataTypes.BOOLEAN())
                        .physicalColumn("col_tinyint", DataTypes.TINYINT())
                        .physicalColumn("col_tinyint_unsigned", DataTypes.SMALLINT())
                        .physicalColumn("col_smallint", DataTypes.SMALLINT())
                        .physicalColumn("col_smallint_unsigned", DataTypes.INT())
                        .physicalColumn("col_int", DataTypes.INT())
                        .physicalColumn("col_int_unsigned", DataTypes.BIGINT())
                        .physicalColumn("col_bigint", DataTypes.BIGINT())
                        .physicalColumn("col_bigint_unsigned", DataTypes.DECIMAL(20, 0))
                        .physicalColumn("col_float", DataTypes.DOUBLE())
                        .physicalColumn("col_double", DataTypes.DOUBLE())
                        .physicalColumn("col_decimal_8", DataTypes.DECIMAL(8, 4))
                        .physicalColumn("col_decimal_38", DataTypes.DECIMAL(38, 0))
                        .physicalColumn("col_decimal_65", DataTypes.STRING())
                        .physicalColumn("col_time", DataTypes.TIME())
                        .physicalColumn("col_date", DataTypes.DATE())
                        .physicalColumn("col_datetime_3", DataTypes.TIMESTAMP(3))
                        .physicalColumn("col_datetime_6", DataTypes.TIMESTAMP(6))
                        .physicalColumn("col_timestamp_3", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn("col_timestamp_6", DataTypes.TIMESTAMP_LTZ(6))
                        .physicalColumn("col_char_3", DataTypes.CHAR(3))
                        .physicalColumn("col_varchar_255", DataTypes.VARCHAR(255))
                        .physicalColumn("col_text", DataTypes.STRING())
                        .physicalColumn("col_binary_1", DataTypes.BINARY(1))
                        .physicalColumn("col_binary_16", DataTypes.BINARY(16))
                        .physicalColumn("col_varbinary_17", DataTypes.VARBINARY(17))
                        .physicalColumn("col_blob", DataTypes.BYTES())
                        .primaryKey("col_id")
                        .build();

        OceanBaseTable table = OceanBaseUtils.toOceanBaseTable(tableId, schema);

        assertEquals(tableId.getSchemaName(), table.getDatabaseName());
        assertEquals(tableId.getTableName(), table.getTableName());
        assertTrue(table.getTableKeys().isPresent());
        assertEquals(OceanBaseTable.TableType.PRIMARY_KEY, table.getTableType());
        assertEquals(Collections.singletonList("col_id"), table.getTableKeys().get());

        verifyColumn(table.getColumn("col_id"), 0, false, OceanBaseUtils.INT, null, null);
        verifyColumn(table.getColumn("col_bool"), 1, true, OceanBaseUtils.BOOLEAN, null, null);
        verifyColumn(table.getColumn("col_bit_1"), 2, true, OceanBaseUtils.BOOLEAN, null, null);
        verifyColumn(table.getColumn("col_bit_64"), 3, true, OceanBaseUtils.BINARY, 8, null);
        verifyColumn(table.getColumn("col_tinyint_1"), 4, true, OceanBaseUtils.BOOLEAN, null, null);
        verifyColumn(table.getColumn("col_tinyint"), 5, true, OceanBaseUtils.TINYINT, null, null);
        verifyColumn(
                table.getColumn("col_tinyint_unsigned"),
                6,
                true,
                OceanBaseUtils.SMALLINT,
                null,
                null);
        verifyColumn(table.getColumn("col_smallint"), 7, true, OceanBaseUtils.SMALLINT, null, null);
        verifyColumn(
                table.getColumn("col_smallint_unsigned"), 8, true, OceanBaseUtils.INT, null, null);
        verifyColumn(table.getColumn("col_int"), 9, true, OceanBaseUtils.INT, null, null);
        verifyColumn(
                table.getColumn("col_int_unsigned"), 10, true, OceanBaseUtils.BIGINT, null, null);
        verifyColumn(table.getColumn("col_bigint"), 11, true, OceanBaseUtils.BIGINT, null, null);
        verifyColumn(
                table.getColumn("col_bigint_unsigned"), 12, true, OceanBaseUtils.DECIMAL, 20, 0);
        verifyColumn(table.getColumn("col_float"), 13, true, OceanBaseUtils.DOUBLE, null, null);
        verifyColumn(table.getColumn("col_double"), 14, true, OceanBaseUtils.DOUBLE, null, null);
        verifyColumn(table.getColumn("col_decimal_8"), 15, true, OceanBaseUtils.DECIMAL, 8, 4);
        verifyColumn(table.getColumn("col_decimal_38"), 16, true, OceanBaseUtils.DECIMAL, 38, 0);
        verifyColumn(
                table.getColumn("col_decimal_65"),
                17,
                true,
                OceanBaseUtils.LONGTEXT,
                Integer.MAX_VALUE,
                null);
        verifyColumn(table.getColumn("col_time"), 18, true, OceanBaseUtils.TIME, 0, null);
        verifyColumn(table.getColumn("col_date"), 19, true, OceanBaseUtils.DATE, null, null);
        verifyColumn(table.getColumn("col_datetime_3"), 20, true, OceanBaseUtils.DATETIME, 3, null);
        verifyColumn(table.getColumn("col_datetime_6"), 21, true, OceanBaseUtils.DATETIME, 6, null);
        verifyColumn(
                table.getColumn("col_timestamp_3"), 22, true, OceanBaseUtils.TIMESTAMP, 3, null);
        verifyColumn(
                table.getColumn("col_timestamp_6"), 23, true, OceanBaseUtils.TIMESTAMP, 6, null);
        verifyColumn(table.getColumn("col_char_3"), 24, true, OceanBaseUtils.CHAR, 3, null);
        verifyColumn(
                table.getColumn("col_varchar_255"), 25, true, OceanBaseUtils.VARCHAR, 255, null);
        verifyColumn(
                table.getColumn("col_text"),
                26,
                true,
                OceanBaseUtils.LONGTEXT,
                Integer.MAX_VALUE,
                null);
        verifyColumn(table.getColumn("col_binary_1"), 27, true, OceanBaseUtils.BINARY, 1, null);
        verifyColumn(table.getColumn("col_binary_16"), 28, true, OceanBaseUtils.BINARY, 16, null);
        verifyColumn(
                table.getColumn("col_varbinary_17"), 29, true, OceanBaseUtils.VARBINARY, 17, null);
        verifyColumn(
                table.getColumn("col_blob"),
                30,
                true,
                OceanBaseUtils.LONGBLOB,
                Integer.MAX_VALUE,
                null);
    }

    private void verifyColumn(
            OceanBaseColumn column,
            int position,
            boolean isNullable,
            String dataType,
            Integer columnSize,
            Integer numericScale) {
        assertEquals(errMessage(column, "position"), position, column.getOrdinalPosition());
        assertEquals(errMessage(column, "datatype"), dataType, column.getDataType());
        assertEquals(errMessage(column, "nullable"), isNullable, column.isNullable());
        if (columnSize == null) {
            assertFalse(column.getColumnSize().isPresent());
        } else {
            assertEquals(
                    errMessage(column, "columnSize"), columnSize, column.getColumnSize().get());
        }
        if (numericScale == null) {
            assertFalse(column.getNumericScale().isPresent());
        } else {
            assertEquals(
                    errMessage(column, "numericScale"),
                    numericScale,
                    column.getNumericScale().get());
        }
    }

    private String errMessage(OceanBaseColumn column, String field) {
        return String.format("Column %s verify failed for %s", column.getColumnName(), field);
    }
}
