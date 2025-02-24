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

package org.apache.flink.cdc.connectors.mysql.testutils;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.JdbcRawDataType;
import org.apache.flink.cdc.common.types.RawDataType;
import org.apache.flink.cdc.common.types.RowType;

import java.sql.Types;
import java.util.stream.Collectors;

/** Test utilities for MySQL data types. */
public class MySqlTypeUtils {

    private static final Schema COMMON_TYPES_SCHEMA_TINY1_AS_BIT =
            Schema.newBuilder()
                    .primaryKey("id")
                    .physicalColumn("id", typeBigintUnsigned(false))
                    .physicalColumn("tiny_c", typeTinyint(true))
                    .physicalColumn("tiny_un_c", typeTinyintUnsigned(true))
                    .physicalColumn("tiny_un_z_c", typeTinyintUnsigned(true))
                    .physicalColumn("small_c", typeSmallint(true))
                    .physicalColumn("small_un_c", typeSmallintUnsigned(true))
                    .physicalColumn("small_un_z_c", typeSmallintUnsigned(true))
                    .physicalColumn("medium_c", typeMediumint(true))
                    .physicalColumn("medium_un_c", typeMediumintUnsigned(true))
                    .physicalColumn("medium_un_z_c", typeMediumintUnsigned(true))
                    .physicalColumn("int_c", typeInt(true))
                    .physicalColumn("int_un_c", typeIntUnsigned(true))
                    .physicalColumn("int_un_z_c", typeIntUnsigned(true))
                    .physicalColumn("int11_c", typeInt(true))
                    .physicalColumn("big_c", typeBigint(true))
                    .physicalColumn("big_un_c", typeBigintUnsigned(true))
                    .physicalColumn("big_un_z_c", typeBigintUnsigned(true))
                    .physicalColumn("varchar_c", typeVarchar(true, 255))
                    .physicalColumn("char_c", typeChar(true, 3))
                    .physicalColumn("real_c", typeReal(true, 22, 0))
                    .physicalColumn("float_c", typeFloat(true))
                    .physicalColumn("float_un_c", typeFloatUnsigned(true))
                    .physicalColumn("float_un_z_c", typeFloatUnsigned(true))
                    .physicalColumn("double_c", typeDouble(true))
                    .physicalColumn("double_un_c", typeDoubleUnsigned(true))
                    .physicalColumn("double_un_z_c", typeDoubleUnsigned(true))
                    .physicalColumn("decimal_c", typeDecimal(true, 8, 4))
                    .physicalColumn("decimal_un_c", typeDecimalUnsigned(true, 8, 4))
                    .physicalColumn("decimal_un_z_c", typeDecimalUnsigned(true, 8, 4))
                    .physicalColumn("numeric_c", typeDecimal(true, 6, 0))
                    .physicalColumn("big_decimal_c", typeBigDecimal(true, 65, 1))
                    .physicalColumn("bit1_c", typeBit(true, 1))
                    .physicalColumn("bit3_c", typeBit(true, 3))
                    .physicalColumn("tiny1_c", typeBit(true, 1))
                    .physicalColumn("boolean_c", typeBit(true, 1))
                    .physicalColumn("file_uuid", typeBinary(true, 16))
                    .physicalColumn("bit_c", typeBit(true, 64))
                    .physicalColumn("text_c", typeText(true))
                    .physicalColumn("tiny_blob_c", typeTinyblob(true))
                    .physicalColumn("blob_c", typeBlob(true))
                    .physicalColumn("medium_blob_c", typeMediumBlob(true))
                    .physicalColumn("long_blob_c", typeLongBlob(true))
                    .physicalColumn("year_c", typeYear(true))
                    .physicalColumn("enum_c", typeEnum(true, 5))
                    .physicalColumn("json_c", typeJson(true))
                    .physicalColumn("point_c", typeGeometry(true))
                    .physicalColumn("geometry_c", typeGeometry(true))
                    .physicalColumn("linestring_c", typeGeometry(true))
                    .physicalColumn("polygon_c", typeGeometry(true))
                    .physicalColumn("multipoint_c", typeGeometry(true))
                    .physicalColumn("multiline_c", typeGeometry(true))
                    .physicalColumn("multipolygon_c", typeGeometry(true))
                    .physicalColumn("geometrycollection_c", typeGeometry(true))
                    .build();

    private static final Schema COMMON_TYPES_SCHEMA_TINY1_AS_TINY =
            Schema.newBuilder()
                    .primaryKey("id")
                    .setColumns(
                            COMMON_TYPES_SCHEMA_TINY1_AS_BIT.getColumns().stream()
                                    .map(
                                            column ->
                                                    "tiny1_c".equals(column.getName())
                                                            ? Column.physicalColumn(
                                                                    column.getName(),
                                                                    typeTinyint(true))
                                                            : column)
                                    .collect(Collectors.toList()))
                    .build();

    private static final Schema TIME_TYPES_SCHEMA_MYSQL57 =
            Schema.newBuilder()
                    .primaryKey("id")
                    .physicalColumn("id", typeBigintUnsigned(false))
                    .physicalColumn("year_c", typeYear(true))
                    .physicalColumn("date_c", typeDate(true))
                    .physicalColumn("time_c", typeTime(true, 8))
                    .physicalColumn("time_3_c", typeTime(true, 12))
                    .physicalColumn("time_6_c", typeTime(true, 15))
                    .physicalColumn("datetime_c", typeDatetime(true, 19))
                    .physicalColumn("datetime3_c", typeDatetime(true, 23))
                    .physicalColumn("datetime6_c", typeDatetime(true, 26))
                    .physicalColumn("timestamp_c", typeTimestamp(true, 19))
                    .physicalColumn("timestamp_def_c", typeTimestamp(true, 19))
                    .build();

    private static final Schema TIME_TYPES_TABLE_SCHEMA_MYSQL8 =
            Schema.newBuilder()
                    .primaryKey("id")
                    .physicalColumn("id", typeBigintUnsigned(false))
                    .physicalColumn("year_c", typeYear(true))
                    .physicalColumn("date_c", typeDate(true))
                    .physicalColumn("time_c", typeTime(true, 8))
                    .physicalColumn("time_3_c", typeTime(true, 12))
                    .physicalColumn("time_6_c", typeTime(true, 15))
                    .physicalColumn("datetime_c", typeDatetime(true, 19))
                    .physicalColumn("datetime3_c", typeDatetime(true, 23))
                    .physicalColumn("datetime6_c", typeDatetime(true, 26))
                    .physicalColumn("timestamp_c", typeTimestamp(true, 19))
                    .physicalColumn("timestamp3_c", typeTimestamp(true, 23))
                    .physicalColumn("timestamp6_c", typeTimestamp(true, 26))
                    .physicalColumn("timestamp_def_c", typeTimestamp(true, 19))
                    .build();

    private static final Schema PRECISION_TYPES_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", typeBigintUnsigned(false))
                    .physicalColumn("decimal_c0", typeDecimal(true, 6, 2))
                    .physicalColumn("decimal_c1", typeDecimal(true, 9, 4))
                    .physicalColumn("decimal_c2", typeDecimal(true, 20, 4))
                    .physicalColumn("time_c", typeTime(true, 8))
                    .physicalColumn("time_3_c", typeTime(true, 12))
                    .physicalColumn("time_6_c", typeTime(true, 15))
                    .physicalColumn("datetime_c", typeDatetime(true, 19))
                    .physicalColumn("datetime3_c", typeDatetime(true, 23))
                    .physicalColumn("datetime6_c", typeDatetime(true, 26))
                    .physicalColumn("timestamp_c", typeTimestamp(true, 19))
                    .physicalColumn("timestamp3_c", typeTimestamp(true, 23))
                    .physicalColumn("timestamp6_c", typeTimestamp(true, 26))
                    .physicalColumn("float_c0", typeFloat(true, 6, 0))
                    .physicalColumn("float_c1", typeFloat(true, 20, 3))
                    .physicalColumn("float_c2", typeFloat(true, 24, 12))
                    .physicalColumn("real_c0", typeReal(true, 6, 0))
                    .physicalColumn("real_c1", typeReal(true, 20, 3))
                    .physicalColumn("real_c2", typeReal(true, 24, 12))
                    .physicalColumn("double_c0", typeDouble(true, 6, 0))
                    .physicalColumn("double_c1", typeDouble(true, 20, 3))
                    .physicalColumn("double_c2", typeDouble(true, 24, 12))
                    .physicalColumn("double_precision_c0", typeDouble(true, 6, 0))
                    .physicalColumn("double_precision_c1", typeDouble(true, 20, 3))
                    .physicalColumn("double_precision_c2", typeDouble(true, 24, 12))
                    .build();

    private static final Schema JSON_TYPES_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", typeBigintUnsigned(false))
                    .physicalColumn("json_c0", typeJson(true))
                    .physicalColumn("json_c1", typeJson(true))
                    .physicalColumn("json_c2", typeJson(true))
                    .physicalColumn("int_c", typeInt(true))
                    .build();

    public static RowType getTestTableRowType(
            String tableName, boolean tinyint1IsBit, boolean isMySql57) {
        return (RowType) getTestTableSchema(tableName, tinyint1IsBit, isMySql57).toRowDataType();
    }

    public static Schema getTestTableSchema(
            String tableName, boolean tinyint1IsBit, boolean isMySql57) {
        switch (tableName) {
            case "common_types":
                return tinyint1IsBit
                        ? COMMON_TYPES_SCHEMA_TINY1_AS_BIT
                        : COMMON_TYPES_SCHEMA_TINY1_AS_TINY;
            case "time_types":
                return isMySql57 ? TIME_TYPES_SCHEMA_MYSQL57 : TIME_TYPES_TABLE_SCHEMA_MYSQL8;
            case "precision_types":
                return PRECISION_TYPES_SCHEMA;
            case "json_types":
                return JSON_TYPES_SCHEMA;
            default:
                throw new IllegalArgumentException("Unknown table: " + tableName);
        }
    }

    public static DataType typeTinyint(boolean nullable) {
        return DataTypes.TINYINT()
                .copy(nullable, new JdbcRawDataType(Types.TINYINT, "TINYINT", 3, null));
    }

    public static DataType typeTinyintUnsigned(boolean nullable) {
        return DataTypes.SMALLINT()
                .copy(nullable, new JdbcRawDataType(Types.TINYINT, "TINYINT UNSIGNED", 3, null));
    }

    public static DataType typeSmallint(boolean nullable) {
        return DataTypes.SMALLINT()
                .copy(nullable, new JdbcRawDataType(Types.SMALLINT, "SMALLINT", 5, null));
    }

    public static DataType typeSmallintUnsigned(boolean nullable) {
        return DataTypes.INT()
                .copy(nullable, new JdbcRawDataType(Types.SMALLINT, "SMALLINT UNSIGNED", 5, null));
    }

    public static DataType typeMediumint(boolean nullable) {
        return DataTypes.INT()
                .copy(nullable, new JdbcRawDataType(Types.INTEGER, "MEDIUMINT", 7, null));
    }

    public static DataType typeMediumintUnsigned(boolean nullable) {
        return DataTypes.INT()
                .copy(nullable, new JdbcRawDataType(Types.INTEGER, "MEDIUMINT UNSIGNED", 8, null));
    }

    public static DataType typeInt(boolean nullable) {
        return DataTypes.INT().copy(nullable, new JdbcRawDataType(Types.INTEGER, "INT", 10, null));
    }

    public static DataType typeIntUnsigned(boolean nullable) {
        return DataTypes.BIGINT()
                .copy(nullable, new JdbcRawDataType(Types.INTEGER, "INT UNSIGNED", 10, null));
    }

    public static DataType typeBigint(boolean nullable) {
        return DataTypes.BIGINT()
                .copy(nullable, new JdbcRawDataType(Types.BIGINT, "BIGINT", 19, null));
    }

    public static DataType typeBigintUnsigned(boolean nullable) {
        return DataTypes.DECIMAL(20, 0)
                .copy(nullable, new JdbcRawDataType(Types.BIGINT, "BIGINT UNSIGNED", 20, null));
    }

    public static DataType typeVarchar(boolean nullable, int length) {
        return DataTypes.VARCHAR(length)
                .copy(nullable, new JdbcRawDataType(Types.VARCHAR, "VARCHAR", length, null));
    }

    public static DataType typeChar(boolean nullable, int length) {
        return DataTypes.CHAR(length)
                .copy(nullable, new JdbcRawDataType(Types.CHAR, "VARCHAR", length, null));
    }

    public static DataType typeReal(boolean nullable) {
        return typeReal(nullable, 22, 0);
    }

    public static DataType typeReal(boolean nullable, int length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(nullable, new JdbcRawDataType(Types.DOUBLE, "DOUBLE", length, scale));
    }

    public static DataType typeFloat(boolean nullable) {
        return typeFloat(nullable, 12, 0);
    }

    public static DataType typeFloat(boolean nullable, int length, Integer scale) {
        DataType dataType = length != -1 ? DataTypes.DOUBLE() : DataTypes.FLOAT();
        return dataType.copy(nullable, new JdbcRawDataType(Types.REAL, "FLOAT", length, scale));
    }

    public static DataType typeFloatUnsigned(boolean nullable) {
        return DataTypes.DOUBLE()
                .copy(nullable, new JdbcRawDataType(Types.REAL, "FLOAT UNSIGNED", 12, 0));
    }

    public static DataType typeDouble(boolean nullable) {
        return typeDouble(nullable, 22, 0);
    }

    public static DataType typeDouble(boolean nullable, int length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(nullable, new JdbcRawDataType(Types.DOUBLE, "DOUBLE", length, scale));
    }

    public static DataType typeDoubleUnsigned(boolean nullable) {
        return DataTypes.DOUBLE()
                .copy(nullable, new JdbcRawDataType(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 0));
    }

    public static DataType typeDecimal(boolean nullable, int length, int scale) {
        return DataTypes.DECIMAL(length, scale)
                .copy(nullable, new JdbcRawDataType(Types.DECIMAL, "DECIMAL", length, scale));
    }

    public static DataType typeDecimalUnsigned(boolean nullable, int length, int scale) {
        return DataTypes.DECIMAL(length, scale)
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.DECIMAL, "DECIMAL UNSIGNED", length, scale));
    }

    public static DataType typeBigDecimal(boolean nullable, int length, int scale) {
        assert length > 38;
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.DECIMAL, "DECIMAL", length, scale));
    }

    public static DataType typeBit(boolean nullable, int length) {
        RawDataType rawDataType = new JdbcRawDataType(Types.BIT, "BIT", length, null);
        if (length == 1) {
            return DataTypes.BOOLEAN().copy(nullable, rawDataType);
        }
        return DataTypes.BINARY((length + 7) / 8).copy(nullable, rawDataType);
    }

    public static DataType typeBinary(boolean nullable, int length) {
        return DataTypes.BINARY(length)
                .copy(nullable, new JdbcRawDataType(Types.BINARY, "BINARY", length, null));
    }

    public static DataType typeText(boolean nullable) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.LONGVARCHAR, "TEXT", 65535, null));
    }

    public static DataType typeTinyblob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(nullable, new JdbcRawDataType(Types.VARBINARY, "TINYBLOB", 255, null));
    }

    public static DataType typeBlob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(nullable, new JdbcRawDataType(Types.LONGVARBINARY, "BLOB", 65535, null));
    }

    public static DataType typeMediumBlob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.LONGVARBINARY, "MEDIUMBLOB", 16777215, null));
    }

    public static DataType typeLongBlob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.LONGVARBINARY, "LONGBLOB", 2147483647, null));
    }

    public static DataType typeYear(boolean nullable) {
        return DataTypes.INT().copy(nullable, new JdbcRawDataType(Types.DATE, "YEAR", 4, null));
    }

    public static DataType typeEnum(boolean nullable, int length) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.CHAR, "ENUM", length, null));
    }

    public static DataType typeJson(boolean nullable) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.LONGVARCHAR, "JSON", 1073741824, null));
    }

    public static DataType typeGeometry(boolean nullable) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.BINARY, "GEOMETRY", 65535, null));
    }

    public static DataType typeDate(boolean nullable) {
        return DataTypes.DATE().copy(nullable, new JdbcRawDataType(Types.DATE, "DATE", 10, null));
    }

    public static DataType typeTime(boolean nullable, int length) {
        return DataTypes.TIME(length <= 8 ? 0 : length - 9)
                .copy(nullable, new JdbcRawDataType(Types.TIME, "TIME", length, null));
    }

    public static DataType typeDatetime(boolean nullable, int length) {
        return DataTypes.TIMESTAMP(length <= 19 ? 0 : length - 20)
                .copy(nullable, new JdbcRawDataType(Types.TIMESTAMP, "DATETIME", length, null));
    }

    public static DataType typeTimestamp(boolean nullable, int length) {
        return DataTypes.TIMESTAMP_LTZ(length <= 19 ? 0 : length - 20)
                .copy(nullable, new JdbcRawDataType(Types.TIMESTAMP, "TIMESTAMP", length, null));
    }
}
