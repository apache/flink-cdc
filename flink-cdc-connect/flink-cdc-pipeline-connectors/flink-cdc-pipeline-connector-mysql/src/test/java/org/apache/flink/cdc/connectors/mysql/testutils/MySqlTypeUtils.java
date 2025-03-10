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

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.JdbcRawDataType;
import org.apache.flink.cdc.common.types.RawDataType;
import org.apache.flink.cdc.common.types.RowType;

import java.sql.Types;

/** Test utilities for MySQL data types. */
public class MySqlTypeUtils {

    public static RowType getTestTableRowType(
            String tableName, boolean tinyint1IsBit, boolean isMysql8) {
        return (RowType) getTestTableSchema(tableName, tinyint1IsBit, isMysql8).toRowDataType();
    }

    public static Schema getTestTableSchema(
            String tableName, boolean tinyint1IsBit, boolean isMySql8) {
        // Display width specification for integer data types was deprecated in MySQL 8.0.17,
        // so the table schemas obtained through SHOW CREATE TABLE statement will be different in
        // MySQL 5.x and MySQL 8.x. https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-19.html
        switch (tableName) {
            case "common_types":
                return Schema.newBuilder()
                        .primaryKey("id")
                        .physicalColumn("id", typeBigintUnsigned(false, isMySql8))
                        .physicalColumn("tiny_c", typeTinyint(true, isMySql8))
                        .physicalColumn("tiny_un_c", typeTinyintUnsigned(true, isMySql8))
                        .physicalColumn("tiny_un_z_c", typeTinyintUnsignedZerofill(true))
                        .physicalColumn("small_c", typeSmallint(true, isMySql8))
                        .physicalColumn("small_un_c", typeSmallintUnsigned(true, isMySql8))
                        .physicalColumn("small_un_z_c", typeSmallintUnsignedZerofill(true))
                        .physicalColumn("medium_c", typeMediumint(true, isMySql8))
                        .physicalColumn("medium_un_c", typeMediumintUnsigned(true, isMySql8))
                        .physicalColumn("medium_un_z_c", typeMediumintUnsignedZerofill(true))
                        .physicalColumn("int_c", typeInt(true, isMySql8))
                        .physicalColumn("int_un_c", typeIntUnsigned(true, isMySql8))
                        .physicalColumn("int_un_z_c", typeIntUnsignedZerofill(true))
                        .physicalColumn("int11_c", typeInt(true, isMySql8))
                        .physicalColumn("big_c", typeBigint(true, isMySql8))
                        .physicalColumn("big_un_c", typeBigintUnsigned(true, isMySql8))
                        .physicalColumn("big_un_z_c", typeBigintUnsignedZerofill(true))
                        .physicalColumn("varchar_c", typeVarchar(true, 255))
                        .physicalColumn("char_c", typeChar(true, 3))
                        .physicalColumn("real_c", typeReal(true))
                        .physicalColumn("float_c", typeFloat(true))
                        .physicalColumn("float_un_c", typeFloatUnsigned(true))
                        .physicalColumn("float_un_z_c", typeFloatUnsignedZerofill(true))
                        .physicalColumn("double_c", typeDouble(true))
                        .physicalColumn("double_un_c", typeDoubleUnsigned(true))
                        .physicalColumn("double_un_z_c", typeDoubleUnsignedZerofill(true))
                        .physicalColumn("decimal_c", typeDecimal(true, 8, 4))
                        .physicalColumn("decimal_un_c", typeDecimalUnsigned(true, 8, 4))
                        .physicalColumn("decimal_un_z_c", typeDecimalUnsignedZerofill(true, 8, 4))
                        .physicalColumn("numeric_c", typeDecimal(true, 6, 0))
                        .physicalColumn("big_decimal_c", typeBigDecimal(true, 65, 1))
                        .physicalColumn("bit1_c", typeBit(true, 1))
                        .physicalColumn("bit3_c", typeBit(true, 3))
                        .physicalColumn("tiny1_c", typeTinyint1(true, tinyint1IsBit))
                        .physicalColumn("boolean_c", typeTinyint1(true, tinyint1IsBit))
                        .physicalColumn("file_uuid", typeBinary(true, 16))
                        .physicalColumn("bit_c", typeBit(true, 64))
                        .physicalColumn("text_c", typeText(true))
                        .physicalColumn("tiny_blob_c", typeTinyblob(true))
                        .physicalColumn("blob_c", typeBlob(true))
                        .physicalColumn("medium_blob_c", typeMediumBlob(true))
                        .physicalColumn("long_blob_c", typeLongBlob(true))
                        .physicalColumn("year_c", typeYear(true, isMySql8))
                        .physicalColumn("enum_c", typeEnum(true))
                        .physicalColumn("json_c", typeOther(true, "JSON"))
                        .physicalColumn("point_c", typeOther(true, "POINT"))
                        .physicalColumn("geometry_c", typeOther(true, "GEOMETRY"))
                        .physicalColumn("linestring_c", typeOther(true, "LINESTRING"))
                        .physicalColumn("polygon_c", typeOther(true, "POLYGON"))
                        .physicalColumn("multipoint_c", typeOther(true, "MULTIPOINT"))
                        .physicalColumn("multiline_c", typeOther(true, "MULTILINESTRING"))
                        .physicalColumn("multipolygon_c", typeOther(true, "MULTIPOLYGON"))
                        .physicalColumn(
                                "geometrycollection_c",
                                typeOther(true, isMySql8 ? "GEOMCOLLECTION" : "GEOMETRYCOLLECTION"))
                        .build();
            case "time_types":
                return isMySql8
                        ? Schema.newBuilder()
                                .primaryKey("id")
                                .physicalColumn("id", typeBigintUnsigned(false, true))
                                .physicalColumn("year_c", typeYear(true, true))
                                .physicalColumn("date_c", typeDate(true))
                                .physicalColumn("time_c", typeTime(true, null))
                                .physicalColumn("time_3_c", typeTime(true, 3))
                                .physicalColumn("time_6_c", typeTime(true, 6))
                                .physicalColumn("datetime_c", typeDatetime(true, null))
                                .physicalColumn("datetime3_c", typeDatetime(true, 3))
                                .physicalColumn("datetime6_c", typeDatetime(true, 6))
                                .physicalColumn("timestamp_c", typeTimestamp(true, null))
                                .physicalColumn("timestamp3_c", typeTimestamp(true, 3))
                                .physicalColumn("timestamp6_c", typeTimestamp(true, 6))
                                .physicalColumn("timestamp_def_c", typeTimestamp(true, null))
                                .build()
                        : Schema.newBuilder()
                                .primaryKey("id")
                                .physicalColumn("id", typeBigintUnsigned(false, false))
                                .physicalColumn("year_c", typeYear(true, false))
                                .physicalColumn("date_c", typeDate(true))
                                .physicalColumn("time_c", typeTime(true, null))
                                .physicalColumn("time_3_c", typeTime(true, 3))
                                .physicalColumn("time_6_c", typeTime(true, 6))
                                .physicalColumn("datetime_c", typeDatetime(true, null))
                                .physicalColumn("datetime3_c", typeDatetime(true, 3))
                                .physicalColumn("datetime6_c", typeDatetime(true, 6))
                                .physicalColumn("timestamp_c", typeTimestamp(true, null))
                                .physicalColumn("timestamp_def_c", typeTimestamp(true, null))
                                .build();
            case "precision_types":
                return Schema.newBuilder()
                        .primaryKey("id")
                        .physicalColumn("id", typeBigintUnsigned(false, isMySql8))
                        .physicalColumn("decimal_c0", typeDecimal(true, 6, 2))
                        .physicalColumn("decimal_c1", typeDecimal(true, 9, 4))
                        .physicalColumn("decimal_c2", typeDecimal(true, 20, 4))
                        .physicalColumn("time_c", typeTime(true, null))
                        .physicalColumn("time_3_c", typeTime(true, 3))
                        .physicalColumn("time_6_c", typeTime(true, 6))
                        .physicalColumn("datetime_c", typeDatetime(true, null))
                        .physicalColumn("datetime3_c", typeDatetime(true, 3))
                        .physicalColumn("datetime6_c", typeDatetime(true, 6))
                        .physicalColumn("timestamp_c", typeTimestamp(true, null))
                        .physicalColumn("timestamp3_c", typeTimestamp(true, 3))
                        .physicalColumn("timestamp6_c", typeTimestamp(true, 6))
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
            case "json_types":
                return Schema.newBuilder()
                        .primaryKey("id")
                        .physicalColumn("id", typeBigintUnsigned(false, isMySql8))
                        .physicalColumn("json_c0", typeOther(true, "JSON"))
                        .physicalColumn("json_c1", typeOther(true, "JSON"))
                        .physicalColumn("json_c2", typeOther(true, "JSON"))
                        .physicalColumn("int_c", typeInt(true, isMySql8))
                        .build();
            default:
                throw new IllegalArgumentException("Unknown table: " + tableName);
        }
    }

    public static DataType typeTinyint1(boolean nullable, boolean asBoolean) {
        DataType dataType = asBoolean ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
        return dataType.copy(nullable, new JdbcRawDataType(Types.SMALLINT, "TINYINT", 1, null));
    }

    public static DataType typeTinyint(boolean nullable, boolean isMysql8) {
        return DataTypes.TINYINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.SMALLINT, "TINYINT", isMysql8 ? -1 : 4, null));
    }

    public static DataType typeTinyintUnsigned(boolean nullable, boolean isMysql8) {
        return DataTypes.SMALLINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.SMALLINT, "TINYINT UNSIGNED", isMysql8 ? -1 : 3, null));
    }

    public static DataType typeTinyintUnsignedZerofill(boolean nullable) {
        return DataTypes.SMALLINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.SMALLINT, "TINYINT UNSIGNED ZEROFILL", 3, null));
    }

    public static DataType typeSmallint(boolean nullable, boolean isMysql8) {
        return DataTypes.SMALLINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.SMALLINT, "SMALLINT", isMysql8 ? -1 : 6, null));
    }

    public static DataType typeSmallintUnsigned(boolean nullable, boolean isMysql8) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.SMALLINT, "SMALLINT UNSIGNED", isMysql8 ? -1 : 5, null));
    }

    public static DataType typeSmallintUnsignedZerofill(boolean nullable) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.SMALLINT, "SMALLINT UNSIGNED ZEROFILL", 5, null));
    }

    public static DataType typeMediumint(boolean nullable, boolean isMysql8) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.INTEGER, "MEDIUMINT", isMysql8 ? -1 : 9, null));
    }

    public static DataType typeMediumintUnsigned(boolean nullable, boolean isMysql8) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.INTEGER, "MEDIUMINT UNSIGNED", isMysql8 ? -1 : 8, null));
    }

    public static DataType typeMediumintUnsignedZerofill(boolean nullable) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.INTEGER, "MEDIUMINT UNSIGNED ZEROFILL", 8, null));
    }

    public static DataType typeInt(boolean nullable, boolean isMysql8) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.INTEGER, "INT", isMysql8 ? -1 : 11, null));
    }

    public static DataType typeIntUnsigned(boolean nullable, boolean isMysql8) {
        return DataTypes.BIGINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.INTEGER, "INT UNSIGNED", isMysql8 ? -1 : 10, null));
    }

    public static DataType typeIntUnsignedZerofill(boolean nullable) {
        return DataTypes.BIGINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.INTEGER, "INT UNSIGNED ZEROFILL", 10, null));
    }

    public static DataType typeBigint(boolean nullable, boolean isMysql8) {
        return DataTypes.BIGINT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.BIGINT, "BIGINT", isMysql8 ? -1 : 20, null));
    }

    public static DataType typeBigintUnsigned(boolean nullable, boolean isMysql8) {
        return DataTypes.DECIMAL(20, 0)
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.BIGINT, "BIGINT UNSIGNED", isMysql8 ? -1 : 20, null));
    }

    public static DataType typeBigintUnsignedZerofill(boolean nullable) {
        return DataTypes.DECIMAL(20, 0)
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.BIGINT, "BIGINT UNSIGNED ZEROFILL", 20, null));
    }

    public static DataType typeVarchar(boolean nullable, int length) {
        return DataTypes.VARCHAR(length)
                .copy(nullable, new JdbcRawDataType(Types.VARCHAR, "VARCHAR", length, null));
    }

    public static DataType typeChar(boolean nullable, int length) {
        DataType dataType = length == 0 ? CharType.ofEmptyLiteral() : DataTypes.CHAR(length);
        return dataType.copy(nullable, new JdbcRawDataType(Types.CHAR, "CHAR", length, null));
    }

    public static DataType typeReal(boolean nullable) {
        return typeReal(nullable, null, null);
    }

    public static DataType typeReal(boolean nullable, Integer length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.DOUBLE, "DOUBLE", length == null ? -1 : length, scale));
    }

    public static DataType typeFloat(boolean nullable) {
        return typeFloat(nullable, null, null);
    }

    public static DataType typeFloat(boolean nullable, Integer length, Integer scale) {
        DataType type = length == null ? DataTypes.FLOAT() : DataTypes.DOUBLE();
        return type.copy(
                nullable,
                new JdbcRawDataType(Types.FLOAT, "FLOAT", length == null ? -1 : length, scale));
    }

    public static DataType typeFloatUnsigned(boolean nullable) {
        return typeFloatUnsigned(nullable, null, null);
    }

    public static DataType typeFloatUnsigned(boolean nullable, Integer length, Integer scale) {
        return DataTypes.FLOAT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.FLOAT,
                                "FLOAT UNSIGNED",
                                length == null ? -1 : length,
                                scale));
    }

    public static DataType typeFloatUnsignedZerofill(boolean nullable) {
        return typeFloatUnsignedZerofill(nullable, null, null);
    }

    public static DataType typeFloatUnsignedZerofill(
            boolean nullable, Integer length, Integer scale) {
        return DataTypes.FLOAT()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.FLOAT,
                                "FLOAT UNSIGNED ZEROFILL",
                                length == null ? -1 : length,
                                scale));
    }

    public static DataType typeDouble(boolean nullable) {
        return typeDouble(nullable, null, null);
    }

    public static DataType typeDouble(boolean nullable, Integer length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.DOUBLE, "DOUBLE", length == null ? -1 : length, scale));
    }

    public static DataType typeDoubleUnsigned(boolean nullable) {
        return typeDoubleUnsigned(nullable, null, null);
    }

    public static DataType typeDoubleUnsigned(boolean nullable, Integer length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.DOUBLE,
                                "DOUBLE UNSIGNED",
                                length == null ? -1 : length,
                                scale));
    }

    public static DataType typeDoubleUnsignedZerofill(boolean nullable) {
        return typeDoubleUnsignedZerofill(nullable, null, null);
    }

    public static DataType typeDoubleUnsignedZerofill(
            boolean nullable, Integer length, Integer scale) {
        return DataTypes.DOUBLE()
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.DOUBLE,
                                "DOUBLE UNSIGNED ZEROFILL",
                                length == null ? -1 : length,
                                scale));
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

    public static DataType typeDecimalUnsignedZerofill(boolean nullable, int length, int scale) {
        return DataTypes.DECIMAL(length, scale)
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.DECIMAL, "DECIMAL UNSIGNED ZEROFILL", length, scale));
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
        DataType dataType = length == 0 ? BinaryType.ofEmptyLiteral() : DataTypes.BINARY(length);
        return dataType.copy(nullable, new JdbcRawDataType(Types.BINARY, "BINARY", length, null));
    }

    public static DataType typeText(boolean nullable) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.VARCHAR, "TEXT", -1, null));
    }

    public static DataType typeTinyblob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(nullable, new JdbcRawDataType(Types.BLOB, "TINYBLOB", -1, null));
    }

    public static DataType typeBlob(boolean nullable) {
        return DataTypes.BYTES().copy(nullable, new JdbcRawDataType(Types.BLOB, "BLOB", -1, null));
    }

    public static DataType typeMediumBlob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(nullable, new JdbcRawDataType(Types.BLOB, "MEDIUMBLOB", -1, null));
    }

    public static DataType typeLongBlob(boolean nullable) {
        return DataTypes.BYTES()
                .copy(nullable, new JdbcRawDataType(Types.BLOB, "LONGBLOB", -1, null));
    }

    public static DataType typeYear(boolean nullable, boolean isMysql8) {
        return DataTypes.INT()
                .copy(
                        nullable,
                        new JdbcRawDataType(Types.INTEGER, "YEAR", isMysql8 ? -1 : 4, null));
    }

    public static DataType typeEnum(boolean nullable) {
        return DataTypes.STRING().copy(nullable, new JdbcRawDataType(Types.CHAR, "ENUM", 1, null));
    }

    public static DataType typeOther(boolean nullable, String typeName) {
        return DataTypes.STRING()
                .copy(nullable, new JdbcRawDataType(Types.OTHER, typeName, -1, null));
    }

    public static DataType typeDate(boolean nullable) {
        return DataTypes.DATE().copy(nullable, new JdbcRawDataType(Types.DATE, "DATE", -1, null));
    }

    public static DataType typeTime(boolean nullable, Integer length) {
        return DataTypes.TIME(length == null ? 0 : length)
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.TIME, "TIME", length == null ? -1 : length, null));
    }

    public static DataType typeDatetime(boolean nullable, Integer length) {
        return DataTypes.TIMESTAMP(length == null ? 0 : length)
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.TIMESTAMP, "DATETIME", length == null ? -1 : length, null));
    }

    public static DataType typeTimestamp(boolean nullable, Integer length) {
        return DataTypes.TIMESTAMP_LTZ(length == null ? 0 : length)
                .copy(
                        nullable,
                        new JdbcRawDataType(
                                Types.TIMESTAMP_WITH_TIMEZONE,
                                "TIMESTAMP",
                                length == null ? -1 : length,
                                null));
    }
}
