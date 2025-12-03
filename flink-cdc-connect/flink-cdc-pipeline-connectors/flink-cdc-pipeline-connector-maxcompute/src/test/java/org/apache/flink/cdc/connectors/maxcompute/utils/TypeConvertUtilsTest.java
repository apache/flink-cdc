/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** test for TypeConvertUtils. */
class TypeConvertUtilsTest {
    static Schema allTypeSchema =
            Schema.newBuilder()
                    .physicalColumn("char(5)", DataTypes.CHAR(5))
                    .physicalColumn("varchar(10)", DataTypes.VARCHAR(10))
                    .physicalColumn("string", DataTypes.STRING())
                    .physicalColumn("boolean", DataTypes.BOOLEAN())
                    .physicalColumn("binary(5)", DataTypes.BINARY(5))
                    .physicalColumn("varbinary(10)", DataTypes.BINARY(10))
                    .physicalColumn("decimal(10, 2)", DataTypes.DECIMAL(10, 2))
                    .physicalColumn("tinyint", DataTypes.TINYINT())
                    .physicalColumn("smallint", DataTypes.SMALLINT())
                    .physicalColumn("int", DataTypes.INT())
                    .physicalColumn("bigint", DataTypes.BIGINT())
                    .physicalColumn("float", DataTypes.FLOAT())
                    .physicalColumn("double", DataTypes.DOUBLE())
                    .physicalColumn("time", DataTypes.TIME())
                    .physicalColumn("date", DataTypes.DATE())
                    .physicalColumn("timestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                    .physicalColumn("timestamp_tz", DataTypes.TIMESTAMP_TZ())
                    .physicalColumn(
                            "array<array<int>>", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                    .physicalColumn(
                            "map<map<int, int>, int>",
                            DataTypes.MAP(
                                    DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                                    DataTypes.INT()))
                    .physicalColumn(
                            "row<map<int, int>, int, int>",
                            DataTypes.ROW(
                                    DataTypes.FIELD(
                                            "f0", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                                    DataTypes.FIELD("f1", DataTypes.INT()),
                                    DataTypes.FIELD("f2", DataTypes.INT())))
                    .build();

    @Test
    void schemaConvertTest() {
        TableSchema maxComputeSchema = TypeConvertUtils.toMaxCompute(allTypeSchema);

        TableSchema expectSchema = new TableSchema();
        expectSchema.addColumn(new Column("char(5)", TypeInfoFactory.STRING));
        expectSchema.addColumn(new Column("varchar(10)", TypeInfoFactory.STRING));
        expectSchema.addColumn(new Column("string", TypeInfoFactory.STRING));
        expectSchema.addColumn(new Column("boolean", TypeInfoFactory.BOOLEAN));
        expectSchema.addColumn(new Column("binary(5)", TypeInfoFactory.BINARY));
        expectSchema.addColumn(new Column("varbinary(10)", TypeInfoFactory.BINARY));
        expectSchema.addColumn(
                new Column("decimal(10, 2)", TypeInfoFactory.getDecimalTypeInfo(10, 2)));
        expectSchema.addColumn(new Column("tinyint", TypeInfoFactory.TINYINT));
        expectSchema.addColumn(new Column("smallint", TypeInfoFactory.SMALLINT));
        expectSchema.addColumn(new Column("int", TypeInfoFactory.INT));
        expectSchema.addColumn(new Column("bigint", TypeInfoFactory.BIGINT));
        expectSchema.addColumn(new Column("float", TypeInfoFactory.FLOAT));
        expectSchema.addColumn(new Column("double", TypeInfoFactory.DOUBLE));
        expectSchema.addColumn(new Column("time", TypeInfoFactory.STRING));
        expectSchema.addColumn(new Column("date", TypeInfoFactory.DATE));
        expectSchema.addColumn(new Column("timestamp", TypeInfoFactory.TIMESTAMP_NTZ));
        expectSchema.addColumn(new Column("timestamp_ltz", TypeInfoFactory.TIMESTAMP));
        expectSchema.addColumn(new Column("timestamp_tz", TypeInfoFactory.TIMESTAMP));
        expectSchema.addColumn(
                new Column(
                        "array<array<int>>",
                        TypeInfoFactory.getArrayTypeInfo(
                                TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT))));
        expectSchema.addColumn(
                new Column(
                        "map<map<int, int>, int>",
                        TypeInfoFactory.getMapTypeInfo(
                                TypeInfoFactory.getMapTypeInfo(
                                        TypeInfoFactory.INT, TypeInfoFactory.INT),
                                TypeInfoFactory.INT)));
        expectSchema.addColumn(
                new Column(
                        "row<map<int, int>, int, int>",
                        TypeInfoFactory.getStructTypeInfo(
                                ImmutableList.of("f0", "f1", "f2"),
                                ImmutableList.of(
                                        TypeInfoFactory.getMapTypeInfo(
                                                TypeInfoFactory.INT, TypeInfoFactory.INT),
                                        TypeInfoFactory.INT,
                                        TypeInfoFactory.INT))));

        List<Column> expect = expectSchema.getAllColumns();
        List<Column> current = maxComputeSchema.getAllColumns();

        for (int i = 0; i < expect.size(); i++) {
            assertThat(current.get(i).getTypeInfo().getTypeName())
                    .isEqualTo(expect.get(i).getTypeInfo().getTypeName());
            assertThat(current.get(i).getName()).isEqualTo(expect.get(i).getName());
        }
    }

    @Test
    void testRecordConvert() {
        Schema schemaWithoutComplexType =
                allTypeSchema.copy(
                        allTypeSchema.getColumns().stream()
                                .limit(18)
                                .collect(ImmutableList.toImmutableList()));
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator((RowType) schemaWithoutComplexType.toRowDataType());
        BinaryRecordData record1 =
                dataGenerator.generate(
                        new Object[] {
                            BinaryStringData.fromString("char"),
                            BinaryStringData.fromString("varchar"),
                            BinaryStringData.fromString("string"),
                            false,
                            new byte[] {1, 2, 3, 4, 5},
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                            DecimalData.zero(10, 2),
                            (byte) 1,
                            (short) 2,
                            12345,
                            12345L,
                            123.456f,
                            123456.789d,
                            TimeData.fromMillisOfDay(1234567),
                            DateData.fromEpochDay(12345),
                            TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-01 00:00:00")),
                            LocalZonedTimestampData.fromInstant(Instant.ofEpochSecond(0)),
                            ZonedTimestampData.fromZonedDateTime(
                                    ZonedDateTime.ofInstant(
                                            Instant.ofEpochSecond(0), ZoneId.of("Asia/Shanghai"))),
                        });

        ArrayRecord arrayRecord =
                new ArrayRecord(TypeConvertUtils.toMaxCompute(schemaWithoutComplexType));
        TypeConvertUtils.toMaxComputeRecord(schemaWithoutComplexType, record1, arrayRecord);

        String expect =
                "char,varchar,string,false,=01=02=03=04=05,=01=02=03=04=05=06=07=08=09=0A,0.00,1,2,12345,12345,123.456,123456.789,00:20:34.567,2003-10-20,1970-01-01T00:00,1970-01-01T00:00:00Z,1970-01-01T00:00:00Z";
        assertThat(arrayRecord).hasToString(expect);
    }
}
