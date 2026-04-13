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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RecordDataSerializerTest extends SerializerTestBase<RecordData> {

    @Override
    protected RecordDataSerializer createSerializer() {
        return RecordDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<RecordData> getTypeClass() {
        return RecordData.class;
    }

    @Override
    protected RecordData[] getTestData() {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));
        return new RecordData[] {
            generator.generate(new Object[] {1L, BinaryStringData.fromString("test1")}),
            generator.generate(new Object[] {2L, BinaryStringData.fromString("test2")}),
            generator.generate(new Object[] {3L, null}),
            GenericRecordData.of(1L, BinaryStringData.fromString("test1")),
            GenericRecordData.of(2L, BinaryStringData.fromString("test2")),
            GenericRecordData.of(3L, null)
        };
    }

    @Test
    void testGenericRecordDataWithVariousTypes() throws Exception {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        GenericRecordData record =
                GenericRecordData.of(
                        true,
                        (byte) 42,
                        (short) 1024,
                        123456,
                        789L,
                        3.14f,
                        2.718281828,
                        BinaryStringData.fromString("hello"),
                        new byte[] {1, 2, 3},
                        DecimalData.fromBigDecimal(new BigDecimal("12345.6789"), 10, 4),
                        TimestampData.fromMillis(1609459200000L, 123456),
                        LocalZonedTimestampData.fromEpochMillis(1609459200000L, 654321),
                        ZonedTimestampData.of(1609459200000L, 789012, "UTC"),
                        DateData.fromEpochDay(18628),
                        TimeData.fromMillisOfDay(43200000),
                        null);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(record, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RecordData deserialized = serializer.deserialize(in);

        assertThat(deserialized).isInstanceOf(GenericRecordData.class);
        assertThat(deserialized.getArity()).isEqualTo(record.getArity());
        assertThat(deserialized.getBoolean(0)).isTrue();
        assertThat(deserialized.getByte(1)).isEqualTo((byte) 42);
        assertThat(deserialized.getShort(2)).isEqualTo((short) 1024);
        assertThat(deserialized.getInt(3)).isEqualTo(123456);
        assertThat(deserialized.getLong(4)).isEqualTo(789L);
        assertThat(deserialized.getFloat(5)).isEqualTo(3.14f);
        assertThat(deserialized.getDouble(6)).isEqualTo(2.718281828);
        assertThat(deserialized.getString(7).toString()).isEqualTo("hello");
        assertThat(deserialized.getBinary(8)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(deserialized.getDecimal(9, 10, 4).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345.6789"));
        assertThat(deserialized.getTimestamp(10, 6).getMillisecond()).isEqualTo(1609459200000L);
        assertThat(deserialized.getTimestamp(10, 6).getNanoOfMillisecond()).isEqualTo(123456);
        assertThat(deserialized.getLocalZonedTimestampData(11, 6).getEpochMillisecond())
                .isEqualTo(1609459200000L);
        assertThat(deserialized.getZonedTimestamp(12, 6).getMillisecond())
                .isEqualTo(1609459200000L);
        assertThat(deserialized.getZonedTimestamp(12, 6).getNanoOfMillisecond()).isEqualTo(789012);
        assertThat(deserialized.getZonedTimestamp(12, 6).getZoneId()).isEqualTo("UTC");
        assertThat(deserialized.getDate(13).toEpochDay()).isEqualTo(18628);
        assertThat(deserialized.getTime(14).toMillisOfDay()).isEqualTo(43200000);
        assertThat(deserialized.isNullAt(15)).isTrue();
    }

    @Test
    void testBinaryRecordDataWithVariousTypes() throws Exception {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        RowType rowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.STRING(),
                        DataTypes.BYTES(),
                        DataTypes.DECIMAL(10, 4),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.TIMESTAMP_TZ(6),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.STRING());

        BinaryRecordData record =
                new BinaryRecordDataGenerator(rowType)
                        .generate(
                                new Object[] {
                                    true,
                                    (byte) 42,
                                    (short) 1024,
                                    123456,
                                    789L,
                                    3.14f,
                                    2.718281828,
                                    BinaryStringData.fromString("hello"),
                                    new byte[] {1, 2, 3},
                                    DecimalData.fromBigDecimal(new BigDecimal("12345.6789"), 10, 4),
                                    TimestampData.fromMillis(1609459200000L, 123456),
                                    LocalZonedTimestampData.fromEpochMillis(1609459200000L, 654321),
                                    ZonedTimestampData.of(1609459200000L, 789012, "UTC"),
                                    DateData.fromEpochDay(18628),
                                    TimeData.fromMillisOfDay(43200000),
                                    null
                                });

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(record, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RecordData deserialized = serializer.deserialize(in);

        assertThat(deserialized).isInstanceOf(BinaryRecordData.class);
        assertThat(deserialized.getArity()).isEqualTo(record.getArity());
        assertThat(deserialized.getBoolean(0)).isTrue();
        assertThat(deserialized.getByte(1)).isEqualTo((byte) 42);
        assertThat(deserialized.getShort(2)).isEqualTo((short) 1024);
        assertThat(deserialized.getInt(3)).isEqualTo(123456);
        assertThat(deserialized.getLong(4)).isEqualTo(789L);
        assertThat(deserialized.getFloat(5)).isEqualTo(3.14f);
        assertThat(deserialized.getDouble(6)).isEqualTo(2.718281828);
        assertThat(deserialized.getString(7).toString()).isEqualTo("hello");
        assertThat(deserialized.getBinary(8)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(deserialized.getDecimal(9, 10, 4).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345.6789"));
        assertThat(deserialized.getTimestamp(10, 6).getMillisecond()).isEqualTo(1609459200000L);
        assertThat(deserialized.getTimestamp(10, 6).getNanoOfMillisecond()).isEqualTo(123456);
        assertThat(deserialized.getLocalZonedTimestampData(11, 6).getEpochMillisecond())
                .isEqualTo(1609459200000L);
        assertThat(deserialized.getZonedTimestamp(12, 6).getMillisecond())
                .isEqualTo(1609459200000L);
        assertThat(deserialized.getDate(13).toEpochDay()).isEqualTo(18628);
        assertThat(deserialized.getTime(14).toMillisOfDay()).isEqualTo(43200000);
        assertThat(deserialized.isNullAt(15)).isTrue();
    }

    @Test
    void testGenericRecordDataWithNestedTypes() throws Exception {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        GenericRecordData nestedGeneric =
                GenericRecordData.of(42, BinaryStringData.fromString("nested"));

        BinaryRecordData nestedBinary =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.STRING()))
                        .generate(new Object[] {99, BinaryStringData.fromString("binary-nested")});

        GenericArrayData intArray = new GenericArrayData(new int[] {1, 2, 3, 4, 5});
        GenericArrayData stringArray =
                new GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("a"), BinaryStringData.fromString("b")
                        });
        GenericMapData map =
                new GenericMapData(
                        Map.of(
                                BinaryStringData.fromString("k1"),
                                100,
                                BinaryStringData.fromString("k2"),
                                200));

        GenericRecordData record =
                GenericRecordData.of(nestedGeneric, nestedBinary, intArray, stringArray, map);

        DataOutputSerializer out = new DataOutputSerializer(512);
        serializer.serialize(record, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RecordData deserialized = serializer.deserialize(in);

        assertThat(deserialized).isInstanceOf(GenericRecordData.class);
        assertThat(deserialized.getArity()).isEqualTo(5);

        RecordData dNestedGeneric = deserialized.getRow(0, 2);
        assertThat(dNestedGeneric.getInt(0)).isEqualTo(42);
        assertThat(dNestedGeneric.getString(1).toString()).isEqualTo("nested");

        RecordData dNestedBinary = deserialized.getRow(1, 2);
        assertThat(dNestedBinary.getInt(0)).isEqualTo(99);
        assertThat(dNestedBinary.getString(1).toString()).isEqualTo("binary-nested");

        ArrayData dIntArray = deserialized.getArray(2);
        assertThat(dIntArray.size()).isEqualTo(5);
        assertThat(dIntArray.getInt(0)).isEqualTo(1);
        assertThat(dIntArray.getInt(4)).isEqualTo(5);

        ArrayData dStringArray = deserialized.getArray(3);
        assertThat(dStringArray.size()).isEqualTo(2);
        assertThat(dStringArray.getString(0).toString()).isEqualTo("a");
        assertThat(dStringArray.getString(1).toString()).isEqualTo("b");

        MapData dMap = deserialized.getMap(4);
        assertThat(dMap.size()).isEqualTo(2);
    }

    @Test
    void testBinaryRecordDataWithNestedTypes() throws Exception {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        RowType rowType =
                RowType.of(
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(
                                DataTypes.FIELD("f1", DataTypes.INT()),
                                DataTypes.FIELD("f2", DataTypes.STRING())));

        BinaryRecordData record =
                new BinaryRecordDataGenerator(rowType)
                        .generate(
                                new Object[] {
                                    new GenericArrayData(
                                            new Object[] {
                                                BinaryStringData.fromString("x"),
                                                BinaryStringData.fromString("y"),
                                                BinaryStringData.fromString("z")
                                            }),
                                    new GenericMapData(
                                            Map.of(
                                                    BinaryStringData.fromString("p"),
                                                    10,
                                                    BinaryStringData.fromString("q"),
                                                    20)),
                                    new BinaryRecordDataGenerator(
                                                    RowType.of(DataTypes.INT(), DataTypes.STRING()))
                                            .generate(
                                                    new Object[] {
                                                        77, BinaryStringData.fromString("inner")
                                                    })
                                });

        DataOutputSerializer out = new DataOutputSerializer(512);
        serializer.serialize(record, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RecordData deserialized = serializer.deserialize(in);

        assertThat(deserialized).isInstanceOf(BinaryRecordData.class);

        ArrayData array = deserialized.getArray(0);
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getString(0).toString()).isEqualTo("x");
        assertThat(array.getString(2).toString()).isEqualTo("z");

        MapData map = deserialized.getMap(1);
        assertThat(map.size()).isEqualTo(2);

        RecordData nested = deserialized.getRow(2, 2);
        assertThat(nested.getInt(0)).isEqualTo(77);
        assertThat(nested.getString(1).toString()).isEqualTo("inner");
    }

    @Test
    void testGenericRecordDataCopy() {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        GenericRecordData record =
                GenericRecordData.of(
                        42, BinaryStringData.fromString("copy-test"), new byte[] {9, 8, 7});

        RecordData copied = serializer.copy(record);

        assertThat(copied).isInstanceOf(GenericRecordData.class).isNotSameAs(record);
        assertThat(copied.getInt(0)).isEqualTo(42);
        assertThat(copied.getString(1).toString()).isEqualTo("copy-test");
        assertThat(copied.getBinary(2)).isEqualTo(new byte[] {9, 8, 7});
        assertThat(copied.getBinary(2)).isNotSameAs(record.getBinary(2));
    }

    @Test
    void testDecimalDataPreservesPrecisionAndScale() throws Exception {
        RecordDataSerializer serializer = RecordDataSerializer.INSTANCE;

        DecimalData decimal1 = DecimalData.fromBigDecimal(new BigDecimal("1.23"), 20, 4);
        DecimalData decimal2 = DecimalData.fromBigDecimal(new BigDecimal("42"), 15, 0);
        DecimalData decimal3 = DecimalData.fromBigDecimal(new BigDecimal("0.0010"), 10, 4);

        GenericRecordData record = GenericRecordData.of(decimal1, decimal2, decimal3);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(record, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RecordData deserialized = serializer.deserialize(in);

        assertThat(deserialized).isInstanceOf(GenericRecordData.class);

        DecimalData dDecimal1 = deserialized.getDecimal(0, 20, 4);
        assertThat(dDecimal1.precision()).isEqualTo(20);
        assertThat(dDecimal1.scale()).isEqualTo(4);
        assertThat(dDecimal1.toBigDecimal()).isEqualByComparingTo(new BigDecimal("1.23"));

        DecimalData dDecimal2 = deserialized.getDecimal(1, 15, 0);
        assertThat(dDecimal2.precision()).isEqualTo(15);
        assertThat(dDecimal2.scale()).isEqualTo(0);
        assertThat(dDecimal2.toBigDecimal()).isEqualByComparingTo(new BigDecimal("42"));

        DecimalData dDecimal3 = deserialized.getDecimal(2, 10, 4);
        assertThat(dDecimal3.precision()).isEqualTo(10);
        assertThat(dDecimal3.scale()).isEqualTo(4);
        assertThat(dDecimal3.toBigDecimal()).isEqualByComparingTo(new BigDecimal("0.0010"));

        assertThat(dDecimal2.isCompact()).isTrue();
    }
}
