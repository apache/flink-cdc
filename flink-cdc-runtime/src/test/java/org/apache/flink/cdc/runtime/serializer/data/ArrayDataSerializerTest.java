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
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.binary.BinaryArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryArrayWriter;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link ArrayDataSerializer}. */
class ArrayDataSerializerTest extends SerializerTestBase<ArrayData> {

    public ArrayDataSerializerTest() {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof ArrayData && o2 instanceof ArrayData,
                                (o1, o2, checker) -> {
                                    ArrayData array1 = (ArrayData) o1;
                                    ArrayData array2 = (ArrayData) o2;
                                    if (array1.size() != array2.size()) {
                                        return false;
                                    }
                                    for (int i = 0; i < array1.size(); i++) {
                                        if (!array1.isNullAt(i) || !array2.isNullAt(i)) {
                                            if (array1.isNullAt(i) || array2.isNullAt(i)) {
                                                return false;
                                            } else {
                                                if (!array1.getString(i)
                                                        .equals(array2.getString(i))) {
                                                    return false;
                                                }
                                            }
                                        }
                                    }
                                    return true;
                                }));
    }

    @Override
    protected ArrayDataSerializer createSerializer() {
        return new ArrayDataSerializer(DataTypes.STRING());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<ArrayData> getTypeClass() {
        return ArrayData.class;
    }

    @Override
    protected ArrayData[] getTestData() {
        return new ArrayData[] {
            new GenericArrayData(
                    new StringData[] {
                        BinaryStringData.fromString("11"), null, BinaryStringData.fromString("ke")
                    })
        };
    }

    static BinaryArrayData createArray(String... vs) {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 8);
        for (int i = 0; i < vs.length; i++) {
            writer.writeString(i, BinaryStringData.fromString(vs[i]));
        }
        writer.complete();
        return array;
    }

    @Test
    void testToBinaryArrayWithNestedTypes() {
        // Create a nested ArrayData
        Map<BinaryStringData, BinaryStringData> map = new HashMap<>();
        map.put(BinaryStringData.fromString("key1"), BinaryStringData.fromString("value1"));
        map.put(BinaryStringData.fromString("key2"), BinaryStringData.fromString("value2"));
        GenericMapData genMapData = new GenericMapData(map);

        ArrayData innerArrayData = new GenericArrayData(new Object[] {genMapData});

        // Serialize to BinaryArrayData
        ArrayDataSerializer serializer =
                new ArrayDataSerializer(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));
        BinaryArrayData binaryArrayData = serializer.toBinaryArray(innerArrayData);

        // Verify the conversion
        MapData mapData = binaryArrayData.getMap(0);
        Assertions.assertThat(mapData.size()).isEqualTo(2);
    }

    @Test
    void testToBinaryArrayWithDeeplyNestedTypes() {
        // Create a nested structure: MapData containing ArrayData elements
        Map<BinaryStringData, ArrayData> nestedMap = new HashMap<>();
        nestedMap.put(
                BinaryStringData.fromString("key1"), new GenericArrayData(new Object[] {42, 43}));
        nestedMap.put(
                BinaryStringData.fromString("key2"), new GenericArrayData(new Object[] {44, 45}));

        GenericMapData genMapData = new GenericMapData(nestedMap);

        // Create an outer ArrayData containing the nested MapData
        ArrayData outerArrayData = new GenericArrayData(new Object[] {genMapData});

        // Serialize to BinaryArrayData
        ArrayDataSerializer serializer =
                new ArrayDataSerializer(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())));
        BinaryArrayData binaryArrayData = serializer.toBinaryArray(outerArrayData);

        // Verify the conversion
        MapData mapData = binaryArrayData.getMap(0);
        assertThat(mapData.size()).isEqualTo(2);

        // Check nested arrays in map
        ArrayData keys = mapData.keyArray();
        ArrayData values = mapData.valueArray();

        // Check the first key-value pair
        int keyIndex = keys.getString(0).toString().equals("key1") ? 0 : 1;
        ArrayData arrayData1 = values.getArray(keyIndex);
        assertThat(arrayData1.getInt(0)).isEqualTo(42);
        assertThat(arrayData1.getInt(1)).isEqualTo(43);

        // Check the second key-value pair
        keyIndex = keys.getString(0).toString().equals("key2") ? 0 : 1;
        ArrayData arrayData2 = values.getArray(keyIndex);
        assertThat(arrayData2.getInt(0)).isEqualTo(44);
        assertThat(arrayData2.getInt(1)).isEqualTo(45);
    }
}
