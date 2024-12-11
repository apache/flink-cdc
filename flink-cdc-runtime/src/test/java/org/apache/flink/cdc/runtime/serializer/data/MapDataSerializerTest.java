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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.binary.BinaryArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryMapData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryArrayWriter;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.cdc.runtime.serializer.data.util.MapDataUtil.convertToJavaMap;

/** A test for the {@link MapDataSerializer}. */
public class MapDataSerializerTest extends SerializerTestBase<MapData> {
    private static final DataType INT = DataTypes.INT();
    private static final DataType STRING = DataTypes.STRING();

    public MapDataSerializerTest() {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof MapData && o2 instanceof MapData,
                                (o1, o2, checker) ->
                                        // Better is more proper to compare the maps after changing
                                        // them to Java maps
                                        // instead of binary maps. For example, consider the
                                        // following two maps:
                                        // {1: 'a', 2: 'b', 3: 'c'} and {3: 'c', 2: 'b', 1: 'a'}
                                        // These are actually the same maps, but their key / value
                                        // order will be
                                        // different when stored as binary maps, and the equalsTo
                                        // method of binary
                                        // map will return false.
                                        convertToJavaMap((MapData) o1, INT, STRING)
                                                .equals(
                                                        convertToJavaMap(
                                                                (MapData) o2, INT, STRING))));
    }

    /** @return MapDataSerializer */
    @Override
    protected TypeSerializer<MapData> createSerializer() {
        return new MapDataSerializer(INT, STRING);
    }

    /**
     * Gets the expected length for the serializer's {@link TypeSerializer#getLength()} method.
     *
     * <p>The expected length should be positive, for fix-length data types, or {@code -1} for
     * variable-length types.
     */
    @Override
    protected int getLength() {
        return -1;
    }

    /** @return MapData clazz */
    @Override
    protected Class<MapData> getTypeClass() {
        return MapData.class;
    }

    /** @return MapData[] */
    @Override
    protected MapData[] getTestData() {
        Map<Object, Object> first = new HashMap<>();
        first.put(1, BinaryStringData.fromString(""));
        return new MapData[] {
            new GenericMapData(first),
            new CustomMapData(first),
            BinaryMapData.valueOf(
                    createArray(1, 2), ArrayDataSerializerTest.createArray("11", "haa")),
            BinaryMapData.valueOf(
                    createArray(1, 3, 4), ArrayDataSerializerTest.createArray("11", "haa", "ke")),
            BinaryMapData.valueOf(
                    createArray(1, 4, 2), ArrayDataSerializerTest.createArray("11", "haa", "ke")),
            BinaryMapData.valueOf(
                    createArray(1, 5, 6, 7),
                    ArrayDataSerializerTest.createArray("11", "lele", "haa", "ke"))
        };
    }

    private static BinaryArrayData createArray(int... vs) {
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 4);
        for (int i = 0; i < vs.length; i++) {
            writer.writeInt(i, vs[i]);
        }
        writer.complete();
        return array;
    }

    /** A simple custom implementation for {@link MapData}. */
    public static class CustomMapData implements MapData {

        private final Map<?, ?> map;

        public CustomMapData(Map<?, ?> map) {
            this.map = map;
        }

        public Object get(Object key) {
            return map.get(key);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public ArrayData keyArray() {
            Object[] keys = map.keySet().toArray();
            return new GenericArrayData(keys);
        }

        @Override
        public ArrayData valueArray() {
            Object[] values = map.values().toArray();
            return new GenericArrayData(values);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof CustomMapData)) {
                return false;
            }
            return map.equals(((CustomMapData) o).map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(map);
        }
    }
}
