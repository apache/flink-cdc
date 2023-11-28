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

package com.ververica.cdc.runtime.serializer.data;

import org.apache.flink.testutils.DeeplyEqualsChecker;

import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.GenericArrayData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

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
}
