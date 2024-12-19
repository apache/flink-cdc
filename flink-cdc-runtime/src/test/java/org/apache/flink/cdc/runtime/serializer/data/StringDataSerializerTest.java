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

import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;

/** A test for the {@link StringDataSerializer}. */
class StringDataSerializerTest extends SerializerTestBase<StringData> {

    @Override
    protected StringDataSerializer createSerializer() {
        return StringDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<StringData> getTypeClass() {
        return StringData.class;
    }

    @Override
    protected StringData[] getTestData() {
        return Arrays.stream(
                        new String[] {
                            "a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"
                        })
                .map(BinaryStringData::fromString)
                .toArray(StringData[]::new);
    }
}
