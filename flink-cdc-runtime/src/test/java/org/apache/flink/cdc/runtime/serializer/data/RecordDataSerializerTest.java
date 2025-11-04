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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

/** A test for the {@link StringDataSerializer}. */
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
            generator.generate(new Object[] {3L, null})
        };
    }
}
