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

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

/** A test for the {@link StringDataSerializer}. */
public class RecordDataSerializerTest extends SerializerTestBase<RecordData> {

    @Override
    protected RecordDataSerializer createSerializer() {
        return new RecordDataSerializer(DataTypes.BIGINT(), DataTypes.STRING());
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
        return new RecordData[] {
            GenericRecordData.of(1L, GenericStringData.fromString("test1")),
            GenericRecordData.of(2L, GenericStringData.fromString("test2"))
        };
    }
}
