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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Serializer for {@link DateData}. */
public final class DateDataSerializer extends TypeSerializerSingleton<DateData> {

    private static final long serialVersionUID = 1L;

    public static final DateDataSerializer INSTANCE = new DateDataSerializer();

    private DateDataSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public DateData createInstance() {
        return DateData.fromEpochDay(0);
    }

    @Override
    public DateData copy(DateData from) {
        return DateData.fromEpochDay(from.toEpochDay());
    }

    @Override
    public DateData copy(DateData from, DateData reuse) {
        return DateData.fromEpochDay(from.toEpochDay());
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(DateData record, DataOutputView target) throws IOException {
        target.writeInt(record.toEpochDay());
    }

    @Override
    public DateData deserialize(DataInputView source) throws IOException {
        return DateData.fromEpochDay(source.readInt());
    }

    @Override
    public DateData deserialize(DateData record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<DateData> snapshotConfiguration() {
        return new DateDataSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DateDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DateData> {

        public DateDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
