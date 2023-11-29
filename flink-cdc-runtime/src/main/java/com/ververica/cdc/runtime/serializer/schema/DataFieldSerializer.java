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

package com.ververica.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.StringSerializer;
import com.ververica.cdc.runtime.serializer.TypeSerializerSingleton;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link DataField}. */
public class DataFieldSerializer extends TypeSerializerSingleton<DataField> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DataFieldSerializer INSTANCE = new DataFieldSerializer();

    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;
    private final DataTypeSerializer dataTypeSerializer = new DataTypeSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public DataField createInstance() {
        return new DataField("unknown", DataTypes.BIGINT());
    }

    @Override
    public DataField copy(DataField from) {
        return new DataField(
                stringSerializer.copy(from.getName()),
                dataTypeSerializer.copy(from.getType()),
                stringSerializer.copy(from.getDescription()));
    }

    @Override
    public DataField copy(DataField from, DataField reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DataField record, DataOutputView target) throws IOException {
        stringSerializer.serialize(record.getName(), target);
        dataTypeSerializer.serialize(record.getType(), target);
        stringSerializer.serialize(record.getDescription(), target);
    }

    @Override
    public DataField deserialize(DataInputView source) throws IOException {
        String name = stringSerializer.deserialize(source);
        DataType type = dataTypeSerializer.deserialize(source);
        String desc = stringSerializer.deserialize(source);
        return new DataField(name, type, desc);
    }

    @Override
    public DataField deserialize(DataField reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<DataField> snapshotConfiguration() {
        return new DataFieldSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DataFieldSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DataField> {

        public DataFieldSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
