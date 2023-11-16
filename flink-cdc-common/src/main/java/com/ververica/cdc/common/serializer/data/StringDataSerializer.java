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

package com.ververica.cdc.common.serializer.data;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.utils.StringUtf8Utils;

import java.io.IOException;

/** Serializer for {@link StringData}. */
public final class StringDataSerializer extends TypeSerializerSingleton<StringData> {

    private static final long serialVersionUID = 1L;

    public static final StringDataSerializer INSTANCE = new StringDataSerializer();

    private StringDataSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public StringData createInstance() {
        return GenericStringData.fromString("");
    }

    @Override
    public StringData copy(StringData from) {
        // GenericStringData is the only implementation of StringData
        return ((GenericStringData) from).copy();
    }

    @Override
    public StringData copy(StringData from, StringData reuse) {
        // GenericStringData is the only implementation of StringData
        return ((GenericStringData) from).copy();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(StringData record, DataOutputView target) throws IOException {
        // GenericStringData is the only implementation of StringData
        GenericStringData string = (GenericStringData) record;
        if (string.toBytes() == null) {
            target.writeInt(0);
        } else {
            byte[] bytes = string.toBytes();
            target.writeInt(bytes.length);
            target.write(bytes);
        }
    }

    @Override
    public StringData deserialize(DataInputView source) throws IOException {
        return deserializeInternal(source);
    }

    public static StringData deserializeInternal(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        // GenericStringData is the only implementation of StringData
        return GenericStringData.fromString(StringUtf8Utils.decodeUTF8(bytes, 0, length));
    }

    @Override
    public StringData deserialize(StringData record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<StringData> snapshotConfiguration() {
        return new StringDataSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class StringDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<StringData> {

        public StringDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
