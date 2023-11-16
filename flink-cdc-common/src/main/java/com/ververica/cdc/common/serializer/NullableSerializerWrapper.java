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

package com.ververica.cdc.common.serializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Type serializer for nullable object. */
public class NullableSerializerWrapper<T> extends TypeSerializer<T> {
    private final TypeSerializer<T> innerSerializer;

    public NullableSerializerWrapper(TypeSerializer<T> innerSerializer) {
        this.innerSerializer = innerSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return innerSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new NullableSerializerWrapper<>(innerSerializer.duplicate());
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T from) {
        if (from == null) {
            return null;
        }
        return innerSerializer.copy(from);
    }

    @Override
    public T copy(T from, T reuse) {
        if (from == null) {
            return null;
        }
        return innerSerializer.copy(from, reuse);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            innerSerializer.serialize(record, target);
        }
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        if (source.readBoolean()) {
            return null;
        }
        return innerSerializer.deserialize(source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NullableSerializerWrapper that = (NullableSerializerWrapper) o;
        return innerSerializer.equals(that.innerSerializer);
    }

    @Override
    public int hashCode() {
        return innerSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return innerSerializer.snapshotConfiguration();
    }
}
