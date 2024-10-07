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

package org.apache.flink.cdc.runtime.serializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.types.utils.runtime.DataInputViewStream;
import org.apache.flink.cdc.common.types.utils.runtime.DataOutputViewStream;
import org.apache.flink.cdc.common.utils.InstantiationUtil;
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
        return new NullableSerializerWrapperSnapshot<>(innerSerializer);
    }

    public TypeSerializer<T> getWrappedSerializer() {
        return innerSerializer;
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class NullableSerializerWrapperSnapshot<T>
            implements TypeSerializerSnapshot<T> {

        private static final int CURRENT_VERSION = 1;

        private TypeSerializer<T> innerSerializer;

        @SuppressWarnings("unused")
        public NullableSerializerWrapperSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        public NullableSerializerWrapperSnapshot(TypeSerializer<T> innerSerializer) {
            this.innerSerializer = innerSerializer;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, innerSerializer);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
                throws IOException {
            try {
                DataInputViewStream inStream = new DataInputViewStream(in);
                this.innerSerializer = InstantiationUtil.deserializeObject(inStream, classLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new NullableSerializerWrapper<>(innerSerializer);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof NullableSerializerWrapper)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            NullableSerializerWrapper<T> newNullableSerializerWrapper =
                    (NullableSerializerWrapper<T>) newSerializer;
            if (!innerSerializer.equals(newNullableSerializerWrapper.innerSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
