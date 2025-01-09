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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryArrayData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.InstantiationUtil;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryArrayWriter;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryWriter;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/** Serializer for {@link ArrayData}. */
public class ArrayDataSerializer extends TypeSerializer<ArrayData> {

    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final TypeSerializer<Object> eleSer;
    private final ArrayData.ElementGetter elementGetter;
    private transient BinaryArrayData reuseArray;
    private transient BinaryArrayWriter reuseWriter;

    public ArrayDataSerializer(DataType eleType) {
        this(eleType, InternalSerializers.create(eleType));
    }

    private ArrayDataSerializer(DataType eleType, TypeSerializer<Object> eleSer) {
        this.eleType = eleType;
        this.eleSer = new NullableSerializerWrapper<>(eleSer);
        this.elementGetter = ArrayData.createElementGetter(eleType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ArrayData> duplicate() {
        return new ArrayDataSerializer(eleType, eleSer.duplicate());
    }

    @Override
    public ArrayData createInstance() {
        return new GenericArrayData(new Object[0]);
    }

    @Override
    public ArrayData copy(ArrayData from) {
        return copyGenericArray((GenericArrayData) from);
    }

    @Override
    public ArrayData copy(ArrayData from, ArrayData reuse) {
        return copy(from);
    }

    private GenericArrayData copyGenericArray(GenericArrayData array) {
        if (array.isPrimitiveArray()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArrayData(
                            Arrays.copyOf(array.toBooleanArray(), array.size()));
                case TINYINT:
                    return new GenericArrayData(Arrays.copyOf(array.toByteArray(), array.size()));
                case SMALLINT:
                    return new GenericArrayData(Arrays.copyOf(array.toShortArray(), array.size()));
                case INTEGER:
                    return new GenericArrayData(Arrays.copyOf(array.toIntArray(), array.size()));
                case BIGINT:
                    return new GenericArrayData(Arrays.copyOf(array.toLongArray(), array.size()));
                case FLOAT:
                    return new GenericArrayData(Arrays.copyOf(array.toFloatArray(), array.size()));
                case DOUBLE:
                    return new GenericArrayData(Arrays.copyOf(array.toDoubleArray(), array.size()));
                default:
                    throw new RuntimeException("Unknown type: " + eleType);
            }
        } else {
            Object[] objectArray = array.toObjectArray();
            Object[] newArray =
                    (Object[])
                            Array.newInstance(
                                    DataTypeUtils.toInternalConversionClass(eleType), array.size());
            for (int i = 0; i < array.size(); i++) {
                if (objectArray[i] != null) {
                    newArray[i] = eleSer.copy(objectArray[i]);
                }
            }
            return new GenericArrayData(newArray);
        }
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ArrayData record, DataOutputView target) throws IOException {
        target.writeInt(record.size());
        for (int i = 0; i < record.size(); i++) {
            eleSer.serialize(elementGetter.getElementOrNull(record, i), target);
        }
    }

    public BinaryArrayData toBinaryArray(ArrayData from) {
        if (from instanceof BinaryArrayData) {
            return (BinaryArrayData) from;
        }

        int numElements = from.size();
        if (reuseArray == null) {
            reuseArray = new BinaryArrayData();
        }
        if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
            reuseWriter =
                    new BinaryArrayWriter(
                            reuseArray,
                            numElements,
                            BinaryArrayData.calculateFixLengthPartSize(eleType));
        } else {
            reuseWriter.reset();
        }

        for (int i = 0; i < numElements; i++) {
            if (from.isNullAt(i)) {
                reuseWriter.setNullAt(i, eleType);
            } else {
                BinaryWriter.write(
                        reuseWriter, i, elementGetter.getElementOrNull(from, i), eleType, eleSer);
            }
        }
        reuseWriter.complete();

        return reuseArray;
    }

    @Override
    public ArrayData deserialize(DataInputView source) throws IOException {
        int size = source.readInt();
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = eleSer.deserialize(source);
        }
        return new GenericArrayData(array);
    }

    @Override
    public ArrayData deserialize(ArrayData reuse, DataInputView source) throws IOException {
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

        ArrayDataSerializer that = (ArrayDataSerializer) o;

        return eleType.equals(that.eleType);
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }

    @VisibleForTesting
    public TypeSerializer getEleSer() {
        return eleSer;
    }

    @Override
    public TypeSerializerSnapshot<ArrayData> snapshotConfiguration() {
        return new ArrayDataSerializerSnapshot(eleType, eleSer);
    }

    /** {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}. */
    public static final class ArrayDataSerializerSnapshot
            implements TypeSerializerSnapshot<ArrayData> {
        private static final int CURRENT_VERSION = 3;

        private DataType previousType;
        private TypeSerializer previousEleSer;

        @SuppressWarnings("unused")
        public ArrayDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        ArrayDataSerializerSnapshot(DataType eleType, TypeSerializer eleSer) {
            this.previousType = eleType;
            this.previousEleSer = eleSer;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, previousType);
            InstantiationUtil.serializeObject(outStream, previousEleSer);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            try {
                DataInputViewStream inStream = new DataInputViewStream(in);
                this.previousType =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.previousEleSer =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<ArrayData> restoreSerializer() {
            return new ArrayDataSerializer(previousType, previousEleSer);
        }

        @Override
        public TypeSerializerSchemaCompatibility<ArrayData> resolveSchemaCompatibility(
                TypeSerializer<ArrayData> newSerializer) {
            if (!(newSerializer instanceof ArrayDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ArrayDataSerializer newArrayDataSerializer = (ArrayDataSerializer) newSerializer;
            if (!previousType.equals(newArrayDataSerializer.eleType)
                    || !previousEleSer.equals(newArrayDataSerializer.eleSer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
