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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.serializer.InternalSerializers;
import com.ververica.cdc.common.serializer.NestedSerializersSnapshotDelegate;
import com.ververica.cdc.common.serializer.NullableSerializerWrapper;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

/** Serializer for {@link RecordData}. */
public class RecordDataSerializer extends TypeSerializer<RecordData> {

    private static final long serialVersionUID = 1L;

    private final DataType[] types;
    private final TypeSerializer[] fieldSerializers;
    private final RecordData.FieldGetter[] fieldGetters;

    public RecordDataSerializer(RowType rowType) {
        this(
                rowType.getChildren().toArray(new DataType[0]),
                rowType.getChildren().stream()
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public RecordDataSerializer(DataType... types) {
        this(
                types,
                Arrays.stream(types)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public RecordDataSerializer(DataType[] types, TypeSerializer<?>[] fieldSerializers) {
        this.types = types;
        this.fieldSerializers =
                Arrays.stream(fieldSerializers)
                        .map(NullableSerializerWrapper::new)
                        .toArray(TypeSerializer[]::new);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> RecordData.createFieldGetter(types[i], i))
                        .toArray(RecordData.FieldGetter[]::new);
    }

    @Override
    public TypeSerializer<RecordData> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new RecordDataSerializer(types, duplicateFieldSerializers);
    }

    @Override
    public RecordData createInstance() {
        // default use binary row to deserializer
        return new GenericRecordData(types.length);
    }

    @Override
    public void serialize(RecordData recordData, DataOutputView target) throws IOException {
        target.writeInt(types.length);
        for (int i = 0; i < types.length; i++) {
            fieldSerializers[i].serialize(fieldGetters[i].getFieldOrNull(recordData), target);
        }
    }

    @Override
    public RecordData deserialize(DataInputView source) throws IOException {
        Object[] fields = new Object[source.readInt()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fieldSerializers[i].deserialize(source);
        }
        return GenericRecordData.of(fields);
    }

    @Override
    public RecordData deserialize(RecordData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public RecordData copy(RecordData from) {
        if (from.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: " + from.getArity() + ", but serializer arity: " + types.length);
        }
        return copyRecordData(from, new GenericRecordData(from.getArity()));
    }

    @Override
    public RecordData copy(RecordData from, RecordData reuse) {
        if (from.getArity() != types.length || reuse.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getArity()
                            + ", reuse Row arity: "
                            + reuse.getArity()
                            + ", but serializer arity: "
                            + types.length);
        }
        return copyRecordData(from, reuse);
    }

    @SuppressWarnings("unchecked")
    private RecordData copyRecordData(RecordData from, RecordData reuse) {
        GenericRecordData ret;
        if (reuse instanceof GenericRecordData) {
            ret = (GenericRecordData) reuse;
        } else {
            ret = new GenericRecordData(from.getArity());
        }
        for (int i = 0; i < from.getArity(); i++) {
            if (!from.isNullAt(i)) {
                ret.setField(i, fieldSerializers[i].copy(fieldGetters[i].getFieldOrNull(from)));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    public int getArity() {
        return types.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RecordDataSerializer) {
            RecordDataSerializer other = (RecordDataSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<RecordData> snapshotConfiguration() {
        return new RecordDataSerializerSnapshot(types, fieldSerializers);
    }

    /** {@link TypeSerializerSnapshot} for {@link RecordDataSerializer}. */
    public static final class RecordDataSerializerSnapshot
            implements TypeSerializerSnapshot<RecordData> {
        private static final int CURRENT_VERSION = 1;

        private DataType[] previousTypes;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        @SuppressWarnings("unused")
        public RecordDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        RecordDataSerializerSnapshot(DataType[] types, TypeSerializer[] serializers) {
            this.previousTypes = types;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousTypes.length);
            DataOutputViewStream stream = new DataOutputViewStream(out);
            for (DataType previousType : previousTypes) {
                InstantiationUtil.serializeObject(stream, previousType);
            }
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int length = in.readInt();
            DataInputViewStream stream = new DataInputViewStream(in);
            previousTypes = new DataType[length];
            for (int i = 0; i < length; i++) {
                try {
                    previousTypes[i] =
                            InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
        }

        @Override
        public RecordDataSerializer restoreSerializer() {
            return new RecordDataSerializer(
                    previousTypes,
                    nestedSerializersSnapshotDelegate.getRestoredNestedSerializers());
        }

        @Override
        public TypeSerializerSchemaCompatibility<RecordData> resolveSchemaCompatibility(
                TypeSerializer<RecordData> newSerializer) {
            if (!(newSerializer instanceof RecordDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            RecordDataSerializer newRowSerializer = (RecordDataSerializer) newSerializer;
            if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RecordData>
                    intermediateResult =
                            CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                                    newRowSerializer.fieldSerializers,
                                    nestedSerializersSnapshotDelegate
                                            .getNestedSerializerSnapshots());

            if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                RecordDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        reconfiguredCompositeSerializer);
            }

            return intermediateResult.getFinalResult();
        }
    }
}
