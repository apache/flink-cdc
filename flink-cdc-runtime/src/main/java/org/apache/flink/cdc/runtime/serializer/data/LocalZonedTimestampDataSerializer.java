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
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Serializer for {@link LocalZonedTimestampData}.
 *
 * <p>A {@link LocalZonedTimestampData} instance can be compactly serialized as a long value(=
 * millisecond) when the Timestamp type is compact. Otherwise it's serialized as a long value and a
 * int value.
 */
public class LocalZonedTimestampDataSerializer extends TypeSerializer<LocalZonedTimestampData> {

    private static final long serialVersionUID = 1L;

    private final int precision;

    public LocalZonedTimestampDataSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<LocalZonedTimestampData> duplicate() {
        return new LocalZonedTimestampDataSerializer(precision);
    }

    @Override
    public LocalZonedTimestampData createInstance() {
        return LocalZonedTimestampData.fromEpochMillis(0, 0);
    }

    @Override
    public LocalZonedTimestampData copy(LocalZonedTimestampData from) {
        return LocalZonedTimestampData.fromEpochMillis(
                from.getEpochMillisecond(), from.getEpochNanoOfMillisecond());
    }

    @Override
    public LocalZonedTimestampData copy(
            LocalZonedTimestampData from, LocalZonedTimestampData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return ((LocalZonedTimestampData.isCompact(precision)) ? 8 : 12);
    }

    @Override
    public void serialize(LocalZonedTimestampData record, DataOutputView target)
            throws IOException {
        if (LocalZonedTimestampData.isCompact(precision)) {
            assert record.getEpochNanoOfMillisecond() == 0;
            target.writeLong(record.getEpochMillisecond());
        } else {
            target.writeLong(record.getEpochMillisecond());
            target.writeInt(record.getEpochNanoOfMillisecond());
        }
    }

    @Override
    public LocalZonedTimestampData deserialize(DataInputView source) throws IOException {
        if (TimestampData.isCompact(precision)) {
            long val = source.readLong();
            return LocalZonedTimestampData.fromEpochMillis(val, 0);
        } else {
            long longVal = source.readLong();
            int intVal = source.readInt();
            return LocalZonedTimestampData.fromEpochMillis(longVal, intVal);
        }
    }

    @Override
    public LocalZonedTimestampData deserialize(LocalZonedTimestampData reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (LocalZonedTimestampData.isCompact(precision)) {
            target.writeLong(source.readLong());
        } else {
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocalZonedTimestampDataSerializer that = (LocalZonedTimestampDataSerializer) obj;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return precision;
    }

    @Override
    public TypeSerializerSnapshot<LocalZonedTimestampData> snapshotConfiguration() {
        return new LocalZonedTimestampDataSerializerSnapshot(precision);
    }

    /** {@link TypeSerializerSnapshot} for {@link TimestampDataSerializer}. */
    public static final class LocalZonedTimestampDataSerializerSnapshot
            implements TypeSerializerSnapshot<LocalZonedTimestampData> {

        private static final int CURRENT_VERSION = 1;

        private int previousPrecision;

        public LocalZonedTimestampDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        LocalZonedTimestampDataSerializerSnapshot(int precision) {
            this.previousPrecision = precision;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousPrecision);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.previousPrecision = in.readInt();
        }

        @Override
        public TypeSerializer<LocalZonedTimestampData> restoreSerializer() {
            return new LocalZonedTimestampDataSerializer(previousPrecision);
        }

        @Override
        public TypeSerializerSchemaCompatibility<LocalZonedTimestampData>
                resolveSchemaCompatibility(TypeSerializer<LocalZonedTimestampData> newSerializer) {
            if (!(newSerializer instanceof LocalZonedTimestampDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            LocalZonedTimestampDataSerializer timestampDataSerializer =
                    (LocalZonedTimestampDataSerializer) newSerializer;
            if (previousPrecision != timestampDataSerializer.precision) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
