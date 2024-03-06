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
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Serializer for {@link ZonedTimestampData}.
 *
 * <p>A {@link ZonedTimestampData} instance can be compactly serialized as a long value(=
 * millisecond) when the Timestamp type is compact. Otherwise it's serialized as a long value and a
 * int value.
 */
public class ZonedTimestampDataSerializer extends TypeSerializer<ZonedTimestampData> {

    private static final long serialVersionUID = 1L;

    private final int precision;

    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;

    public ZonedTimestampDataSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<ZonedTimestampData> duplicate() {
        return new ZonedTimestampDataSerializer(precision);
    }

    @Override
    public ZonedTimestampData createInstance() {
        return ZonedTimestampData.of(1, 1, "UTC");
    }

    @Override
    public ZonedTimestampData copy(ZonedTimestampData from) {
        return ZonedTimestampData.of(
                from.getMillisecond(), from.getNanoOfMillisecond(), from.getZoneId());
    }

    @Override
    public ZonedTimestampData copy(ZonedTimestampData from, ZonedTimestampData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return ((ZonedTimestampData.isCompact(precision)) ? 8 : 12);
    }

    @Override
    public void serialize(ZonedTimestampData record, DataOutputView target) throws IOException {
        stringSerializer.serialize(record.getZoneId(), target);
        if (ZonedTimestampData.isCompact(precision)) {
            assert record.getNanoOfMillisecond() == 0;
            target.writeLong(record.getMillisecond());
        } else {
            target.writeLong(record.getMillisecond());
            target.writeInt(record.getNanoOfMillisecond());
        }
    }

    @Override
    public ZonedTimestampData deserialize(DataInputView source) throws IOException {
        String zoneId = stringSerializer.deserialize(source);
        if (TimestampData.isCompact(precision)) {
            long val = source.readLong();
            return ZonedTimestampData.of(val, 0, zoneId);
        } else {
            long longVal = source.readLong();
            int intVal = source.readInt();
            return ZonedTimestampData.of(longVal, intVal, zoneId);
        }
    }

    @Override
    public ZonedTimestampData deserialize(ZonedTimestampData reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        stringSerializer.serialize(stringSerializer.deserialize(source), target);
        if (ZonedTimestampData.isCompact(precision)) {
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

        ZonedTimestampDataSerializer that = (ZonedTimestampDataSerializer) obj;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return precision;
    }

    @Override
    public TypeSerializerSnapshot<ZonedTimestampData> snapshotConfiguration() {
        return new ZonedTimestampDataSerializerSnapshot(precision);
    }

    /** {@link TypeSerializerSnapshot} for {@link ZonedTimestampDataSerializer}. */
    public static final class ZonedTimestampDataSerializerSnapshot
            implements TypeSerializerSnapshot<ZonedTimestampData> {

        private static final int CURRENT_VERSION = 1;

        private int previousPrecision;

        public ZonedTimestampDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        ZonedTimestampDataSerializerSnapshot(int precision) {
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
        public TypeSerializer<ZonedTimestampData> restoreSerializer() {
            return new ZonedTimestampDataSerializer(previousPrecision);
        }

        @Override
        public TypeSerializerSchemaCompatibility<ZonedTimestampData> resolveSchemaCompatibility(
                TypeSerializer<ZonedTimestampData> newSerializer) {
            if (!(newSerializer instanceof ZonedTimestampDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ZonedTimestampDataSerializer timestampDataSerializer =
                    (ZonedTimestampDataSerializer) newSerializer;
            if (previousPrecision != timestampDataSerializer.precision) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
