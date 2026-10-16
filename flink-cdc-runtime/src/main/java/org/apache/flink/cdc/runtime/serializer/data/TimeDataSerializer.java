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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotAdapter;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Serializer for {@link TimeData}.
 *
 * <p>There is a single encoding path for every precision: an eight-byte {@code Long.MIN_VALUE |
 * nanoOfDay} value, mirroring the in-memory layout used by the binary records and arrays. Only the
 * serializer restored from a pre-upgrade checkpoint reads the historical four-byte millisecond
 * encoding; new state is always written with the current format.
 */
public final class TimeDataSerializer extends TypeSerializer<TimeData> {

    private static final long serialVersionUID = 1L;

    /**
     * The current, precision-independent eight-byte {@code Long.MIN_VALUE | nanoOfDay} encoding.
     */
    public static final TimeDataSerializer INSTANCE = new TimeDataSerializer(false);

    /**
     * When {@code true}, this instance reads and writes the pre-upgrade four-byte millisecond
     * encoding. It is only produced by {@link TimeDataSerializerSnapshot#restoreSerializer()} so
     * that Flink can read old managed state and migrate it with {@link #INSTANCE}.
     */
    private boolean legacyMillisFormat;

    TimeDataSerializer(boolean legacyMillisFormat) {
        this.legacyMillisFormat = legacyMillisFormat;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<TimeData> duplicate() {
        return new TimeDataSerializer(legacyMillisFormat);
    }

    @Override
    public TimeData createInstance() {
        return TimeData.fromNanoOfDay(0);
    }

    @Override
    public TimeData copy(TimeData from) {
        return TimeData.fromNanoOfDay(from.toNanoOfDay());
    }

    @Override
    public TimeData copy(TimeData from, TimeData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return legacyMillisFormat ? Integer.BYTES : Long.BYTES;
    }

    @Override
    public void serialize(TimeData record, DataOutputView target) throws IOException {
        if (legacyMillisFormat) {
            target.writeInt(record.toMillisOfDay());
        } else {
            target.writeLong(Long.MIN_VALUE | record.toNanoOfDay());
        }
    }

    @Override
    public TimeData deserialize(DataInputView source) throws IOException {
        if (legacyMillisFormat) {
            return TimeData.fromMillisOfDay(source.readInt());
        }
        long encoded = source.readLong();
        if (encoded < 0) {
            return TimeData.fromNanoOfDay(encoded & Long.MAX_VALUE);
        }
        // Defensive fallback for a payload written by a pre-upgrade serializer whose millisecond
        // value occupies the low four bytes of the slot.
        return TimeData.fromMillisOfDay((int) encoded);
    }

    @Override
    public TimeData deserialize(TimeData record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (legacyMillisFormat) {
            target.writeInt(source.readInt());
        } else {
            target.writeLong(source.readLong());
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
        return legacyMillisFormat == ((TimeDataSerializer) obj).legacyMillisFormat;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(legacyMillisFormat);
    }

    @Override
    public TypeSerializerSnapshot<TimeData> snapshotConfiguration() {
        return new TimeDataSerializerSnapshot(legacyMillisFormat);
    }

    /** Reads Java-serialized serializer instances embedded in old array/map snapshots. */
    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        ObjectInputStream.GetField fields = input.readFields();
        // The historical singleton carried no configuration at all, so a missing field means the
        // instance came from a pre-upgrade checkpoint and must use the millisecond encoding.
        legacyMillisFormat =
                fields.defaulted("legacyMillisFormat") || fields.get("legacyMillisFormat", false);
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    public static final class TimeDataSerializerSnapshot
            implements TypeSerializerSnapshotAdapter<TimeData> {

        // Version 2 wrote the serializer class name; version 3 wrote nothing. Both belonged to
        // SimpleTypeSerializerSnapshot and predate the explicit encoding marker.
        private static final int CURRENT_VERSION = 4;

        private boolean previousLegacyMillisFormat;

        public TimeDataSerializerSnapshot() {
            // Used when restoring from a checkpoint/savepoint.
        }

        private TimeDataSerializerSnapshot(boolean legacyMillisFormat) {
            this.previousLegacyMillisFormat = legacyMillisFormat;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeBoolean(previousLegacyMillisFormat);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion == 2) {
                // SimpleTypeSerializerSnapshot v2 wrote its serializer class name.
                in.readUTF();
                previousLegacyMillisFormat = true;
            } else if (readVersion == 3) {
                previousLegacyMillisFormat = true;
            } else if (readVersion == CURRENT_VERSION) {
                previousLegacyMillisFormat = in.readBoolean();
            } else {
                throw new IOException(
                        "Unrecognized TimeDataSerializer snapshot version " + readVersion);
            }
        }

        @Override
        public TypeSerializer<TimeData> restoreSerializer() {
            return new TimeDataSerializer(previousLegacyMillisFormat);
        }

        @Override
        public TypeSerializerSchemaCompatibility<TimeData> resolveSchemaCompatibility(
                TypeSerializer<TimeData> newSerializer) {
            if (!(newSerializer instanceof TimeDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            TimeDataSerializer timeSerializer = (TimeDataSerializer) newSerializer;
            return previousLegacyMillisFormat == timeSerializer.legacyMillisFormat
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }
    }
}
