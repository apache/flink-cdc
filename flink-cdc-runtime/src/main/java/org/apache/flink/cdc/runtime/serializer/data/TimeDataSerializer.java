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
 * <p>TIME values with precision up to 3 retain the historical four-byte millisecond encoding.
 * Higher precisions use an eight-byte nanosecond-of-day encoding.
 */
public final class TimeDataSerializer extends TypeSerializer<TimeData> {

    private static final long serialVersionUID = 1L;

    /** The historical singleton represents the millisecond TIME encoding. */
    public static final TimeDataSerializer INSTANCE = new TimeDataSerializer(3);

    private int precision;
    private boolean legacyFormat;

    public TimeDataSerializer(int precision) {
        this(precision, false);
    }

    private TimeDataSerializer(int precision, boolean legacyFormat) {
        if (precision < 0 || precision > 9) {
            throw new IllegalArgumentException("TIME precision must be between 0 and 9");
        }
        this.precision = precision;
        this.legacyFormat = legacyFormat;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<TimeData> duplicate() {
        return new TimeDataSerializer(precision, legacyFormat);
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
        return usesMillisEncoding() ? Integer.BYTES : Long.BYTES;
    }

    @Override
    public void serialize(TimeData record, DataOutputView target) throws IOException {
        if (usesMillisEncoding()) {
            target.writeInt(record.toMillisOfDay());
        } else {
            target.writeLong(record.toNanoOfDay());
        }
    }

    @Override
    public TimeData deserialize(DataInputView source) throws IOException {
        return usesMillisEncoding()
                ? TimeData.fromMillisOfDay(source.readInt())
                : TimeData.fromNanoOfDay(source.readLong());
    }

    @Override
    public TimeData deserialize(TimeData record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (usesMillisEncoding()) {
            target.writeInt(source.readInt());
        } else {
            target.writeLong(source.readLong());
        }
    }

    private boolean usesMillisEncoding() {
        return legacyFormat || precision <= 3;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeDataSerializer that = (TimeDataSerializer) obj;
        return precision == that.precision && legacyFormat == that.legacyFormat;
    }

    @Override
    public int hashCode() {
        return 31 * precision + Boolean.hashCode(legacyFormat);
    }

    @Override
    public TypeSerializerSnapshot<TimeData> snapshotConfiguration() {
        return new TimeDataSerializerSnapshot(precision, legacyFormat);
    }

    /** Reads Java-serialized serializer instances embedded in old array/map snapshots. */
    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        ObjectInputStream.GetField fields = input.readFields();
        if (fields.defaulted("precision")) {
            precision = 3;
            legacyFormat = true;
        } else {
            precision = fields.get("precision", 3);
            legacyFormat = fields.get("legacyFormat", false);
        }
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    public static final class TimeDataSerializerSnapshot
            implements TypeSerializerSnapshotAdapter<TimeData> {

        // Versions 2 and 3 belonged to SimpleTypeSerializerSnapshot and contained no precision.
        private static final int CURRENT_VERSION = 4;

        private int previousPrecision;
        private boolean previousLegacyFormat;

        public TimeDataSerializerSnapshot() {
            // Used when restoring from a checkpoint/savepoint.
        }

        private TimeDataSerializerSnapshot(int precision, boolean legacyFormat) {
            this.previousPrecision = precision;
            this.previousLegacyFormat = legacyFormat;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousPrecision);
            out.writeBoolean(previousLegacyFormat);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion == 2) {
                // SimpleTypeSerializerSnapshot v2 wrote its serializer class name.
                in.readUTF();
                previousPrecision = 3;
                previousLegacyFormat = true;
            } else if (readVersion == 3) {
                previousPrecision = 3;
                previousLegacyFormat = true;
            } else if (readVersion == CURRENT_VERSION) {
                previousPrecision = in.readInt();
                previousLegacyFormat = in.readBoolean();
            } else {
                throw new IOException(
                        "Unrecognized TimeDataSerializer snapshot version " + readVersion);
            }
        }

        @Override
        public TypeSerializer<TimeData> restoreSerializer() {
            return new TimeDataSerializer(previousPrecision, previousLegacyFormat);
        }

        @Override
        public TypeSerializerSchemaCompatibility<TimeData> resolveSchemaCompatibility(
                TypeSerializer<TimeData> newSerializer) {
            if (!(newSerializer instanceof TimeDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            TimeDataSerializer timeSerializer = (TimeDataSerializer) newSerializer;
            boolean previousMillisEncoding = previousLegacyFormat || previousPrecision <= 3;
            return previousMillisEncoding == timeSerializer.usesMillisEncoding()
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }
    }
}
