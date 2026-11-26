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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.InstantiationUtil;
import org.apache.flink.cdc.runtime.serializer.IntSerializer;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * State compatibility tests for {@code TIME} values that are nested inside other serializers.
 *
 * <p>Older array, map and nullable snapshots stored the nested serializers themselves instead of
 * their snapshots. Restoring such a snapshot yields a nested serializer that still carries the
 * pre-upgrade configuration, so a plain {@code equals} comparison with the serializer built by the
 * upgraded code rejects the state even when the previous format is perfectly readable. These tests
 * feed the exact bytes a pre-upgrade job persisted to the new snapshot classes and pin both
 * directions: readable legacy payloads may migrate, and nested changes that really are unreadable
 * must still be reported as incompatible.
 */
class NestedTimeStateCompatibilityTest {

    private static final ClassLoader CLASS_LOADER = Thread.currentThread().getContextClassLoader();

    /** {@link RecordDataSerializer} tag for {@code BinaryRecordData} payloads. */
    private static final byte BINARY_RECORD_TYPE = 0;

    /** {@link RecordDataSerializer} tag for self-describing {@code GenericRecordData} payloads. */
    private static final byte GENERIC_RECORD_TYPE = 1;

    /**
     * The historical {@code GenericRecordDataSerializer} field tag for millisecond-of-day TIME. It
     * is no longer written, but pre-upgrade rows still use it.
     */
    private static final byte LEGACY_TIME_TAG = 15;

    private static final int MILLIS_OF_DAY = 3_723_123;

    @Test
    void millisecondPrecisionTimeRestoresWithoutMigration() throws Exception {
        TypeSerializerSnapshot<TimeData> legacySnapshot = readLegacyDirectTimeSnapshot();

        assertThat(
                        resolve(legacySnapshot, TimeDataSerializer.INSTANCE)
                                .isCompatibleAfterMigration())
                .isTrue();
        assertThat(resolve(legacySnapshot, new TimeDataSerializer(true)).isCompatibleAsIs())
                .isTrue();

        DataOutputSerializer oldPayload = new DataOutputSerializer(Integer.BYTES);
        oldPayload.writeInt(MILLIS_OF_DAY);
        assertThat(
                        legacySnapshot
                                .restoreSerializer()
                                .deserialize(
                                        new DataInputDeserializer(oldPayload.getCopyOfBuffer()))
                                .toNanoOfDay())
                .isEqualTo(MILLIS_OF_DAY * 1_000_000L);
    }

    @Test
    void nullableWrapperMigratesNestedTimeSerializer() throws Exception {
        // Byte-for-byte what an older NullableSerializerWrapperSnapshot persisted: the versioned
        // snapshot header followed by the Java-serialized pre-upgrade TimeDataSerializer.
        TypeSerializerSnapshot<?> legacySnapshot =
                readManualSnapshot(
                        NullableSerializerWrapper.NullableSerializerWrapperSnapshot.class,
                        1,
                        TimeDataSerializerCompatibilityTest.legacyMillisTimeSerializerBytes());

        assertThat(
                        resolve(
                                        legacySnapshot,
                                        new NullableSerializerWrapper<>(
                                                TimeDataSerializer.INSTANCE))
                                .isCompatibleAfterMigration())
                .isTrue();
        assertThat(
                        resolve(
                                        legacySnapshot,
                                        new NullableSerializerWrapper<>(
                                                new TimeDataSerializer(true)))
                                .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void arrayElementTimeSerializerMigratesFromLegacyState() throws Exception {
        TypeSerializer<TimeData> legacyElement =
                new NullableSerializerWrapper<>(
                        TimeDataSerializerCompatibilityTest.legacyMillisTimeSerializer());

        TypeSerializerSnapshot<?> legacyMillisArray =
                readManualSnapshot(
                        ArrayDataSerializer.ArrayDataSerializerSnapshot.class,
                        3,
                        InstantiationUtil.serializeObject(DataTypes.TIME(3)),
                        InstantiationUtil.serializeObject(legacyElement));

        assertThat(
                        resolve(legacyMillisArray, new ArrayDataSerializer(DataTypes.TIME(3)))
                                .isCompatibleAfterMigration())
                .isTrue();

        // Pre-upgrade array state is still readable through the restored serializer, which keeps
        // the
        // historical four-byte millisecond element encoding.
        ArrayDataSerializer restoredMillisArray =
                (ArrayDataSerializer) legacyMillisArray.restoreSerializer();
        DataOutputSerializer oldPayload = new DataOutputSerializer(Integer.BYTES * 4);
        restoredMillisArray.serialize(
                new GenericArrayData(new Object[] {TimeData.fromMillisOfDay(MILLIS_OF_DAY)}),
                oldPayload);
        byte[] oldBytes = oldPayload.getCopyOfBuffer();

        assertThat(readArrayTime(restoredMillisArray, oldBytes).toMillisOfDay())
                .isEqualTo(MILLIS_OF_DAY);
    }

    @Test
    void mapValueTimeSerializerMigratesFromLegacyState() throws Exception {
        TypeSerializer<?> legacyMillisTime =
                TimeDataSerializerCompatibilityTest.legacyMillisTimeSerializer();

        TypeSerializerSnapshot<?> legacyMillisMap =
                readManualSnapshot(
                        MapDataSerializer.MapDataSerializerSnapshot.class,
                        0,
                        InstantiationUtil.serializeObject(DataTypes.STRING()),
                        InstantiationUtil.serializeObject(DataTypes.TIME(3)),
                        InstantiationUtil.serializeObject(
                                InternalSerializers.create(DataTypes.STRING())),
                        InstantiationUtil.serializeObject(legacyMillisTime));

        assertThat(
                        resolve(
                                        legacyMillisMap,
                                        new MapDataSerializer(
                                                DataTypes.STRING(), DataTypes.TIME(3)))
                                .isCompatibleAfterMigration())
                .isTrue();

        // Pre-upgrade map state with the millisecond layout stays readable.
        MapDataSerializer restoredMillisMap =
                (MapDataSerializer) legacyMillisMap.restoreSerializer();
        Map<StringData, TimeData> source = new LinkedHashMap<>();
        source.put(BinaryStringData.fromString("time"), TimeData.fromMillisOfDay(MILLIS_OF_DAY));
        DataOutputSerializer oldPayload = new DataOutputSerializer(64);
        restoredMillisMap.serialize(new GenericMapData(source), oldPayload);
        MapData restored =
                restoredMillisMap.deserialize(
                        new DataInputDeserializer(oldPayload.getCopyOfBuffer()));
        assertThat(restored.valueArray().getTime(0).toMillisOfDay()).isEqualTo(MILLIS_OF_DAY);
    }

    @Test
    void recordDataStateKeepsLegacyTimePayloadReadable() throws Exception {
        TypeSerializerSnapshot<?> legacySnapshot =
                readManualSnapshot(RecordDataSerializer.RecordDataSerializerSnapshot.class, 3);

        assertThat(resolve(legacySnapshot, RecordDataSerializer.INSTANCE).isCompatibleAsIs())
                .isTrue();
        assertThat(resolve(legacySnapshot, new RecordDataSerializer()).isCompatibleAsIs()).isTrue();

        // A ROW payload written before the upgrade stores TIME as the legacy tag followed by a
        // millisecond-of-day int. The upgraded deserializer must still understand those bytes.
        DataOutputSerializer oldPayload = new DataOutputSerializer(Integer.BYTES * 3);
        oldPayload.writeByte(GENERIC_RECORD_TYPE);
        oldPayload.writeInt(1);
        oldPayload.writeByte(LEGACY_TIME_TAG);
        oldPayload.writeInt(MILLIS_OF_DAY);

        RecordData record =
                RecordDataSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(oldPayload.getCopyOfBuffer()));
        assertThat(record).isInstanceOf(GenericRecordData.class);
        assertThat(record.getTime(0).toMillisOfDay()).isEqualTo(MILLIS_OF_DAY);

        // Binary rows are unaffected: RecordDataSerializer has no nested serializer configuration.
        DataOutputSerializer binaryPayload = new DataOutputSerializer(Integer.BYTES * 3);
        binaryPayload.writeByte(BINARY_RECORD_TYPE);
        binaryPayload.writeInt(1);
        binaryPayload.writeInt(0);
        assertThat(
                        RecordDataSerializer.INSTANCE
                                .deserialize(
                                        new DataInputDeserializer(binaryPayload.getCopyOfBuffer()))
                                .getArity())
                .isEqualTo(1);
    }

    @Test
    void unmigratableNestedSerializersAreStillRejected() throws Exception {
        // A nested serializer whose snapshot has no format-aware compatibility logic cannot be
        // upgraded, so the previous outright rejection must be preserved.
        TypeSerializerSnapshot<?> legacyNullable =
                readManualSnapshot(
                        NullableSerializerWrapper.NullableSerializerWrapperSnapshot.class,
                        1,
                        InstantiationUtil.serializeObject(StringSerializer.INSTANCE));
        assertThat(
                        resolve(
                                        legacyNullable,
                                        new NullableSerializerWrapper<>(IntSerializer.INSTANCE))
                                .isIncompatible())
                .isTrue();

        // Swapping the element serializer while keeping the element type is not an upgrade either.
        TypeSerializerSnapshot<?> legacyArray =
                readManualSnapshot(
                        ArrayDataSerializer.ArrayDataSerializerSnapshot.class,
                        3,
                        InstantiationUtil.serializeObject(DataTypes.STRING()),
                        InstantiationUtil.serializeObject(
                                new NullableSerializerWrapper<>(StringSerializer.INSTANCE)));
        assertThat(
                        resolve(legacyArray, new ArrayDataSerializer(DataTypes.STRING()))
                                .isIncompatible())
                .isTrue();

        // A different TIME precision is a different array element type.
        TypeSerializerSnapshot<?> legacyNanosArray =
                readManualSnapshot(
                        ArrayDataSerializer.ArrayDataSerializerSnapshot.class,
                        3,
                        InstantiationUtil.serializeObject(DataTypes.TIME(6)),
                        InstantiationUtil.serializeObject(
                                new NullableSerializerWrapper<>(
                                        TimeDataSerializerCompatibilityTest
                                                .legacyMillisTimeSerializer())));
        assertThat(
                        resolve(legacyNanosArray, new ArrayDataSerializer(DataTypes.TIME(3)))
                                .isIncompatible())
                .isTrue();
    }

    // ------------------------------------------------------------------------
    //  Test utilities
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static TypeSerializerSchemaCompatibility<?> resolve(
            TypeSerializerSnapshot<?> previousSnapshot, TypeSerializer<?> newSerializer) {
        return ((TypeSerializerSnapshot<Object>) previousSnapshot)
                .resolveSchemaCompatibility((TypeSerializer<Object>) newSerializer);
    }

    private static TimeData readArrayTime(TypeSerializer<ArrayData> serializer, byte[] payload)
            throws IOException {
        return serializer.deserialize(new DataInputDeserializer(payload)).getTime(0);
    }

    /**
     * Reads the top-level state envelope an older job wrote for {@link TimeDataSerializer}: the
     * versioned snapshot proxy followed by the class name and the historical {@code
     * SimpleTypeSerializerSnapshot} version, which carried no payload.
     */
    private static TypeSerializerSnapshot<TimeData> readLegacyDirectTimeSnapshot()
            throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeInt(2); // TypeSerializerSnapshotSerializationUtil proxy version
        out.writeUTF(TimeDataSerializer.TimeDataSerializerSnapshot.class.getName());
        out.writeInt(3); // SimpleTypeSerializerSnapshot#getCurrentVersion before the upgrade
        return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                new DataInputDeserializer(out.getCopyOfBuffer()), CLASS_LOADER);
    }

    /**
     * Rebuilds the pre-upgrade state of a composite snapshot: the versioned snapshot header an
     * older job wrote, followed by the raw Java-serialization bytes of the serializers that the
     * historical {@code writeSnapshot}/{@code readSnapshot} pair persisted.
     */
    private static TypeSerializerSnapshot<?> readManualSnapshot(
            Class<?> snapshotClass, int version, byte[]... javaSerializedPayloads)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        out.writeUTF(snapshotClass.getName());
        out.writeInt(version);
        for (byte[] payload : javaSerializedPayloads) {
            out.write(payload);
        }
        return TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(out.getCopyOfBuffer()), CLASS_LOADER);
    }
}
