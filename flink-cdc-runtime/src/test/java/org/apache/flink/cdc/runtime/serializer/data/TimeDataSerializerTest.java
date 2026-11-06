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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.time.LocalTime;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeDataSerializer}. */
class TimeDataSerializerTest extends SerializerTestBase<TimeData> {

    @Override
    protected TypeSerializer<TimeData> createSerializer() {
        return TimeDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return Long.BYTES;
    }

    @Override
    protected Class<TimeData> getTypeClass() {
        return TimeData.class;
    }

    @Override
    protected TimeData[] getTestData() {
        return new TimeData[] {
            TimeData.fromNanoOfDay(102_400),
            TimeData.fromNanoOfDay(20_480_123_456L),
            TimeData.fromIsoLocalTimeString("14:28:25.123456789"),
            TimeData.fromLocalTime(LocalTime.of(23, 59, 59, 999_999_999))
        };
    }

    @Test
    void roundTripKeepsNanosecondPrecision() throws Exception {
        long nanos = 3_723_123_456_789L;
        DataOutputSerializer output = new DataOutputSerializer(Long.BYTES);
        TimeDataSerializer.INSTANCE.serialize(TimeData.fromNanoOfDay(nanos), output);

        TimeData restored =
                TimeDataSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(restored.toNanoOfDay()).isEqualTo(nanos);
    }

    @Test
    void allPrecisionsShareTheSameEncodingWidth() {
        // The element serializer is precision-independent: every TIME precision resolves to the
        // same
        // eight-byte serializer, so the previous precision-dependent width is gone.
        assertThat(TimeDataSerializer.INSTANCE.getLength()).isEqualTo(Long.BYTES);
        assertThat(InternalSerializers.create(DataTypes.TIME(0)))
                .isSameAs(TimeDataSerializer.INSTANCE);
        assertThat(InternalSerializers.create(DataTypes.TIME(3)))
                .isSameAs(TimeDataSerializer.INSTANCE);
        assertThat(InternalSerializers.create(DataTypes.TIME(9)))
                .isSameAs(TimeDataSerializer.INSTANCE);
    }
}

/** Compatibility tests for {@link TimeDataSerializer} state written before the upgrade. */
final class TimeDataSerializerCompatibilityTest {

    private static final String LEGACY_SERIALIZER_BASE64 =
            "rO0ABXNyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuZGF0YS5UaW1lRGF0YVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuVHlwZVNlcmlhbGl6ZXJTaW5nbGV0b255qYeqxy53RQIAAHhyADRvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVTZXJpYWxpemVyAAAAAAAAAAECAAB4cA==";

    private static final String LEGACY_SNAPSHOT_BASE64 =
            "AAAAAgBab3JnLmFwYWNoZS5mbGluay5jZGMucnVudGltZS5zZXJpYWxpemVyLmRhdGEuVGltZURhdGFTZXJpYWxpemVyJFRpbWVEYXRhU2VyaWFsaXplclNuYXBzaG90AAAAAw==";

    @Test
    void oldMillisecondSnapshotIsMigratableToTheCurrentFormat() throws Exception {
        TimeDataSerializer.TimeDataSerializerSnapshot oldSnapshot =
                new TimeDataSerializer.TimeDataSerializerSnapshot();
        oldSnapshot.readSnapshot(
                3,
                new DataInputDeserializer(new byte[0]),
                Thread.currentThread().getContextClassLoader());

        assertThat(
                        oldSnapshot
                                .resolveSchemaCompatibility(TimeDataSerializer.INSTANCE)
                                .isCompatibleAfterMigration())
                .isTrue();
        assertThat(
                        oldSnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(true))
                                .isCompatibleAsIs())
                .isTrue();

        DataOutputSerializer oldBytes = new DataOutputSerializer(Integer.BYTES);
        oldBytes.writeInt(3_723_123);
        TimeData restored =
                oldSnapshot
                        .restoreSerializer()
                        .deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()));
        assertThat(restored.toNanoOfDay()).isEqualTo(3_723_123_000_000L);
    }

    @Test
    void readsLegacySnapshotEnvelopeAndFourBytePayload() throws Exception {
        TypeSerializerSnapshot<TimeData> snapshot =
                TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                        new DataInputDeserializer(
                                Base64.getDecoder().decode(LEGACY_SNAPSHOT_BASE64)),
                        Thread.currentThread().getContextClassLoader());
        assertThat(snapshot).isInstanceOf(TimeDataSerializer.TimeDataSerializerSnapshot.class);

        assertThat(
                        snapshot.resolveSchemaCompatibility(TimeDataSerializer.INSTANCE)
                                .isCompatibleAfterMigration())
                .isTrue();
        DataOutputSerializer oldBytes = new DataOutputSerializer(Integer.BYTES);
        oldBytes.writeInt(3_723_123);
        assertThat(
                        snapshot.restoreSerializer()
                                .deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()))
                                .toNanoOfDay())
                .isEqualTo(3_723_123_000_000L);
    }

    @Test
    void readsLegacyJavaSerializedSingleton() throws Exception {
        TimeDataSerializer serializer = legacyMillisTimeSerializer();

        assertThat(serializer.getLength()).isEqualTo(Integer.BYTES);
        DataOutputSerializer oldBytes = new DataOutputSerializer(Integer.BYTES);
        oldBytes.writeInt(3_723_123);
        assertThat(
                        serializer
                                .deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()))
                                .toNanoOfDay())
                .isEqualTo(3_723_123_000_000L);
        assertThat(
                        serializer
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(TimeDataSerializer.INSTANCE)
                                .isCompatibleAfterMigration())
                .isTrue();
    }

    /**
     * Restores the pre-upgrade {@link TimeDataSerializer} singleton from the exact bytes that older
     * jobs persisted. The historical class carried no configuration, so {@code readObject} has to
     * fall back to the millisecond encoding; nested composite snapshots restore their element
     * serializers the same way.
     */
    static TimeDataSerializer legacyMillisTimeSerializer() throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(legacyMillisTimeSerializerBytes()))) {
            return (TimeDataSerializer) input.readObject();
        }
    }

    /**
     * The raw Java-serialization bytes of the pre-upgrade {@link TimeDataSerializer} singleton, as
     * embedded inside the nested snapshots that stored serializers instead of snapshots.
     */
    static byte[] legacyMillisTimeSerializerBytes() {
        return Base64.getDecoder().decode(LEGACY_SERIALIZER_BASE64);
    }
}
