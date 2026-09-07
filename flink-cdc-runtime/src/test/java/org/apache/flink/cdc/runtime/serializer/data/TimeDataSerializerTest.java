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
import org.apache.flink.cdc.common.data.TimeData;
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
abstract class TimeDataSerializerTest extends SerializerTestBase<TimeData> {
    @Override
    protected TypeSerializer<TimeData> createSerializer() {
        return new TimeDataSerializer(getPrecision());
    }

    @Override
    protected int getLength() {
        return getPrecision() <= 3 ? Integer.BYTES : Long.BYTES;
    }

    @Override
    protected Class<TimeData> getTypeClass() {
        return TimeData.class;
    }

    @Override
    protected TimeData[] getTestData() {
        if (getPrecision() <= 3) {
            return new TimeData[] {
                TimeData.fromSecondOfDay(1024),
                TimeData.fromMillisOfDay(20480),
                TimeData.fromIsoLocalTimeString("14:28:25.123"),
                TimeData.fromLocalTime(LocalTime.NOON)
            };
        }
        return new TimeData[] {
            TimeData.fromNanoOfDay(102_400),
            TimeData.fromNanoOfDay(20_480_123_456L),
            TimeData.fromIsoLocalTimeString("14:28:25.123456789"),
            TimeData.fromLocalTime(LocalTime.of(23, 59, 59, 999_999_999))
        };
    }

    protected abstract int getPrecision();

    @Test
    void roundTripRetainsDirectNumericPrecision() throws Exception {
        long nanos = getPrecision() <= 3 ? 3_723_123_000_000L : 3_723_123_456_789L;
        TimeDataSerializer serializer = new TimeDataSerializer(getPrecision());
        DataOutputSerializer output = new DataOutputSerializer(serializer.getLength());
        serializer.serialize(TimeData.fromNanoOfDay(nanos), output);

        TimeData restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(restored.toNanoOfDay()).isEqualTo(nanos);
    }
}

final class TimeDataSerializer3Test extends TimeDataSerializerTest {
    @Override
    protected int getPrecision() {
        return 3;
    }
}

final class TimeDataSerializer6Test extends TimeDataSerializerTest {
    @Override
    protected int getPrecision() {
        return 6;
    }
}

final class TimeDataSerializer9Test extends TimeDataSerializerTest {
    @Override
    protected int getPrecision() {
        return 9;
    }
}

final class TimeDataSerializerCompatibilityTest {

    private static final String LEGACY_SERIALIZER_BASE64 =
            "rO0ABXNyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuZGF0YS5UaW1lRGF0YVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuVHlwZVNlcmlhbGl6ZXJTaW5nbGV0b255qYeqxy53RQIAAHhyADRvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVTZXJpYWxpemVyAAAAAAAAAAECAAB4cA==";

    private static final String LEGACY_SNAPSHOT_BASE64 =
            "AAAAAgBab3JnLmFwYWNoZS5mbGluay5jZGMucnVudGltZS5zZXJpYWxpemVyLmRhdGEuVGltZURhdGFTZXJpYWxpemVyJFRpbWVEYXRhU2VyaWFsaXplclNuYXBzaG90AAAAAw==";

    @Test
    void oldMillisecondSnapshotIsCompatibleOrMigratableByPrecision() throws Exception {
        TimeDataSerializer.TimeDataSerializerSnapshot oldSnapshot =
                new TimeDataSerializer.TimeDataSerializerSnapshot();
        oldSnapshot.readSnapshot(
                3,
                new DataInputDeserializer(new byte[0]),
                Thread.currentThread().getContextClassLoader());

        TypeSerializerSchemaCompatibility<TimeData> millisCompatibility =
                oldSnapshot.resolveSchemaCompatibility(new TimeDataSerializer(3));
        TypeSerializerSchemaCompatibility<TimeData> microsCompatibility =
                oldSnapshot.resolveSchemaCompatibility(new TimeDataSerializer(6));
        assertThat(millisCompatibility.isCompatibleAsIs()).isTrue();
        assertThat(microsCompatibility.isCompatibleAfterMigration()).isTrue();

        DataOutputSerializer oldBytes = new DataOutputSerializer(Integer.BYTES);
        oldBytes.writeInt(3_723_123);
        TimeData restored =
                oldSnapshot
                        .restoreSerializer()
                        .deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()));
        assertThat(restored.toNanoOfDay()).isEqualTo(3_723_123_000_000L);
    }

    @Test
    void serializerCompatibilityDependsOnBinaryEncodingWidth() {
        TimeDataSerializer.TimeDataSerializerSnapshot millisSnapshot =
                (TimeDataSerializer.TimeDataSerializerSnapshot)
                        new TimeDataSerializer(3).snapshotConfiguration();
        TimeDataSerializer.TimeDataSerializerSnapshot nanosSnapshot =
                (TimeDataSerializer.TimeDataSerializerSnapshot)
                        new TimeDataSerializer(6).snapshotConfiguration();

        assertThat(
                        millisSnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(0))
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(
                        nanosSnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(9))
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(
                        millisSnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(6))
                                .isCompatibleAfterMigration())
                .isTrue();
        assertThat(
                        nanosSnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(3))
                                .isCompatibleAfterMigration())
                .isTrue();
    }

    @Test
    void readsLegacySnapshotEnvelopeAndFourBytePayload() throws Exception {
        TypeSerializerSnapshot<TimeData> snapshot =
                TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                        new DataInputDeserializer(
                                Base64.getDecoder().decode(LEGACY_SNAPSHOT_BASE64)),
                        Thread.currentThread().getContextClassLoader());
        assertThat(snapshot).isInstanceOf(TimeDataSerializer.TimeDataSerializerSnapshot.class);
        TimeDataSerializer.TimeDataSerializerSnapshot legacySnapshot =
                (TimeDataSerializer.TimeDataSerializerSnapshot) snapshot;

        assertThat(
                        legacySnapshot
                                .resolveSchemaCompatibility(new TimeDataSerializer(3))
                                .isCompatibleAsIs())
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
        TimeDataSerializer serializer;
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(
                                Base64.getDecoder().decode(LEGACY_SERIALIZER_BASE64)))) {
            serializer = (TimeDataSerializer) input.readObject();
        }

        assertThat(serializer.getLength()).isEqualTo(Integer.BYTES);
        DataOutputSerializer oldBytes = new DataOutputSerializer(Integer.BYTES);
        oldBytes.writeInt(3_723_123);
        assertThat(
                        serializer
                                .deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()))
                                .toNanoOfDay())
                .isEqualTo(3_723_123_000_000L);
    }
}
