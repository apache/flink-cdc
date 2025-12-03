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

import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.cdc.common.utils.InstantiationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/** A test for the {@link EnumSerializer}. */
class EnumSerializerTest extends TestLogger {

    @Test
    void testPublicEnum() {
        testEnumSerializer(PrivateEnum.ONE, PrivateEnum.TWO, PrivateEnum.THREE);
    }

    @Test
    void testPrivateEnum() {
        testEnumSerializer(
                PublicEnum.FOO,
                PublicEnum.BAR,
                PublicEnum.PETER,
                PublicEnum.NATHANIEL,
                PublicEnum.EMMA,
                PublicEnum.PAULA);
    }

    @Test
    void testEmptyEnum() {
        Assertions.assertThatThrownBy(() -> new EnumSerializer<>(EmptyEnum.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReconfiguration() {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializerSnapshot.resolveSchemaCompatibility(serializer);
        Assertions.assertThat(compatibility.isCompatibleWithReconfiguredSerializer()).isTrue();

        // after reconfiguration, the order should be first the original BAR, PAULA, NATHANIEL,
        // followed by the "new enum constants" FOO, PETER, EMMA
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            Assertions.assertThat(configuredSerializer.getValueToOrdinal().get(constant).intValue())
                    .isEqualTo(i);
            i++;
        }

        Assertions.assertThat(configuredSerializer.getValues()).isEqualTo(expectedOrder);
    }

    @Test
    void testConfigurationSnapshotSerialization() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), serializer.snapshotConfiguration());
            serializedConfig = out.toByteArray();
        }

        TypeSerializerSnapshot<PublicEnum> restoredConfig;
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            restoredConfig =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        TypeSerializerSchemaCompatibility<PublicEnum> compatResult =
                restoredConfig.resolveSchemaCompatibility(serializer);
        Assertions.assertThat(compatResult.isCompatibleAsIs()).isTrue();

        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        Assertions.assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());
    }

    @Test
    void testSerializeEnumSerializer() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify original transient parameters
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        Assertions.assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());

        byte[] serializedSerializer = InstantiationUtil.serializeObject(serializer);

        // deserialize and re-verify transient parameters
        serializer =
                InstantiationUtil.deserializeObject(
                        serializedSerializer, Thread.currentThread().getContextClassLoader());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        Assertions.assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());
    }

    @Test
    void testSerializeReconfiguredEnumSerializer() throws Exception {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        Assertions.assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializerSnapshot.resolveSchemaCompatibility(serializer);
        Assertions.assertThat(compatibility.isCompatibleWithReconfiguredSerializer()).isTrue();

        // verify that after the serializer was read, the reconfigured constant ordering is
        // untouched
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            Assertions.assertThat(configuredSerializer.getValueToOrdinal().get(constant).intValue())
                    .isEqualTo(i);
            i++;
        }

        Assertions.assertThat(configuredSerializer.getValues()).isEqualTo(expectedOrder);
    }

    @SafeVarargs
    public final <T extends Enum<T>> void testEnumSerializer(T... data) {
        @SuppressWarnings("unchecked")
        final Class<T> clazz = (Class<T>) data.getClass().getComponentType();

        SerializerTestInstance<T> tester =
                new SerializerTestInstance<T>(new EnumSerializer<T>(clazz), clazz, 4, data) {};

        tester.testAll();
    }

    // ------------------------------------------------------------------------
    //  Test enums
    // ------------------------------------------------------------------------

    /** A test enum. */
    public enum PublicEnum {
        FOO,
        BAR,
        PETER,
        NATHANIEL,
        EMMA,
        PAULA
    }

    /** A test enum. */
    public enum EmptyEnum {}

    private enum PrivateEnum {
        ONE,
        TWO,
        THREE
    }
}
