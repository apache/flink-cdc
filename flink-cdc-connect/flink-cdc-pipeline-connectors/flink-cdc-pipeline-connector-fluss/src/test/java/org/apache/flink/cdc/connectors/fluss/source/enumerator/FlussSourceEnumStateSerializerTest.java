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

package org.apache.flink.cdc.connectors.fluss.source.enumerator;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSourceEnumStateSerializer}. */
class FlussSourceEnumStateSerializerTest {

    @Test
    void testSerializeStateInVersionThree() throws Exception {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(TablePath.of("database", "table"));
        FlussSplitBase split = new FlussLogSplit(physicalTablePath, new TableBucket(1L, 0), 42L);
        FlussSourceEnumState state =
                new FlussSourceEnumState(
                        Collections.singleton(physicalTablePath),
                        Collections.singletonList(split),
                        "lease-id");
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(serializer.getVersion()).isEqualTo(3);
        assertThat(restored.getAssignedPhysicalTablePaths()).containsExactly(physicalTablePath);
        assertThat(restored.getRemainingSplits()).containsExactly(split);
        assertThat(restored.getLeaseId()).isEqualTo("lease-id");
        assertThat(restored.getPendingRemovalTablePaths()).isEmpty();
        assertThat(restored.getPendingRemovalPartitionPaths()).isEmpty();
    }

    @Test
    void testSerializePendingRemovalTablePathsInVersionThreeState() throws Exception {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(TablePath.of("database", "table"));
        TablePath pendingRemovalTablePath = TablePath.of("database", "removed_table");
        FlussSplitBase split = new FlussLogSplit(physicalTablePath, new TableBucket(1L, 0), 42L);
        FlussSourceEnumState state =
                new FlussSourceEnumState(
                        Collections.singleton(physicalTablePath),
                        Collections.singletonList(split),
                        "lease-id",
                        Collections.singleton(pendingRemovalTablePath));
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(serializer.getVersion()).isEqualTo(3);
        assertThat(restored.getPendingRemovalTablePaths()).containsExactly(pendingRemovalTablePath);
        assertThat(restored.getPendingRemovalPartitionPaths()).isEmpty();
    }

    @Test
    void testSerializePendingTableAndPartitionRemovalsInVersionThreeState() throws Exception {
        TablePath tablePath = TablePath.of("database", "table");
        PhysicalTablePath assignedPartitionPath = PhysicalTablePath.of(tablePath, "retained");
        PhysicalTablePath pendingRemovalPartitionPath = PhysicalTablePath.of(tablePath, "expired");
        TablePath pendingRemovalTablePath = TablePath.of("database", "removed_table");
        FlussSplitBase split =
                new FlussLogSplit(assignedPartitionPath, new TableBucket(1L, 2L, 0), 42L);
        FlussSourceEnumState state =
                new FlussSourceEnumState(
                        Collections.singleton(assignedPartitionPath),
                        Collections.singletonList(split),
                        "lease-id",
                        Collections.singleton(pendingRemovalTablePath),
                        Collections.singleton(pendingRemovalPartitionPath));
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored.getAssignedPhysicalTablePaths()).containsExactly(assignedPartitionPath);
        assertThat(restored.getRemainingSplits()).containsExactly(split);
        assertThat(restored.getLeaseId()).isEqualTo("lease-id");
        assertThat(restored.getPendingRemovalTablePaths()).containsExactly(pendingRemovalTablePath);
        assertThat(restored.getPendingRemovalPartitionPaths())
                .containsExactly(pendingRemovalPartitionPath);
    }

    @Test
    void testDeserializeVersionOneStateWithoutPendingRemovals() throws Exception {
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored = serializer.deserialize(1, serializeLegacyState(1));

        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("database", "table"), "retained");
        assertThat(restored.getAssignedPhysicalTablePaths()).containsExactly(partitionPath);
        assertThat(restored.getRemainingSplits())
                .containsExactly(new FlussLogSplit(partitionPath, new TableBucket(1L, 2L, 0), 42L));
        assertThat(restored.getLeaseId()).isEqualTo("lease-id");
        assertThat(restored.getPendingRemovalTablePaths()).isEmpty();
        assertThat(restored.getPendingRemovalPartitionPaths()).isEmpty();
    }

    @Test
    void testDeserializeVersionTwoStateWithoutPendingPartitionRemovals() throws Exception {
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored = serializer.deserialize(2, serializeLegacyState(2));

        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("database", "table"), "retained");
        assertThat(restored.getAssignedPhysicalTablePaths()).containsExactly(partitionPath);
        assertThat(restored.getRemainingSplits())
                .containsExactly(new FlussLogSplit(partitionPath, new TableBucket(1L, 2L, 0), 42L));
        assertThat(restored.getLeaseId()).isEqualTo("lease-id");
        assertThat(restored.getPendingRemovalTablePaths())
                .containsExactly(TablePath.of("database", "removed_table"));
        assertThat(restored.getPendingRemovalPartitionPaths()).isEmpty();
    }

    @Test
    void testRejectUnknownStateVersion() {
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        assertThatThrownBy(() -> serializer.deserialize(4, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown Fluss source enumerator state version");
    }

    private static byte[] serializeLegacyState(int version) throws IOException {
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("database", "table"), "retained");
        FlussSplitBase split = new FlussLogSplit(partitionPath, new TableBucket(1L, 2L, 0), 42L);
        byte[] splitBytes = new FlussSplitSerializer().serialize(split);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(bytes)) {
            out.writeInt(1);
            out.writeUTF("database");
            out.writeUTF("table");
            out.writeBoolean(true);
            out.writeUTF("retained");
            out.writeInt(1);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
            out.writeUTF("lease-id");
            if (version >= 2) {
                out.writeInt(1);
                out.writeUTF("database");
                out.writeUTF("removed_table");
            }
            return bytes.toByteArray();
        }
    }
}
