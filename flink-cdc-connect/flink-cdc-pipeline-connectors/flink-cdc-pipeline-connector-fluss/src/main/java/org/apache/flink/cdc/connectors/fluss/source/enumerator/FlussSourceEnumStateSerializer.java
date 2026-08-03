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

import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Serializer for {@link FlussSourceEnumState}. */
public class FlussSourceEnumStateSerializer
        implements SimpleVersionedSerializer<FlussSourceEnumState> {

    private static final int VERSION = 1;
    private final FlussSplitSerializer splitSerializer = new FlussSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FlussSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            // Serialize assigned physical table paths
            Set<PhysicalTablePath> assignedPaths = state.getAssignedPhysicalTablePaths();
            out.writeInt(assignedPaths.size());
            for (PhysicalTablePath path : assignedPaths) {
                out.writeUTF(path.getDatabaseName());
                out.writeUTF(path.getTableName());
                out.writeBoolean(path.getPartitionName() != null);
                if (path.getPartitionName() != null) {
                    out.writeUTF(path.getPartitionName());
                }
            }
            // Serialize remaining splits
            List<FlussSplitBase> remaining = state.getRemainingSplits();
            out.writeInt(remaining.size());
            for (FlussSplitBase split : remaining) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public FlussSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {
            int pathCount = in.readInt();
            Set<PhysicalTablePath> assignedPaths = new LinkedHashSet<>();
            for (int i = 0; i < pathCount; i++) {
                String db = in.readUTF();
                String table = in.readUTF();
                boolean hasPartition = in.readBoolean();
                String partitionName = hasPartition ? in.readUTF() : null;
                assignedPaths.add(PhysicalTablePath.of(new TablePath(db, table), partitionName));
            }
            int splitCount = in.readInt();
            List<FlussSplitBase> remaining = new ArrayList<>();
            for (int i = 0; i < splitCount; i++) {
                int len = in.readInt();
                byte[] splitBytes = new byte[len];
                in.readFully(splitBytes);
                remaining.add(
                        splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
            }
            return new FlussSourceEnumState(assignedPaths, remaining);
        }
    }
}
