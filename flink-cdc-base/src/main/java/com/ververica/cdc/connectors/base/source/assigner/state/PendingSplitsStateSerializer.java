/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.source.assigner.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import io.debezium.schema.DataCollectionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState}. */
public class PendingSplitsStateSerializer<ID extends DataCollectionId, S>
        implements SimpleVersionedSerializer<PendingSplitsState<ID, S>> {

    private static final int VERSION = 4;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int STREAM_PENDING_SPLITS_STATE_FLAG = 2;
    private static final int HYBRID_PENDING_SPLITS_STATE_FLAG = 3;

    private final SourceSplitSerializer<ID, S> splitSerializer;

    public PendingSplitsStateSerializer(SourceSplitSerializer<ID, S> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsState<ID, S> state) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeInt(splitSerializer.getVersion());
        if (state instanceof SnapshotPendingSplitsState) {
            out.writeInt(SNAPSHOT_PENDING_SPLITS_STATE_FLAG);
            serializeSnapshotPendingSplitsState((SnapshotPendingSplitsState<ID, S>) state, out);
        } else if (state instanceof StreamPendingSplitsState) {
            out.writeInt(STREAM_PENDING_SPLITS_STATE_FLAG);
            serializeStreamPendingSplitsState((StreamPendingSplitsState<ID, S>) state, out);
        } else if (state instanceof HybridPendingSplitsState) {
            out.writeInt(HYBRID_PENDING_SPLITS_STATE_FLAG);
            serializeHybridPendingSplitsState((HybridPendingSplitsState<ID, S>) state, out);
        } else {
            throw new IOException(
                    "Unsupported to serialize PendingSplitsState class: "
                            + state.getClass().getName());
        }

        final byte[] result = out.getCopyOfBuffer();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        state.serializedFormCache = result;
        out.clear();
        return result;
    }

    @Override
    public PendingSplitsState<ID, S> deserialize(int version, byte[] serialized)
            throws IOException {
        switch (version) {
            case 1:
            case 2:
                return deserializeLegacyPendingSplitsState(serialized);
            case 3:
                return deserializePendingSplitsStateV3(serialized);
            case 4:
                return deserializePendingSplitsStateV4(serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public PendingSplitsState<ID, S> deserializeLegacyPendingSplitsState(byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeLegacyHybridPendingSplitsState(splitVersion, in);
        } else if (stateFlag == STREAM_PENDING_SPLITS_STATE_FLAG) {
            return deserializeStreamPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    public PendingSplitsState<ID, S> deserializePendingSplitsStateV4(byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeSnapshotPendingSplitsState(splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeHybridPendingSplitsStateV4(splitVersion, in);
        } else if (stateFlag == STREAM_PENDING_SPLITS_STATE_FLAG) {
            return deserializeStreamPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    public PendingSplitsState<ID, S> deserializePendingSplitsStateV3(byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeSnapshotPendingSplitsState(splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeHybridPendingSplitsStateV3(splitVersion, in);
        } else if (stateFlag == STREAM_PENDING_SPLITS_STATE_FLAG) {
            return deserializeStreamPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Serialize
    // ------------------------------------------------------------------------------------------

    private void serializeSnapshotPendingSplitsState(
            SnapshotPendingSplitsState<ID, S> state, DataOutputSerializer out) throws IOException {
        writeTableIds(state.getAlreadyProcessedTables(), out);
        writeSourceSplits(state.getRemainingSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSplits(), out);
        writeFinishedOffsets(state.getSplitFinishedOffsets(), out);
        out.writeBoolean(state.isAssignerFinished());
        writeTableIds(state.getRemainingTables(), out);
        out.writeBoolean(state.isTableIdCaseSensitive());
    }

    private void serializeHybridPendingSplitsState(
            HybridPendingSplitsState<ID, S> state, DataOutputSerializer out) throws IOException {
        serializeSnapshotPendingSplitsState(state.getSnapshotPendingSplits(), out);
        out.writeBoolean(state.isStreamSplitAssigned());
        splitSerializer.writeOffsetPosition(state.getStartupOffset(), out);
    }

    private void serializeStreamPendingSplitsState(
            StreamPendingSplitsState<ID, S> state, DataOutputSerializer out) throws IOException {
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    // ------------------------------------------------------------------------------------------
    // Deserialize
    // ------------------------------------------------------------------------------------------

    private SnapshotPendingSplitsState<ID, S> deserializeLegacySnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<ID> alreadyProcessedTables = readTableIds(in);
        List<SnapshotSplit<ID, S>> remainingSplits = readSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit<ID, S>> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        boolean isAssignerFinished = in.readBoolean();
        return new SnapshotPendingSplitsState<>(
                alreadyProcessedTables,
                remainingSplits,
                assignedSnapshotSplits,
                finishedOffsets,
                isAssignerFinished,
                new ArrayList<>(),
                false,
                false);
    }

    private HybridPendingSplitsState<ID, S> deserializeLegacyHybridPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState<ID, S> snapshotPendingSplitsState =
                deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        boolean isStreamSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState<>(
                snapshotPendingSplitsState, isStreamSplitAssigned, null);
    }

    private SnapshotPendingSplitsState<ID, S> deserializeSnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<ID> alreadyProcessedTables = readTableIds(in);
        List<SnapshotSplit<ID, S>> remainingSplits = readSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit<ID, S>> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        boolean isAssignerFinished = in.readBoolean();
        List<ID> remainingTableIds = readTableIds(in);
        boolean isTableIdCaseSensitive = in.readBoolean();
        return new SnapshotPendingSplitsState<>(
                alreadyProcessedTables,
                remainingSplits,
                assignedSnapshotSplits,
                finishedOffsets,
                isAssignerFinished,
                remainingTableIds,
                isTableIdCaseSensitive,
                true);
    }

    private HybridPendingSplitsState<ID, S> deserializeHybridPendingSplitsStateV4(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState<ID, S> snapshotPendingSplitsState =
                deserializeSnapshotPendingSplitsState(splitVersion, in);
        boolean isStreamSplitAssigned = in.readBoolean();
        Offset initialOffset = splitSerializer.readOffsetPosition(splitVersion, in);
        return new HybridPendingSplitsState<>(
                snapshotPendingSplitsState, isStreamSplitAssigned, initialOffset);
    }

    private HybridPendingSplitsState<ID, S> deserializeHybridPendingSplitsStateV3(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState<ID, S> snapshotPendingSplitsState =
                deserializeSnapshotPendingSplitsState(splitVersion, in);
        boolean isStreamSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState<>(
                snapshotPendingSplitsState, isStreamSplitAssigned, null);
    }

    private StreamPendingSplitsState<ID, S> deserializeStreamPendingSplitsState(
            DataInputDeserializer in) throws IOException {
        return new StreamPendingSplitsState<>(in.readBoolean());
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void writeFinishedOffsets(Map<String, Offset> splitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = splitsInfo.size();
        out.writeInt(size);
        for (Map.Entry<String, Offset> splitInfo : splitsInfo.entrySet()) {
            out.writeUTF(splitInfo.getKey());
            splitSerializer.writeOffsetPosition(splitInfo.getValue(), out);
        }
    }

    private Map<String, Offset> readFinishedOffsets(int offsetVersion, DataInputDeserializer in)
            throws IOException {
        Map<String, Offset> splitsInfo = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            Offset offset = splitSerializer.readOffsetPosition(offsetVersion, in);
            //            Offset binlogOffset = readBnlogPosition(offsetVersion, in);
            splitsInfo.put(splitId, offset);
        }
        return splitsInfo;
    }

    private void writeAssignedSnapshotSplits(
            Map<String, SnapshotSplit<ID, S>> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, SnapshotSplit<ID, S>> entry : assignedSplits.entrySet()) {
            out.writeUTF(entry.getKey());
            byte[] splitBytes = splitSerializer.serialize(entry.getValue());
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private Map<String, SnapshotSplit<ID, S>> readAssignedSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        Map<String, SnapshotSplit<ID, S>> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            SnapshotSplit<ID, S> snapshotSplit =
                    readSourceSplit(splitVersion, in).asSnapshotSplit();
            assignedSplits.put(splitId, snapshotSplit);
        }
        return assignedSplits;
    }

    private <T extends SourceSplitBase<ID, S>> void writeSourceSplits(
            Collection<T> sourceSplits, DataOutputSerializer out) throws IOException {
        final int size = sourceSplits.size();
        out.writeInt(size);
        for (SourceSplitBase<ID, S> split : sourceSplits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<SnapshotSplit<ID, S>> readSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<SnapshotSplit<ID, S>> snapshotSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SnapshotSplit<ID, S> snapshotSplit =
                    readSourceSplit(splitVersion, in).asSnapshotSplit();
            snapshotSplits.add(snapshotSplit);
        }
        return snapshotSplits;
    }

    private SourceSplitBase<ID, S> readSourceSplit(int splitVersion, DataInputDeserializer in)
            throws IOException {
        int splitBytesLen = in.readInt();
        byte[] splitBytes = new byte[splitBytesLen];
        in.read(splitBytes);
        return splitSerializer.deserialize(splitVersion, splitBytes);
    }

    private void writeTableIds(Collection<ID> tableIds, DataOutputSerializer out)
            throws IOException {
        final int size = tableIds.size();
        out.writeInt(size);
        for (ID tableId : tableIds) {
            out.writeUTF(tableId.identifier());
        }
    }

    private List<ID> readTableIds(DataInputDeserializer in) throws IOException {
        List<ID> tableIds = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tableIdStr = in.readUTF();
            tableIds.add(splitSerializer.parseTableId(tableIdStr));
        }
        return tableIds;
    }
}
