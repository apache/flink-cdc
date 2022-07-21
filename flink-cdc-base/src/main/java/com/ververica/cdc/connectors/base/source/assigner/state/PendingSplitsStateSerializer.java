/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState} of MySQL CDC
 * source.
 */
public class PendingSplitsStateSerializer implements SimpleVersionedSerializer<PendingSplitsState> {

    private static final int VERSION = 3;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int BINLOG_PENDING_SPLITS_STATE_FLAG = 2;
    private static final int HYBRID_PENDING_SPLITS_STATE_FLAG = 3;

    private final SourceSplitSerializer splitSerializer;

    public PendingSplitsStateSerializer(SourceSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsState state) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeInt(splitSerializer.getVersion());
        if (state instanceof SnapshotPendingSplitsState) {
            out.writeInt(SNAPSHOT_PENDING_SPLITS_STATE_FLAG);
            serializeSnapshotPendingSplitsState((SnapshotPendingSplitsState) state, out);
        } else if (state instanceof StreamPendingSplitsState) {
            out.writeInt(BINLOG_PENDING_SPLITS_STATE_FLAG);
            serializeBinlogPendingSplitsState((StreamPendingSplitsState) state, out);
        } else if (state instanceof HybridPendingSplitsState) {
            out.writeInt(HYBRID_PENDING_SPLITS_STATE_FLAG);
            serializeHybridPendingSplitsState((HybridPendingSplitsState) state, out);
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
    public PendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
            case 2:
                return deserializeLegacyPendingSplitsState(serialized);
            case 3:
                return deserializePendingSplitsState(serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public PendingSplitsState deserializeLegacyPendingSplitsState(byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeLegacyHybridPendingSplitsState(splitVersion, in);
        } else if (stateFlag == BINLOG_PENDING_SPLITS_STATE_FLAG) {
            return deserializeBinlogPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    public PendingSplitsState deserializePendingSplitsState(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeSnapshotPendingSplitsState(splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeHybridPendingSplitsState(splitVersion, in);
        } else if (stateFlag == BINLOG_PENDING_SPLITS_STATE_FLAG) {
            return deserializeBinlogPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Serialize
    // ------------------------------------------------------------------------------------------

    private void serializeSnapshotPendingSplitsState(
            SnapshotPendingSplitsState state, DataOutputSerializer out) throws IOException {
        writeTableIds(state.getAlreadyProcessedTables(), out);
        writeMySqlSplits(state.getRemainingSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSplits(), out);
        writeFinishedOffsets(state.getSplitFinishedOffsets(), out);
        out.writeBoolean(state.isAssignerFinished());
        writeTableIds(state.getRemainingTables(), out);
        out.writeBoolean(state.isTableIdCaseSensitive());
    }

    private void serializeHybridPendingSplitsState(
            HybridPendingSplitsState state, DataOutputSerializer out) throws IOException {
        serializeSnapshotPendingSplitsState(state.getSnapshotPendingSplits(), out);
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    private void serializeBinlogPendingSplitsState(
            StreamPendingSplitsState state, DataOutputSerializer out) throws IOException {
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    // ------------------------------------------------------------------------------------------
    // Deserialize
    // ------------------------------------------------------------------------------------------

    private SnapshotPendingSplitsState deserializeLegacySnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(in);
        List<SnapshotSplit> remainingSplits = readMySqlSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        boolean isAssignerFinished = in.readBoolean();
        return new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSplits,
                assignedSnapshotSplits,
                finishedOffsets,
                isAssignerFinished,
                new ArrayList<>(),
                false,
                false);
    }

    private HybridPendingSplitsState deserializeLegacyHybridPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isBinlogSplitAssigned);
    }

    private SnapshotPendingSplitsState deserializeSnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(in);
        List<SnapshotSplit> remainingSplits = readMySqlSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        boolean isAssignerFinished = in.readBoolean();
        List<TableId> remainingTableIds = readTableIds(in);
        boolean isTableIdCaseSensitive = in.readBoolean();
        return new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSplits,
                assignedSnapshotSplits,
                finishedOffsets,
                isAssignerFinished,
                remainingTableIds,
                isTableIdCaseSensitive,
                true);
    }

    private HybridPendingSplitsState deserializeHybridPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeSnapshotPendingSplitsState(splitVersion, in);
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isBinlogSplitAssigned);
    }

    private StreamPendingSplitsState deserializeBinlogPendingSplitsState(DataInputDeserializer in)
            throws IOException {
        return new StreamPendingSplitsState(in.readBoolean());
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
            Offset binlogOffset = splitSerializer.readOffsetPosition(offsetVersion, in);
            //            Offset binlogOffset = readBinlogPosition(offsetVersion, in);
            splitsInfo.put(splitId, binlogOffset);
        }
        return splitsInfo;
    }

    private void writeAssignedSnapshotSplits(
            Map<String, SnapshotSplit> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, SnapshotSplit> entry : assignedSplits.entrySet()) {
            out.writeUTF(entry.getKey());
            byte[] splitBytes = splitSerializer.serialize(entry.getValue());
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private Map<String, SnapshotSplit> readAssignedSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            SnapshotSplit mySqlSplit = readMySqlSplit(splitVersion, in).asSnapshotSplit();
            assignedSplits.put(splitId, mySqlSplit);
        }
        return assignedSplits;
    }

    private <T extends SourceSplitBase> void writeMySqlSplits(
            Collection<T> mySqlSplits, DataOutputSerializer out) throws IOException {
        final int size = mySqlSplits.size();
        out.writeInt(size);
        for (SourceSplitBase split : mySqlSplits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<SnapshotSplit> readMySqlSnapshotSplits(int splitVersion, DataInputDeserializer in)
            throws IOException {
        List<SnapshotSplit> mySqlSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SnapshotSplit mySqlSplit = readMySqlSplit(splitVersion, in).asSnapshotSplit();
            mySqlSplits.add(mySqlSplit);
        }
        return mySqlSplits;
    }

    private SourceSplitBase readMySqlSplit(int splitVersion, DataInputDeserializer in)
            throws IOException {
        int splitBytesLen = in.readInt();
        byte[] splitBytes = new byte[splitBytesLen];
        in.read(splitBytes);
        return splitSerializer.deserialize(splitVersion, splitBytes);
    }

    private void writeTableIds(Collection<TableId> tableIds, DataOutputSerializer out)
            throws IOException {
        final int size = tableIds.size();
        out.writeInt(size);
        for (TableId tableId : tableIds) {
            out.writeUTF(tableId.toString());
        }
    }

    private List<TableId> readTableIds(DataInputDeserializer in) throws IOException {
        List<TableId> tableIds = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tableIdStr = in.readUTF();
            tableIds.add(TableId.parse(tableIdStr));
        }
        return tableIds;
    }
}
