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

package org.apache.flink.cdc.connectors.base.source.assigner.state;

import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer.readTableSchemas;
import static org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer.writeTableSchemas;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState}. The
 * modification of the 6th version:
 *
 * <p>1. Change isAssignerFinished(boolean) to assignStatus in SnapshotPendingSplitsState to
 * represent a more comprehensive assignment status.
 *
 * <p>2. Add streamSplitTaskId(int) to HybridPendingSplitsState, which represents the task ID
 * assigned to the stream split.
 *
 * <p>The modification of 8th version: add ChunkSplitterState to SnapshotPendingSplitsState, which
 * contains the asynchronously splitting chunk info.
 */
public class PendingSplitsStateSerializer implements SimpleVersionedSerializer<PendingSplitsState> {

    private static final int VERSION = 8;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int STREAM_PENDING_SPLITS_STATE_FLAG = 2;
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

        try {
            out.writeInt(splitSerializer.getVersion());
            if (state instanceof SnapshotPendingSplitsState) {
                out.writeInt(SNAPSHOT_PENDING_SPLITS_STATE_FLAG);
                serializeSnapshotPendingSplitsState((SnapshotPendingSplitsState) state, out);
            } else if (state instanceof StreamPendingSplitsState) {
                out.writeInt(STREAM_PENDING_SPLITS_STATE_FLAG);
                serializeStreamPendingSplitsState((StreamPendingSplitsState) state, out);
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

            return result;
        } finally {
            out.clear();
        }
    }

    @Override
    public PendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
            case 2:
                return deserializeLegacyPendingSplitsState(serialized);
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                return deserializePendingSplitsState(version, serialized);
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
        } else if (stateFlag == STREAM_PENDING_SPLITS_STATE_FLAG) {
            return deserializeStreamPendingSplitsState(in);
        } else {
            throw new IOException(
                    "Unsupported to deserialize PendingSplitsState flag: " + stateFlag);
        }
    }

    public PendingSplitsState deserializePendingSplitsState(int version, byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateFlag = in.readInt();
        if (stateFlag == SNAPSHOT_PENDING_SPLITS_STATE_FLAG) {
            return deserializeSnapshotPendingSplitsState(version, splitVersion, in);
        } else if (stateFlag == HYBRID_PENDING_SPLITS_STATE_FLAG) {
            return deserializeHybridPendingSplitsState(version, splitVersion, in);
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
            SnapshotPendingSplitsState state, DataOutputSerializer out) throws IOException {
        writeTableIds(state.getAlreadyProcessedTables(), out);
        writeRemainingSplits(state.getRemainingSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSplits(), out);
        writeFinishedOffsets(state.getSplitFinishedOffsets(), out);
        out.writeInt(state.getSnapshotAssignerStatus().getStatusCode());
        writeTableIds(state.getRemainingTables(), out);
        out.writeBoolean(state.isTableIdCaseSensitive());
        writeTableSchemas(state.getTableSchemas(), out);

        writeSplitFinishedCheckpointIds(state.getSplitFinishedCheckpointIds(), out);

        // The modification of 8th version: add ChunkSplitterState to SnapshotPendingSplitsState,
        // which contains the asynchronously splitting chunk info.
        boolean hasTableIsSplitting =
                state.getChunkSplitterState().getCurrentSplittingTableId() != null;
        out.writeBoolean(hasTableIsSplitting);
        if (hasTableIsSplitting) {
            ChunkSplitterState chunkSplitterState = state.getChunkSplitterState();
            out.writeUTF(chunkSplitterState.getCurrentSplittingTableId().toDoubleQuotedString());
            out.writeUTF(
                    SerializerUtils.rowToSerializedString(
                            new Object[] {chunkSplitterState.getNextChunkStart().getValue()}));
            out.writeInt(chunkSplitterState.getNextChunkId());
        }
    }

    private void serializeHybridPendingSplitsState(
            HybridPendingSplitsState state, DataOutputSerializer out) throws IOException {
        serializeSnapshotPendingSplitsState(state.getSnapshotPendingSplits(), out);
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    private void serializeStreamPendingSplitsState(
            StreamPendingSplitsState state, DataOutputSerializer out) throws IOException {
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    // ------------------------------------------------------------------------------------------
    // Deserialize
    // ------------------------------------------------------------------------------------------

    private SnapshotPendingSplitsState deserializeLegacySnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(2, in);
        List<SnapshotSplit> remainingSplits = readSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);

        final List<SchemalessSnapshotSplit> remainingSchemalessSplits = new ArrayList<>();
        final Map<String, SchemalessSnapshotSplit> assignedSchemalessSnapshotSplits =
                new HashMap<>();
        final Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        remainingSplits.forEach(
                split -> {
                    tableSchemas.putAll(split.getTableSchemas());
                    remainingSchemalessSplits.add(split.toSchemalessSnapshotSplit());
                });
        assignedSnapshotSplits
                .entrySet()
                .forEach(
                        entry -> {
                            tableSchemas.putAll(entry.getValue().getTableSchemas());
                            assignedSchemalessSnapshotSplits.put(
                                    entry.getKey(), entry.getValue().toSchemalessSnapshotSplit());
                        });
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        AssignerStatus assignerStatus;
        boolean isAssignerFinished = in.readBoolean();
        if (isAssignerFinished) {
            assignerStatus = AssignerStatus.INITIAL_ASSIGNING_FINISHED;
        } else {
            assignerStatus = AssignerStatus.INITIAL_ASSIGNING;
        }
        return new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSchemalessSplits,
                assignedSchemalessSnapshotSplits,
                tableSchemas,
                finishedOffsets,
                assignerStatus,
                new ArrayList<>(),
                false,
                false,
                new HashMap<>(),
                ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    private HybridPendingSplitsState deserializeLegacyHybridPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        boolean isStreamSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isStreamSplitAssigned);
    }

    private SnapshotPendingSplitsState deserializeSnapshotPendingSplitsState(
            int version, int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(version, in);
        List<SnapshotSplit> remainingSplits = readSnapshotSplits(splitVersion, in);
        Map<String, SnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, Offset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        AssignerStatus assignerStatus;
        if (version >= 6) {
            assignerStatus = AssignerStatus.fromStatusCode(in.readInt());
        } else {
            boolean isAssignerFinished = in.readBoolean();
            if (isAssignerFinished) {
                assignerStatus = AssignerStatus.INITIAL_ASSIGNING_FINISHED;
            } else {
                assignerStatus = AssignerStatus.INITIAL_ASSIGNING;
            }
        }
        List<TableId> remainingTableIds = readTableIds(version, in);
        boolean isTableIdCaseSensitive = in.readBoolean();
        final List<SchemalessSnapshotSplit> remainingSchemalessSplits = new ArrayList<>();
        final Map<String, SchemalessSnapshotSplit> assignedSchemalessSnapshotSplits =
                new HashMap<>();
        final Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        remainingSplits.forEach(
                split -> {
                    tableSchemas.putAll(split.getTableSchemas());
                    remainingSchemalessSplits.add(split.toSchemalessSnapshotSplit());
                });
        assignedSnapshotSplits
                .entrySet()
                .forEach(
                        entry -> {
                            tableSchemas.putAll(entry.getValue().getTableSchemas());
                            assignedSchemalessSnapshotSplits.put(
                                    entry.getKey(), entry.getValue().toSchemalessSnapshotSplit());
                        });
        if (version >= 4) {
            tableSchemas.putAll(readTableSchemas(splitVersion, in));
        }
        Map<String, Long> splitFinishedCheckpointIds = new HashMap<>();
        if (version >= 7) {
            splitFinishedCheckpointIds = readSplitFinishedCheckpointIds(in);
        }

        // The modification of 8th version: add ChunkSplitterState to SnapshotPendingSplitsState,
        // which contains the asynchronously splitting chunk info.
        ChunkSplitterState chunkSplitterState = ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
        if (version >= 8) {
            boolean hasTableIsSplitting = in.readBoolean();
            if (hasTableIsSplitting) {
                TableId splittingTableId = TableId.parse(in.readUTF());
                Object nextChunkStart = SerializerUtils.serializedStringToRow(in.readUTF())[0];
                Integer nextChunkId = in.readInt();
                chunkSplitterState =
                        new ChunkSplitterState(
                                splittingTableId,
                                ChunkSplitterState.ChunkBound.middleOf(nextChunkStart),
                                nextChunkId);
            }
        }
        return new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSchemalessSplits,
                assignedSchemalessSnapshotSplits,
                tableSchemas,
                finishedOffsets,
                assignerStatus,
                remainingTableIds,
                isTableIdCaseSensitive,
                true,
                splitFinishedCheckpointIds,
                chunkSplitterState);
    }

    private HybridPendingSplitsState deserializeHybridPendingSplitsState(
            int version, int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeSnapshotPendingSplitsState(version, splitVersion, in);
        boolean isStreamSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isStreamSplitAssigned);
    }

    private StreamPendingSplitsState deserializeStreamPendingSplitsState(DataInputDeserializer in)
            throws IOException {
        return new StreamPendingSplitsState(in.readBoolean());
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void writeSplitFinishedCheckpointIds(
            Map<String, Long> splitFinishedCheckpointIds, DataOutputSerializer out)
            throws IOException {
        final int size = splitFinishedCheckpointIds.size();
        out.writeInt(size);
        for (Map.Entry<String, Long> splitFinishedCheckpointId :
                splitFinishedCheckpointIds.entrySet()) {
            out.writeUTF(splitFinishedCheckpointId.getKey());
            out.writeLong(splitFinishedCheckpointId.getValue());
        }
    }

    private Map<String, Long> readSplitFinishedCheckpointIds(DataInputDeserializer in)
            throws IOException {
        Map<String, Long> splitFinishedCheckpointIds = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            Long checkpointId = in.readLong();
            splitFinishedCheckpointIds.put(splitId, checkpointId);
        }
        return splitFinishedCheckpointIds;
    }

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
            Offset offsetPosition = splitSerializer.readOffsetPosition(offsetVersion, in);
            splitsInfo.put(splitId, offsetPosition);
        }
        return splitsInfo;
    }

    private void writeAssignedSnapshotSplits(
            Map<String, SchemalessSnapshotSplit> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, SchemalessSnapshotSplit> entry : assignedSplits.entrySet()) {
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
            SnapshotSplit snapshotSplit = readSnapshotSplit(splitVersion, in).asSnapshotSplit();
            assignedSplits.put(splitId, snapshotSplit);
        }
        return assignedSplits;
    }

    private <T extends SourceSplitBase> void writeRemainingSplits(
            Collection<T> remainingSplits, DataOutputSerializer out) throws IOException {
        final int size = remainingSplits.size();
        out.writeInt(size);
        for (SourceSplitBase split : remainingSplits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<SnapshotSplit> readSnapshotSplits(int splitVersion, DataInputDeserializer in)
            throws IOException {
        List<SnapshotSplit> snapshotSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SnapshotSplit snapshotSplit = readSnapshotSplit(splitVersion, in).asSnapshotSplit();
            snapshotSplits.add(snapshotSplit);
        }
        return snapshotSplits;
    }

    private SourceSplitBase readSnapshotSplit(int splitVersion, DataInputDeserializer in)
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
            boolean useCatalogBeforeSchema = SerializerUtils.shouldUseCatalogBeforeSchema(tableId);
            out.writeBoolean(useCatalogBeforeSchema);
            out.writeUTF(tableId.toDoubleQuotedString());
        }
    }

    private List<TableId> readTableIds(int version, DataInputDeserializer in) throws IOException {
        List<TableId> tableIds = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            boolean useCatalogBeforeSchema = true;
            if (version >= 5) {
                useCatalogBeforeSchema = in.readBoolean();
            }
            String tableIdStr = in.readUTF();
            tableIds.add(TableId.parse(tableIdStr, useCatalogBeforeSchema));
        }
        return tableIds;
    }
}
