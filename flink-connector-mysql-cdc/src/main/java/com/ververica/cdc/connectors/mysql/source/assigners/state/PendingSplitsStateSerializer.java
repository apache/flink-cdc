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

package com.ververica.cdc.connectors.mysql.source.assigners.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer.readTableSchemas;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer.writeTableSchemas;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.rowToSerializedString;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState} of MySQL CDC
 * source.
 */
public class PendingSplitsStateSerializer implements SimpleVersionedSerializer<PendingSplitsState> {

    private static final int VERSION = 5;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int BINLOG_PENDING_SPLITS_STATE_FLAG = 2;
    private static final int HYBRID_PENDING_SPLITS_STATE_FLAG = 3;

    private final SimpleVersionedSerializer<MySqlSplit> splitSerializer;

    public PendingSplitsStateSerializer(SimpleVersionedSerializer<MySqlSplit> splitSerializer) {
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
        } else if (state instanceof BinlogPendingSplitsState) {
            out.writeInt(BINLOG_PENDING_SPLITS_STATE_FLAG);
            serializeBinlogPendingSplitsState((BinlogPendingSplitsState) state, out);
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
            case 4:
            case 5:
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
        } else if (stateFlag == BINLOG_PENDING_SPLITS_STATE_FLAG) {
            return deserializeBinlogPendingSplitsState(in);
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
        writeRemainingSplits(state.getRemainingSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSplits(), out);
        writeFinishedOffsets(state.getSplitFinishedOffsets(), out);
        out.writeInt(state.getSnapshotAssignerStatus().getStatusCode());
        writeTableIds(state.getRemainingTables(), out);
        out.writeBoolean(state.isTableIdCaseSensitive());
        writeTableSchemas(state.getTableSchemas(), out);

        boolean hasTableIsSplitting =
                state.getChunkSplitterState().getCurrentSplittingTableId() != null;
        out.writeBoolean(hasTableIsSplitting);
        if (hasTableIsSplitting) {
            ChunkSplitterState chunkSplitterState = state.getChunkSplitterState();
            out.writeUTF(chunkSplitterState.getCurrentSplittingTableId().toString());
            out.writeUTF(
                    rowToSerializedString(
                            new Object[] {chunkSplitterState.getNextChunkStart().getValue()}));
            out.writeInt(chunkSplitterState.getNextChunkId());
        }
    }

    private void serializeHybridPendingSplitsState(
            HybridPendingSplitsState state, DataOutputSerializer out) throws IOException {
        serializeSnapshotPendingSplitsState(state.getSnapshotPendingSplits(), out);
        out.writeBoolean(state.isBinlogSplitAssigned());
    }

    private void serializeBinlogPendingSplitsState(
            BinlogPendingSplitsState state, DataOutputSerializer out) throws IOException {
        out.writeBoolean(state.isBinlogSplitAssigned());
    }

    // ------------------------------------------------------------------------------------------
    // Deserialize
    // ------------------------------------------------------------------------------------------

    private SnapshotPendingSplitsState deserializeLegacySnapshotPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(in);
        List<MySqlSnapshotSplit> remainingSplits = readMySqlSnapshotSplits(splitVersion, in);
        Map<String, MySqlSnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);

        final List<MySqlSchemalessSnapshotSplit> remainingSchemalessSplits = new ArrayList<>();
        final Map<String, MySqlSchemalessSnapshotSplit> assignedSchemalessSnapshotSplits =
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
        Map<String, BinlogOffset> finishedOffsets = readFinishedOffsets(splitVersion, in);
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
                NO_SPLITTING_TABLE_STATE);
    }

    private HybridPendingSplitsState deserializeLegacyHybridPendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeLegacySnapshotPendingSplitsState(splitVersion, in);
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isBinlogSplitAssigned);
    }

    private SnapshotPendingSplitsState deserializeSnapshotPendingSplitsState(
            int version, int splitVersion, DataInputDeserializer in) throws IOException {
        List<TableId> alreadyProcessedTables = readTableIds(in);
        List<MySqlSnapshotSplit> remainingSplits = readMySqlSnapshotSplits(splitVersion, in);
        Map<String, MySqlSnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        Map<String, BinlogOffset> finishedOffsets = readFinishedOffsets(splitVersion, in);
        AssignerStatus assignerStatus;
        if (version >= 3) {
            assignerStatus = AssignerStatus.fromStatusCode(in.readInt());
        } else {
            boolean isAssignerFinished = in.readBoolean();
            if (isAssignerFinished) {
                assignerStatus = AssignerStatus.INITIAL_ASSIGNING_FINISHED;
            } else {
                assignerStatus = AssignerStatus.INITIAL_ASSIGNING;
            }
        }
        List<TableId> remainingTableIds = readTableIds(in);
        boolean isTableIdCaseSensitive = in.readBoolean();
        final List<MySqlSchemalessSnapshotSplit> remainingSchemalessSplits = new ArrayList<>();
        final Map<String, MySqlSchemalessSnapshotSplit> assignedSchemalessSnapshotSplits =
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

        TableId splittingTableId = null;
        Object nextChunkStart = null;
        Integer nextChunkId = null;
        if (version > 4) {
            boolean hasTableIsSplitting = in.readBoolean();
            if (hasTableIsSplitting) {
                splittingTableId = TableId.parse(in.readUTF());
                nextChunkStart = serializedStringToRow(in.readUTF())[0];
                nextChunkId = in.readInt();
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
                splittingTableId == null
                        ? NO_SPLITTING_TABLE_STATE
                        : new ChunkSplitterState(
                                splittingTableId,
                                ChunkSplitterState.ChunkBound.middleOf(nextChunkStart),
                                nextChunkId));
    }

    private HybridPendingSplitsState deserializeHybridPendingSplitsState(
            int version, int splitVersion, DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotPendingSplitsState =
                deserializeSnapshotPendingSplitsState(version, splitVersion, in);
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotPendingSplitsState, isBinlogSplitAssigned);
    }

    private BinlogPendingSplitsState deserializeBinlogPendingSplitsState(DataInputDeserializer in)
            throws IOException {
        return new BinlogPendingSplitsState(in.readBoolean());
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void writeFinishedOffsets(
            Map<String, BinlogOffset> splitsInfo, DataOutputSerializer out) throws IOException {
        final int size = splitsInfo.size();
        out.writeInt(size);
        for (Map.Entry<String, BinlogOffset> splitInfo : splitsInfo.entrySet()) {
            out.writeUTF(splitInfo.getKey());
            writeBinlogPosition(splitInfo.getValue(), out);
        }
    }

    private Map<String, BinlogOffset> readFinishedOffsets(
            int offsetVersion, DataInputDeserializer in) throws IOException {
        Map<String, BinlogOffset> splitsInfo = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            BinlogOffset binlogOffset = readBinlogPosition(offsetVersion, in);
            splitsInfo.put(splitId, binlogOffset);
        }
        return splitsInfo;
    }

    private void writeAssignedSnapshotSplits(
            Map<String, MySqlSchemalessSnapshotSplit> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, MySqlSchemalessSnapshotSplit> entry : assignedSplits.entrySet()) {
            out.writeUTF(entry.getKey());
            byte[] splitBytes = splitSerializer.serialize(entry.getValue());
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private Map<String, MySqlSnapshotSplit> readAssignedSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        Map<String, MySqlSnapshotSplit> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            MySqlSnapshotSplit mySqlSplit = readMySqlSplit(splitVersion, in).asSnapshotSplit();
            assignedSplits.put(splitId, mySqlSplit);
        }
        return assignedSplits;
    }

    private <T extends MySqlSplit> void writeRemainingSplits(
            Collection<T> mySqlSplits, DataOutputSerializer out) throws IOException {
        final int size = mySqlSplits.size();
        out.writeInt(size);
        for (MySqlSplit split : mySqlSplits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<MySqlSnapshotSplit> readMySqlSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<MySqlSnapshotSplit> mySqlSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            MySqlSnapshotSplit mySqlSplit = readMySqlSplit(splitVersion, in).asSnapshotSplit();
            mySqlSplits.add(mySqlSplit);
        }
        return mySqlSplits;
    }

    private MySqlSplit readMySqlSplit(int splitVersion, DataInputDeserializer in)
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
