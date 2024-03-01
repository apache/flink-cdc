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

package org.apache.flink.cdc.connectors.base.source.assigner.state.version5;

import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.version4.LegacySourceSplitSerializierVersion4;
import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The 5th version of PendingSplitsStateSerializer. The modification of the 6th version: Change
 * isAssignerFinished(boolean) to assignStatus in SnapshotPendingSplitsState to represent a more
 * comprehensive assignment status.
 */
public class PendingSplitsStateSerializerVersion5 {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int STREAM_PENDING_SPLITS_STATE_FLAG = 2;
    private static final int HYBRID_PENDING_SPLITS_STATE_FLAG = 3;

    public static byte[] serialize(PendingSplitsState state) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeInt(5);

        if (state instanceof SnapshotPendingSplitsStateVersion5) {
            out.writeInt(SNAPSHOT_PENDING_SPLITS_STATE_FLAG);
            serializeSnapshotPendingSplitsState((SnapshotPendingSplitsStateVersion5) state, out);
        } else if (state instanceof HybridPendingSplitsStateVersion5) {
            out.writeInt(HYBRID_PENDING_SPLITS_STATE_FLAG);
            serializeHybridPendingSplitsState((HybridPendingSplitsStateVersion5) state, out);
        } else if (state instanceof StreamPendingSplitsState) {
            out.writeInt(STREAM_PENDING_SPLITS_STATE_FLAG);
            serializeStreamPendingSplitsState((StreamPendingSplitsState) state, out);
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    private static void serializeHybridPendingSplitsState(
            HybridPendingSplitsStateVersion5 state, DataOutputSerializer out) throws IOException {
        serializeSnapshotPendingSplitsState(state.getSnapshotPendingSplits(), out);
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    private static void serializeSnapshotPendingSplitsState(
            SnapshotPendingSplitsStateVersion5 state, DataOutputSerializer out) throws IOException {
        writeTableIds(state.getAlreadyProcessedTables(), out);
        writeRemainingSplits(state.getRemainingSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSplits(), out);
        writeFinishedOffsets(state.getSplitFinishedOffsets(), out);
        out.writeBoolean(state.isAssignerFinished());
        writeTableIds(state.getRemainingTables(), out);
        out.writeBoolean(state.isTableIdCaseSensitive());
        writeTableSchemas(state.getTableSchemas(), out);
    }

    private static void serializeStreamPendingSplitsState(
            StreamPendingSplitsState state, DataOutputSerializer out) throws IOException {
        out.writeBoolean(state.isStreamSplitAssigned());
    }

    private static void writeTableIds(Collection<TableId> tableIds, DataOutputSerializer out)
            throws IOException {
        final int size = tableIds.size();
        out.writeInt(size);
        for (TableId tableId : tableIds) {
            boolean useCatalogBeforeSchema = SerializerUtils.shouldUseCatalogBeforeSchema(tableId);
            out.writeBoolean(useCatalogBeforeSchema);
            out.writeUTF(tableId.toString());
        }
    }

    private static void writeRemainingSplits(
            List<SchemalessSnapshotSplit> remainingSplits, DataOutputSerializer out)
            throws IOException {
        final int size = remainingSplits.size();
        out.writeInt(size);
        for (SchemalessSnapshotSplit split : remainingSplits) {
            byte[] splitBytes = LegacySourceSplitSerializierVersion4.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private static void writeAssignedSnapshotSplits(
            Map<String, SchemalessSnapshotSplit> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, SchemalessSnapshotSplit> entry : assignedSplits.entrySet()) {
            out.writeUTF(entry.getKey());
            byte[] splitBytes = LegacySourceSplitSerializierVersion4.serialize(entry.getValue());
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private static void writeFinishedOffsets(
            Map<String, Offset> splitsInfo, DataOutputSerializer out) throws IOException {
        final int size = splitsInfo.size();
        out.writeInt(size);
        for (Map.Entry<String, Offset> splitInfo : splitsInfo.entrySet()) {
            out.writeUTF(splitInfo.getKey());
            LegacySourceSplitSerializierVersion4.writeOffsetPosition(splitInfo.getValue(), out);
        }
    }

    private static void writeTableSchemas(
            Map<TableId, TableChanges.TableChange> tableSchemas, DataOutputSerializer out)
            throws IOException {
        FlinkJsonTableChangeSerializer jsonSerializer = new FlinkJsonTableChangeSerializer();
        DocumentWriter documentWriter = DocumentWriter.defaultWriter();
        final int size = tableSchemas.size();
        out.writeInt(size);
        for (Map.Entry<TableId, TableChanges.TableChange> entry : tableSchemas.entrySet()) {
            boolean useCatalogBeforeSchema =
                    SerializerUtils.shouldUseCatalogBeforeSchema(entry.getKey());
            out.writeBoolean(useCatalogBeforeSchema);
            out.writeUTF(entry.getKey().toString());
            final String tableChangeStr =
                    documentWriter.write(jsonSerializer.toDocument(entry.getValue()));
            final byte[] tableChangeBytes = tableChangeStr.getBytes(StandardCharsets.UTF_8);
            out.writeInt(tableChangeBytes.length);
            out.write(tableChangeBytes);
        }
    }
}
