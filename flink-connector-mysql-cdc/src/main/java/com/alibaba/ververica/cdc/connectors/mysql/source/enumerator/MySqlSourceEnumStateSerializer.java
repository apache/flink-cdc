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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of MySQL CDC source.
 */
public class MySqlSourceEnumStateSerializer
        implements SimpleVersionedSerializer<MySqlSourceEnumState> {

    private static final int VERSION = 1;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final SimpleVersionedSerializer<MySqlSplit> splitSerializer;

    public MySqlSourceEnumStateSerializer(SimpleVersionedSerializer<MySqlSplit> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MySqlSourceEnumState sourceEnumState) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (sourceEnumState.serializedFormCache != null) {
            return sourceEnumState.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeInt(splitSerializer.getVersion());
        writeMySqlSplits(sourceEnumState.getRemainingSplits(), out);
        writeTableIds(sourceEnumState.getAlreadyProcessedTables(), out);
        writeAssignedSplits(sourceEnumState.getAssignedSnapshotSplits(), out);
        writeFinishedSnapshotSplits(sourceEnumState.getFinishedSnapshotSplits(), out);
        out.writeBoolean(sourceEnumState.isBinlogSplitAssigned());

        final byte[] result = out.getCopyOfBuffer();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        sourceEnumState.serializedFormCache = result;
        out.clear();
        return result;
    }

    @Override
    public MySqlSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == VERSION) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private MySqlSourceEnumState deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final int splitVersion = in.readInt();
        final Collection<MySqlSplit> splits = readMySqlSplits(splitVersion, in);
        final Collection<TableId> tableIds = readTableIds(in);
        final Map<Integer, List<MySqlSnapshotSplit>> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        final Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits =
                readFinishedSnapshotSplits(in);
        boolean binlogSplitAssigned = in.readBoolean();
        return new MySqlSourceEnumState(
                splits,
                tableIds,
                assignedSnapshotSplits,
                finishedSnapshotSplits,
                binlogSplitAssigned);
    }

    private void writeFinishedSnapshotSplits(
            Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits,
            DataOutputSerializer out)
            throws IOException {
        final int size = finishedSnapshotSplits.size();
        out.writeInt(size);
        for (Map.Entry<Integer, Map<String, BinlogOffset>> entry :
                finishedSnapshotSplits.entrySet()) {
            int subtaskId = entry.getKey();
            out.writeInt(subtaskId);
            Map<String, BinlogOffset> splitsInfo = entry.getValue();
            writeSplitsInfo(splitsInfo, out);
        }
    }

    private Map<Integer, Map<String, BinlogOffset>> readFinishedSnapshotSplits(
            DataInputDeserializer in) throws IOException {
        Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int subtaskId = in.readInt();
            Map<String, BinlogOffset> splitsInfo = readSplitsInfo(in);
            finishedSnapshotSplits.put(subtaskId, splitsInfo);
        }
        return finishedSnapshotSplits;
    }

    private void writeSplitsInfo(Map<String, BinlogOffset> splitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = splitsInfo.size();
        out.writeInt(size);
        for (Map.Entry<String, BinlogOffset> splitInfo : splitsInfo.entrySet()) {
            out.writeUTF(splitInfo.getKey());
            writeBinlogPosition(splitInfo.getValue(), out);
        }
    }

    private Map<String, BinlogOffset> readSplitsInfo(DataInputDeserializer in) throws IOException {
        Map<String, BinlogOffset> splitsInfo = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            BinlogOffset binlogOffset = readBinlogPosition(in);
            splitsInfo.put(splitId, binlogOffset);
        }
        return splitsInfo;
    }

    private void writeAssignedSplits(
            Map<Integer, List<MySqlSnapshotSplit>> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<Integer, List<MySqlSnapshotSplit>> entry : assignedSplits.entrySet()) {
            int subtaskId = entry.getKey();
            List<MySqlSnapshotSplit> splits = entry.getValue();
            out.writeInt(subtaskId);
            writeMySqlSplits(splits, out);
        }
    }

    private Map<Integer, List<MySqlSnapshotSplit>> readAssignedSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        Map<Integer, List<MySqlSnapshotSplit>> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int subtaskId = in.readInt();
            List<MySqlSnapshotSplit> mySqlSplits =
                    readMySqlSplits(splitVersion, in).stream()
                            .map(MySqlSplit::asSnapshotSplit)
                            .collect(Collectors.toList());
            assignedSplits.put(subtaskId, mySqlSplits);
        }
        return assignedSplits;
    }

    private <T extends MySqlSplit> void writeMySqlSplits(
            Collection<T> mySqlSplits, DataOutputSerializer out) throws IOException {
        final int size = mySqlSplits.size();
        out.writeInt(size);
        for (MySqlSplit split : mySqlSplits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<MySqlSplit> readMySqlSplits(int splitVersion, DataInputDeserializer in)
            throws IOException {
        List<MySqlSplit> mySqlSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int splitBytesLen = in.readInt();
            byte[] splitBytes = new byte[splitBytesLen];
            in.read(splitBytes);
            MySqlSplit mySqlSplit = splitSerializer.deserialize(splitVersion, splitBytes);
            mySqlSplits.add(mySqlSplit);
        }
        return mySqlSplits;
    }

    private void writeTableIds(Collection<TableId> tableIds, DataOutputSerializer out)
            throws IOException {
        final int size = tableIds.size();
        out.writeInt(size);
        for (TableId tableId : tableIds) {
            out.writeUTF(tableId.toString());
        }
    }

    private Collection<TableId> readTableIds(DataInputDeserializer in) throws IOException {
        Collection<TableId> tableIds = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tableIdStr = in.readUTF();
            tableIds.add(TableId.parse(tableIdStr));
        }
        return tableIds;
    }
}
