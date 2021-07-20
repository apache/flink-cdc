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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    private static final ThreadLocal<DataInputDeserializer> DESERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataInputDeserializer());
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

        writeMySQLSplits(sourceEnumState.getRemainingSplits(), out);
        writeTableIds(sourceEnumState.getAlreadyProcessedTables(), out);
        writeAssignedSplits(sourceEnumState.getAssignedSplits(), out);
        writeFinishedSnapshotSplits(sourceEnumState.getFinishedSnapshotSplits(), out);

        final byte[] result = out.getCopyOfBuffer();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        sourceEnumState.serializedFormCache = result;
        return result;
    }

    @Override
    public MySqlSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private MySqlSourceEnumState deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = DESERIALIZER_CACHE.get();
        in.setBuffer(serialized);

        final Collection<MySqlSplit> splits = readMySQLSplits(in);
        final Collection<TableId> tableIds = readTableIds(in);
        final Map<Integer, List<MySqlSplit>> assignedSplits = readAssignedSplits(in);
        final Map<Integer, List<Tuple2<String, BinlogOffset>>> finishedSnapshotSplits =
                readFinishedSnapshotSplits(in);
        in.releaseArrays();
        return new MySqlSourceEnumState(splits, tableIds, assignedSplits, finishedSnapshotSplits);
    }

    private void writeFinishedSnapshotSplits(
            Map<Integer, List<Tuple2<String, BinlogOffset>>> finishedSnapshotSplits,
            DataOutputSerializer out)
            throws IOException {
        final int size = finishedSnapshotSplits.size();
        out.writeInt(size);
        for (Map.Entry<Integer, List<Tuple2<String, BinlogOffset>>> entry :
                finishedSnapshotSplits.entrySet()) {
            int subtaskId = entry.getKey();
            out.writeInt(subtaskId);
            List<Tuple2<String, BinlogOffset>> splitsInfo = entry.getValue();
            writeSplitsInfo(splitsInfo, out);
        }
    }

    private Map<Integer, List<Tuple2<String, BinlogOffset>>> readFinishedSnapshotSplits(
            DataInputDeserializer in) throws IOException {
        Map<Integer, List<Tuple2<String, BinlogOffset>>> finishedSnapshotSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int subtaskId = in.readInt();
            List<Tuple2<String, BinlogOffset>> splitsInfo = readSplitsInfo(in);
            finishedSnapshotSplits.put(subtaskId, splitsInfo);
        }
        return finishedSnapshotSplits;
    }

    private void writeSplitsInfo(
            List<Tuple2<String, BinlogOffset>> splitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = splitsInfo.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            Tuple2<String, BinlogOffset> splitInfo = splitsInfo.get(i);
            out.writeUTF(splitInfo.f0);
            writeBinlogPosition(splitInfo.f1, out);
        }
    }

    private List<Tuple2<String, BinlogOffset>> readSplitsInfo(DataInputDeserializer in)
            throws IOException {
        List<Tuple2<String, BinlogOffset>> splitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            BinlogOffset binlogOffset = readBinlogPosition(in);
            splitsInfo.add(Tuple2.of(splitId, binlogOffset));
        }
        return splitsInfo;
    }

    private void writeAssignedSplits(
            Map<Integer, List<MySqlSplit>> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<Integer, List<MySqlSplit>> entry : assignedSplits.entrySet()) {
            int subtaskId = entry.getKey();
            List<MySqlSplit> splits = entry.getValue();
            out.writeInt(subtaskId);
            writeMySQLSplits(splits, out);
        }
    }

    private Map<Integer, List<MySqlSplit>> readAssignedSplits(DataInputDeserializer in)
            throws IOException {
        Map<Integer, List<MySqlSplit>> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int subtaskId = in.readInt();
            List<MySqlSplit> mySqlSplits = (List<MySqlSplit>) readMySQLSplits(in);
            assignedSplits.put(subtaskId, mySqlSplits);
        }
        return assignedSplits;
    }

    private void writeMySQLSplits(Collection<MySqlSplit> mySqlSplits, DataOutputSerializer out)
            throws IOException {
        final int size = mySqlSplits.size();
        out.writeInt(size);
        final Iterator<MySqlSplit> iterator = mySqlSplits.iterator();
        while (iterator.hasNext()) {
            MySqlSplit split = iterator.next();
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private Collection<MySqlSplit> readMySQLSplits(DataInputDeserializer in) throws IOException {
        Collection<MySqlSplit> mySqlSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int splitBytesLen = in.readInt();
            byte[] splitBytes = new byte[splitBytesLen];
            in.read(splitBytes);
            MySqlSplit mySQLSplit = splitSerializer.deserialize(getVersion(), splitBytes);
            mySqlSplits.add(mySQLSplit);
        }
        return mySqlSplits;
    }

    private void writeTableIds(Collection<TableId> tableIds, DataOutputSerializer out)
            throws IOException {
        final int size = tableIds.size();
        out.writeInt(size);
        final Iterator<TableId> idIterator = tableIds.iterator();
        while (idIterator.hasNext()) {
            TableId tableId = idIterator.next();
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
