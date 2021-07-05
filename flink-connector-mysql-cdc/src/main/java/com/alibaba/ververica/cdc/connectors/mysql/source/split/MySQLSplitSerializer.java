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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition.INITIAL_OFFSET;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.rowToSerializedString;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/** A serializer for the {@link MySQLSplit}. */
public final class MySQLSplitSerializer implements SimpleVersionedSerializer<MySQLSplit> {

    private static final int VERSION = 1;

    public static final MySQLSplitSerializer INSTANCE = new MySQLSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    private static final ThreadLocal<DataInputDeserializer> DESERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataInputDeserializer());

    private static final ThreadLocal<DocumentWriter> DOCUMENT_WRITER =
            ThreadLocal.withInitial(DocumentWriter::defaultWriter);

    private static final ThreadLocal<DocumentReader> DOCUMENT_READER =
            ThreadLocal.withInitial(DocumentReader::defaultReader);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MySQLSplit split) throws IOException {

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeUTF(split.getSplitKind().toString());
        out.writeUTF(split.getTableId().toString());
        out.writeUTF(split.getSplitId());
        out.writeUTF(split.getSplitBoundaryType().asSerializableString());

        final boolean isSnapshotSplit = split.getSplitKind() == MySQLSplitKind.SNAPSHOT;

        out.writeBoolean(isSnapshotSplit);
        // snapshot split
        if (isSnapshotSplit) {

            final Object[] splitBoundaryStart = split.getSplitBoundaryStart();
            final Object[] splitBoundaryEnd = split.getSplitBoundaryEnd();
            // rowToSerializedString deals null case
            out.writeUTF(rowToSerializedString(splitBoundaryStart));
            out.writeUTF(rowToSerializedString(splitBoundaryEnd));

            writeBinlogPosition(split.getLowWatermark(), out);
            writeBinlogPosition(split.getHighWatermark(), out);

            out.writeBoolean(split.isSnapshotReadFinished());
        }
        // binlog split
        else {
            writeBinlogPosition(split.getOffset(), out);
            writeFinishedSplitsInfo(split.getFinishedSplitsInfo(), out);
        }
        writeDatabaseHistory(split.getDatabaseHistory(), out);

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public MySQLSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    public MySQLSplit deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = DESERIALIZER_CACHE.get();
        in.setBuffer(serialized);

        MySQLSplitKind splitKind = MySQLSplitKind.fromString(in.readUTF());
        TableId tableId = TableId.parse(in.readUTF());
        String splitId = in.readUTF();
        RowType splitBoundaryType = (RowType) LogicalTypeParser.parse(in.readUTF());
        boolean isSnapshotSplit = in.readBoolean();

        if (isSnapshotSplit) {
            Object[] splitBoundaryStart = serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = serializedStringToRow(in.readUTF());
            BinlogPosition lowWatermark = readBinlogPosition(in);
            BinlogPosition highWatermark = readBinlogPosition(in);
            boolean isSnapshotReadFinished = in.readBoolean();
            Map<TableId, SchemaRecord> databaseHistory = readDatabaseHistory(in);

            in.releaseArrays();
            return new MySQLSplit(
                    splitKind,
                    tableId,
                    splitId,
                    splitBoundaryType,
                    splitBoundaryStart,
                    splitBoundaryEnd,
                    lowWatermark,
                    highWatermark,
                    isSnapshotReadFinished,
                    INITIAL_OFFSET,
                    new ArrayList<>(),
                    databaseHistory);
        } else {
            BinlogPosition offset = readBinlogPosition(in);
            List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo =
                    readFinishedSplitsInfo(in);
            Map<TableId, SchemaRecord> databaseHistory = readDatabaseHistory(in);
            in.releaseArrays();
            return new MySQLSplit(
                    splitKind,
                    tableId,
                    splitId,
                    splitBoundaryType,
                    null,
                    null,
                    null,
                    null,
                    true,
                    offset,
                    finishedSplitsInfo,
                    databaseHistory);
        }
    }

    private static void writeDatabaseHistory(
            Map<TableId, SchemaRecord> databaseHistory, DataOutputSerializer out)
            throws IOException {
        final int size = databaseHistory.size();
        out.writeInt(size);
        for (Map.Entry<TableId, SchemaRecord> entry : databaseHistory.entrySet()) {
            out.writeUTF(entry.getKey().toString());
            out.writeUTF(DOCUMENT_WRITER.get().write(entry.getValue().toDocument()));
        }
    }

    private static Map<TableId, SchemaRecord> readDatabaseHistory(DataInputDeserializer in)
            throws IOException {
        Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            Document document = DOCUMENT_READER.get().read(in.readUTF());
            SchemaRecord historyRecord = new SchemaRecord(document);
            databaseHistory.put(tableId, historyRecord);
        }
        return databaseHistory;
    }

    private static void writeFinishedSplitsInfo(
            List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo,
            DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            Tuple5<TableId, String, Object[], Object[], BinlogPosition> splitInfo =
                    finishedSplitsInfo.get(i);
            out.writeUTF(splitInfo.f0.toString());
            out.writeUTF(splitInfo.f1);
            out.writeUTF(rowToSerializedString(splitInfo.f2));
            out.writeUTF(rowToSerializedString(splitInfo.f3));
            writeBinlogPosition(splitInfo.f4, out);
        }
    }

    private static List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>>
            readFinishedSplitsInfo(DataInputDeserializer in) throws IOException {
        List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo =
                new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            BinlogPosition highWatermark = readBinlogPosition(in);
            finishedSplitsInfo.add(
                    Tuple5.of(tableId, splitId, splitStart, splitEnd, highWatermark));
        }
        return finishedSplitsInfo;
    }
}
