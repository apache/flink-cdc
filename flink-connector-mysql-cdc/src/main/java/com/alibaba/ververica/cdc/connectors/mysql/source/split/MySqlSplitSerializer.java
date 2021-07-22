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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges.TableChange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.rowToSerializedString;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/** A serializer for the {@link MySqlSplit}. */
public final class MySqlSplitSerializer implements SimpleVersionedSerializer<MySqlSplit> {

    private static final int VERSION = 1;

    public static final MySqlSplitSerializer INSTANCE = new MySqlSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    private static final ThreadLocal<DataInputDeserializer> DESERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataInputDeserializer());

    private static final ThreadLocal<DocumentWriter> DOCUMENT_WRITER =
            ThreadLocal.withInitial(DocumentWriter::defaultWriter);

    private static final ThreadLocal<DocumentReader> DOCUMENT_READER =
            ThreadLocal.withInitial(DocumentReader::defaultReader);

    private static final ThreadLocal<JsonTableChangeSerializer> JSON_TABLE_CHANGE_SERIALIZER =
            ThreadLocal.withInitial(JsonTableChangeSerializer::new);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MySqlSplit split) throws IOException {
        if (split.isSnapshotSplit()) {
            final MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
            // optimization: the splits lazily cache their own serialized form
            if (snapshotSplit.serializedFormCache != null) {
                return snapshotSplit.serializedFormCache;
            }

            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeUTF(String.valueOf(MySqlSplitKind.SNAPSHOT));
            out.writeUTF(snapshotSplit.getTableId().toString());
            out.writeUTF(snapshotSplit.splitId());
            out.writeUTF(snapshotSplit.getSplitKeyType().asSerializableString());

            final Object[] splitStart = snapshotSplit.getSplitStart();
            final Object[] splitEnd = snapshotSplit.getSplitEnd();
            // rowToSerializedString deals null case
            out.writeUTF(rowToSerializedString(splitStart));
            out.writeUTF(rowToSerializedString(splitEnd));
            writeBinlogPosition(snapshotSplit.getHighWatermark(), out);
            writeTableSchemas(snapshotSplit.getTableSchemas(), out);
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            snapshotSplit.serializedFormCache = result;
            return result;
        } else {
            final MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
            // optimization: the splits lazily cache their own serialized form
            if (binlogSplit.serializedFormCache != null) {
                return binlogSplit.serializedFormCache;
            }
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeUTF(String.valueOf(MySqlSplitKind.BINLOG));
            out.writeUTF(binlogSplit.splitId());
            out.writeUTF(binlogSplit.getSplitKeyType().asSerializableString());

            writeBinlogPosition(binlogSplit.getStartingOffset(), out);
            writeBinlogPosition(binlogSplit.getEndingOffset(), out);
            writeFinishedSplitsInfo(binlogSplit.getFinishedSnapshotSplitInfos(), out);
            writeTableSchemas(binlogSplit.getTableSchemas(), out);
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            binlogSplit.serializedFormCache = result;
            return result;
        }
    }

    @Override
    public MySqlSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    public MySqlSplit deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = DESERIALIZER_CACHE.get();
        in.setBuffer(serialized);

        MySqlSplitKind splitKind = MySqlSplitKind.fromString(in.readUTF());
        if (splitKind == MySqlSplitKind.SNAPSHOT) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            Object[] splitBoundaryStart = serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(in);
            Map<TableId, TableChange> databaseHistory = readTableSchemas(in);

            in.releaseArrays();
            return new MySqlSnapshotSplit(
                    tableId,
                    splitId,
                    splitKeyType,
                    splitBoundaryStart,
                    splitBoundaryEnd,
                    highWatermark,
                    databaseHistory);
        } else {
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            BinlogOffset startingOffset = readBinlogPosition(in);
            BinlogOffset endingOffset = readBinlogPosition(in);
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo = readFinishedSplitsInfo(in);
            Map<TableId, TableChange> tableChangeMap = readTableSchemas(in);
            in.releaseArrays();
            return new MySqlBinlogSplit(
                    splitId,
                    splitKeyType,
                    startingOffset,
                    endingOffset,
                    finishedSplitsInfo,
                    tableChangeMap);
        }
    }

    private static void writeTableSchemas(
            Map<TableId, TableChange> tableSchemas, DataOutputSerializer out) throws IOException {
        final int size = tableSchemas.size();
        out.writeInt(size);
        for (Map.Entry<TableId, TableChange> entry : tableSchemas.entrySet()) {
            out.writeUTF(entry.getKey().toString());
            out.writeUTF(
                    DOCUMENT_WRITER
                            .get()
                            .write(
                                    JSON_TABLE_CHANGE_SERIALIZER
                                            .get()
                                            .toDocument(entry.getValue())));
        }
    }

    private static Map<TableId, TableChange> readTableSchemas(DataInputDeserializer in)
            throws IOException {
        Map<TableId, TableChange> databaseHistory = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            Document document = DOCUMENT_READER.get().read(in.readUTF());
            TableChange tableChange = JsonTableChangeSerializer.fromDocument(document, true);
            databaseHistory.put(tableId, tableChange);
        }
        return databaseHistory;
    }

    private static void writeFinishedSplitsInfo(
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo) {
            out.writeUTF(splitInfo.getTableId().toString());
            out.writeUTF(splitInfo.getSplitId());
            out.writeUTF(rowToSerializedString(splitInfo.getSplitStart()));
            out.writeUTF(rowToSerializedString(splitInfo.getSplitEnd()));
            writeBinlogPosition(splitInfo.getHighWatermark(), out);
        }
    }

    private static List<FinishedSnapshotSplitInfo> readFinishedSplitsInfo(DataInputDeserializer in)
            throws IOException {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(in);
            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            tableId, splitId, splitStart, splitEnd, highWatermark));
        }
        return finishedSplitsInfo;
    }

    /** The split to describe a split of a MySql table. */
    private enum MySqlSplitKind {

        /** The split that reads snapshot records of MySQL table. */
        SNAPSHOT,

        /** The split that reads binlog records of MySQL table. */
        BINLOG;

        public static MySqlSplitKind fromString(String kind) {
            if (SNAPSHOT.toString().equalsIgnoreCase(kind)) {
                return SNAPSHOT;
            }
            return BINLOG;
        }
    }
}
