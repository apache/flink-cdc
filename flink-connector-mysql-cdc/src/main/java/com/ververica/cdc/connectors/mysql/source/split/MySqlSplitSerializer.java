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

package com.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.rowToSerializedString;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/** A serializer for the {@link MySqlSplit}. */
public final class MySqlSplitSerializer implements SimpleVersionedSerializer<MySqlSplit> {

    public static final MySqlSplitSerializer INSTANCE = new MySqlSplitSerializer();

    private static final int VERSION = 4;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int BINLOG_SPLIT_FLAG = 2;

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
            out.writeInt(SNAPSHOT_SPLIT_FLAG);
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
            out.writeInt(BINLOG_SPLIT_FLAG);
            out.writeUTF(binlogSplit.splitId());
            out.writeUTF("");
            writeBinlogPosition(binlogSplit.getStartingOffset(), out);
            writeBinlogPosition(binlogSplit.getEndingOffset(), out);
            writeFinishedSplitsInfo(binlogSplit.getFinishedSnapshotSplitInfos(), out);
            writeTableSchemas(binlogSplit.getTableSchemas(), out);
            out.writeInt(binlogSplit.getTotalFinishedSplitSize());
            out.writeBoolean(binlogSplit.isSuspended());
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
        switch (version) {
            case 1:
            case 2:
            case 3:
            case 4:
                return deserializeSplit(version, serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public MySqlSplit deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        int splitKind = in.readInt();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            Object[] splitBoundaryStart = serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(version, in);
            Map<TableId, TableChange> tableSchemas = readTableSchemas(version, in);

            return new MySqlSnapshotSplit(
                    tableId,
                    splitId,
                    splitKeyType,
                    splitBoundaryStart,
                    splitBoundaryEnd,
                    highWatermark,
                    tableSchemas);
        } else if (splitKind == BINLOG_SPLIT_FLAG) {
            String splitId = in.readUTF();
            // skip split Key Type
            in.readUTF();
            BinlogOffset startingOffset = readBinlogPosition(version, in);
            BinlogOffset endingOffset = readBinlogPosition(version, in);
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                    readFinishedSplitsInfo(version, in);
            Map<TableId, TableChange> tableChangeMap = readTableSchemas(version, in);
            int totalFinishedSplitSize = finishedSplitsInfo.size();
            boolean isSuspended = false;
            if (version >= 3) {
                totalFinishedSplitSize = in.readInt();
                if (version > 3) {
                    isSuspended = in.readBoolean();
                }
            }
            in.releaseArrays();
            return new MySqlBinlogSplit(
                    splitId,
                    startingOffset,
                    endingOffset,
                    finishedSplitsInfo,
                    tableChangeMap,
                    totalFinishedSplitSize,
                    isSuspended);
        } else {
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }

    public static void writeTableSchemas(
            Map<TableId, TableChange> tableSchemas, DataOutputSerializer out) throws IOException {
        FlinkJsonTableChangeSerializer jsonSerializer = new FlinkJsonTableChangeSerializer();
        DocumentWriter documentWriter = DocumentWriter.defaultWriter();
        final int size = tableSchemas.size();
        out.writeInt(size);
        for (Map.Entry<TableId, TableChange> entry : tableSchemas.entrySet()) {
            out.writeUTF(entry.getKey().toString());
            final String tableChangeStr =
                    documentWriter.write(jsonSerializer.toDocument(entry.getValue()));
            final byte[] tableChangeBytes = tableChangeStr.getBytes(StandardCharsets.UTF_8);
            out.writeInt(tableChangeBytes.length);
            out.write(tableChangeBytes);
        }
    }

    public static Map<TableId, TableChange> readTableSchemas(int version, DataInputDeserializer in)
            throws IOException {
        DocumentReader documentReader = DocumentReader.defaultReader();
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            final String tableChangeStr;
            switch (version) {
                case 1:
                    tableChangeStr = in.readUTF();
                    break;
                case 2:
                case 3:
                case 4:
                    final int len = in.readInt();
                    final byte[] bytes = new byte[len];
                    in.read(bytes);
                    tableChangeStr = new String(bytes, StandardCharsets.UTF_8);
                    break;
                default:
                    throw new IOException("Unknown version: " + version);
            }
            Document document = documentReader.read(tableChangeStr);
            TableChange tableChange = FlinkJsonTableChangeSerializer.fromDocument(document, true);
            tableSchemas.put(tableId, tableChange);
        }
        return tableSchemas;
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

    private static List<FinishedSnapshotSplitInfo> readFinishedSplitsInfo(
            int version, DataInputDeserializer in) throws IOException {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(version, in);
            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            tableId, splitId, splitStart, splitEnd, highWatermark));
        }
        return finishedSplitsInfo;
    }
}
