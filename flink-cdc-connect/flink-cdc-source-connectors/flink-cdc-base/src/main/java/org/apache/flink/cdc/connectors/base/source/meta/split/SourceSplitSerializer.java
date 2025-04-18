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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetDeserializerSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

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

/**
 * A serializer for the {@link SourceSplitBase}.
 *
 * <p>The modification of 5th version: add isSuspended(boolean) to StreamSplit, which means whether
 * stream split read is suspended.
 */
public abstract class SourceSplitSerializer
        implements SimpleVersionedSerializer<SourceSplitBase>, OffsetDeserializerSerializer {

    private static final int VERSION = 6;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int STREAM_SPLIT_FLAG = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SourceSplitBase split) throws IOException {
        if (split.isSnapshotSplit()) {
            final SnapshotSplit snapshotSplit = split.asSnapshotSplit();
            // optimization: the splits lazily cache their own serialized form
            if (snapshotSplit.serializedFormCache != null) {
                return snapshotSplit.serializedFormCache;
            }

            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(SNAPSHOT_SPLIT_FLAG);
            boolean useCatalogBeforeSchema =
                    SerializerUtils.shouldUseCatalogBeforeSchema(snapshotSplit.getTableId());
            out.writeBoolean(useCatalogBeforeSchema);
            out.writeUTF(snapshotSplit.getTableId().toDoubleQuotedString());
            out.writeUTF(snapshotSplit.splitId());
            out.writeUTF(snapshotSplit.getSplitKeyType().asSerializableString());

            final Object[] splitStart = snapshotSplit.getSplitStart();
            final Object[] splitEnd = snapshotSplit.getSplitEnd();
            // rowToSerializedString deals null case
            out.writeUTF(SerializerUtils.rowToSerializedString(splitStart));
            out.writeUTF(SerializerUtils.rowToSerializedString(splitEnd));
            writeOffsetPosition(snapshotSplit.getHighWatermark(), out);
            writeTableSchemas(snapshotSplit.getTableSchemas(), out);
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            snapshotSplit.serializedFormCache = result;
            return result;
        } else {
            final StreamSplit streamSplit = split.asStreamSplit();
            // optimization: the splits lazily cache their own serialized form
            if (streamSplit.serializedFormCache != null) {
                return streamSplit.serializedFormCache;
            }
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(STREAM_SPLIT_FLAG);
            out.writeUTF(streamSplit.splitId());
            out.writeUTF("");
            writeOffsetPosition(streamSplit.getStartingOffset(), out);
            writeOffsetPosition(streamSplit.getEndingOffset(), out);
            writeFinishedSplitsInfo(streamSplit.getFinishedSnapshotSplitInfos(), out);
            writeTableSchemas(streamSplit.getTableSchemas(), out);
            out.writeInt(streamSplit.getTotalFinishedSplitSize());
            out.writeBoolean(streamSplit.isSuspended());
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            streamSplit.serializedFormCache = result;
            return result;
        }
    }

    @Override
    public SourceSplitBase deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
                return deserializeSplit(version, serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public SourceSplitBase deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        int splitKind = in.readInt();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            boolean useCatalogBeforeSchema = true;
            if (version >= 4) {
                useCatalogBeforeSchema = in.readBoolean();
            }
            TableId tableId = TableId.parse(in.readUTF(), useCatalogBeforeSchema);
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            Object[] splitBoundaryStart = SerializerUtils.serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = SerializerUtils.serializedStringToRow(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);
            Map<TableId, TableChange> tableSchemas = readTableSchemas(version, in);

            return new SnapshotSplit(
                    tableId,
                    splitId,
                    splitKeyType,
                    splitBoundaryStart,
                    splitBoundaryEnd,
                    highWatermark,
                    tableSchemas);
        } else if (splitKind == STREAM_SPLIT_FLAG) {
            String splitId = in.readUTF();
            // skip split Key Type
            in.readUTF();
            Offset startingOffset = readOffsetPosition(version, in);
            Offset endingOffset = readOffsetPosition(version, in);
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                    readFinishedSplitsInfo(version, in);
            Map<TableId, TableChange> tableChangeMap = readTableSchemas(version, in);
            int totalFinishedSplitSize = finishedSplitsInfo.size();
            if (version >= 3) {
                totalFinishedSplitSize = in.readInt();
            }

            boolean isSuspended = false;
            if (version == 5) {
                isSuspended = in.readBoolean();
            }

            boolean isSnapshotCompleted = false;
            if (version >= 6) {
                isSnapshotCompleted = in.readBoolean();
            }

            in.releaseArrays();
            return new StreamSplit(
                    splitId,
                    startingOffset,
                    endingOffset,
                    finishedSplitsInfo,
                    tableChangeMap,
                    totalFinishedSplitSize,
                    isSuspended,
                    isSnapshotCompleted);
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

    public static Map<TableId, TableChange> readTableSchemas(int version, DataInputDeserializer in)
            throws IOException {
        DocumentReader documentReader = DocumentReader.defaultReader();
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            boolean useCatalogBeforeSchema = in.readBoolean();
            TableId tableId = TableId.parse(in.readUTF(), useCatalogBeforeSchema);
            final String tableChangeStr;
            switch (version) {
                case 1:
                    tableChangeStr = in.readUTF();
                    break;
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
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

    private void writeFinishedSplitsInfo(
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo) {
            splitInfo.serialize(out);
        }
    }

    private List<FinishedSnapshotSplitInfo> readFinishedSplitsInfo(
            int version, DataInputDeserializer in) throws IOException {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tableIdStr = in.readUTF();
            String splitId = in.readUTF();
            Object[] splitStart = SerializerUtils.serializedStringToRow(in.readUTF());
            Object[] splitEnd = SerializerUtils.serializedStringToRow(in.readUTF());
            OffsetFactory offsetFactory =
                    (OffsetFactory) SerializerUtils.serializedStringToObject(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);
            boolean useCatalogBeforeSchema = true;
            if (version >= 4) {
                useCatalogBeforeSchema = in.readBoolean();
            }
            TableId tableId = TableId.parse(tableIdStr, useCatalogBeforeSchema);

            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory));
        }
        return finishedSplitsInfo;
    }
}
