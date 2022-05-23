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

package com.ververica.cdc.connectors.base.source.meta.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetDeserializerSerializer;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.utils.SerializerUtils;
import io.debezium.schema.DataCollectionId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.base.utils.SerializerUtils.serializedStringToObject;
import static com.ververica.cdc.connectors.base.utils.SerializerUtils.serializedStringToRow;

/** A serializer for the {@link SourceSplitBase}. */
public abstract class SourceSplitSerializer<ID extends DataCollectionId, S>
        implements SimpleVersionedSerializer<SourceSplitBase<ID, S>>, OffsetDeserializerSerializer {

    private static final int VERSION = 3;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int STREAM_SPLIT_FLAG = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SourceSplitBase<ID, S> split) throws IOException {
        if (split.isSnapshotSplit()) {
            final SnapshotSplit<ID, S> snapshotSplit = split.asSnapshotSplit();
            // optimization: the splits lazily cache their own serialized form
            if (snapshotSplit.serializedFormCache != null) {
                return snapshotSplit.serializedFormCache;
            }

            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(SNAPSHOT_SPLIT_FLAG);
            out.writeUTF(snapshotSplit.getTableId().identifier());
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
            final StreamSplit<ID, S> streamSplit = split.asStreamSplit();
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
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            streamSplit.serializedFormCache = result;
            return result;
        }
    }

    @Override
    public SourceSplitBase<ID, S> deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
            case 2:
            case 3:
                return deserializeSplit(version, serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public SourceSplitBase<ID, S> deserializeSplit(int version, byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        int splitKind = in.readInt();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            ID tableId = parseTableId(in.readUTF());
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            Object[] splitBoundaryStart = SerializerUtils.serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = SerializerUtils.serializedStringToRow(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);
            Map<ID, S> tableSchemas = readTableSchemas(version, in);

            return new SnapshotSplit<>(
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
            List<FinishedSnapshotSplitInfo<ID>> finishedSplitsInfo =
                    readFinishedSplitsInfo(version, in);
            Map<ID, S> tableChangeMap = readTableSchemas(version, in);
            int totalFinishedSplitSize = finishedSplitsInfo.size();
            if (version == 3) {
                totalFinishedSplitSize = in.readInt();
            }
            in.releaseArrays();
            return new StreamSplit<>(
                    splitId,
                    startingOffset,
                    endingOffset,
                    finishedSplitsInfo,
                    tableChangeMap,
                    totalFinishedSplitSize);
        } else {
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }

    public abstract ID parseTableId(String identity);

    protected abstract S readTableSchema(String serialized) throws IOException;

    protected abstract String writeTableSchema(S tableSchema) throws IOException;

    private void writeTableSchemas(Map<ID, S> tableSchemas, DataOutputSerializer out)
            throws IOException {
        final int size = tableSchemas.size();
        out.writeInt(size);
        for (Map.Entry<ID, S> entry : tableSchemas.entrySet()) {
            out.writeUTF(entry.getKey().toString());
            final String tableChangeStr = writeTableSchema(entry.getValue());
            final byte[] tableChangeBytes = tableChangeStr.getBytes(StandardCharsets.UTF_8);
            out.writeInt(tableChangeBytes.length);
            out.write(tableChangeBytes);
        }
    }

    private Map<ID, S> readTableSchemas(int version, DataInputDeserializer in) throws IOException {
        Map<ID, S> tableSchemas = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ID tableId = parseTableId(in.readUTF());
            final String tableChangeStr;
            switch (version) {
                case 1:
                    tableChangeStr = in.readUTF();
                    break;
                case 2:
                case 3:
                    final int len = in.readInt();
                    final byte[] bytes = new byte[len];
                    in.read(bytes);
                    tableChangeStr = new String(bytes, StandardCharsets.UTF_8);
                    break;
                default:
                    throw new IOException("Unknown version: " + version);
            }
            S tableSchema = readTableSchema(tableChangeStr);
            tableSchemas.put(tableId, tableSchema);
        }
        return tableSchemas;
    }

    private void writeFinishedSplitsInfo(
            List<FinishedSnapshotSplitInfo<ID>> finishedSplitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (FinishedSnapshotSplitInfo<ID> splitInfo : finishedSplitsInfo) {
            splitInfo.serialize(out);
        }
    }

    private List<FinishedSnapshotSplitInfo<ID>> readFinishedSplitsInfo(
            int version, DataInputDeserializer in) throws IOException {
        List<FinishedSnapshotSplitInfo<ID>> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ID tableId = parseTableId(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            OffsetFactory offsetFactory = (OffsetFactory) serializedStringToObject(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);

            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo<>(
                            tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory));
        }
        return finishedSplitsInfo;
    }

    public FinishedSnapshotSplitInfo<ID> readFinishedSplitInfo(byte[] serialized) {
        try {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            ID tableId = parseTableId(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            OffsetFactory offsetFactory = (OffsetFactory) serializedStringToObject(in.readUTF());
            Offset highWatermark = readOffsetPosition(in);
            in.releaseArrays();

            return new FinishedSnapshotSplitInfo<>(
                    tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
