/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.base.source.meta.split.version4;

import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetDeserializerSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.utils.SerializerUtils;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** The 4th version of legacy SourceSplitSerializier for test. */
public class LegacySourceSplitSerializierVersion4 {
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int STREAM_SPLIT_FLAG = 2;

    public static byte[] serialize(SourceSplitBase sourceSplit) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        if (sourceSplit instanceof SnapshotSplit) {
            serializeSnapshotSplit(sourceSplit.asSnapshotSplit(), out);
        } else if (sourceSplit instanceof StreamSplitVersion4) {
            serializeStreamSplit((StreamSplitVersion4) sourceSplit, out);
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    private static void serializeSnapshotSplit(
            SnapshotSplit snapshotSplit, DataOutputSerializer out) throws IOException {
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
    }

    private static void serializeStreamSplit(
            StreamSplitVersion4 streamSplit, DataOutputSerializer out) throws IOException {

        out.writeInt(STREAM_SPLIT_FLAG);
        out.writeUTF(streamSplit.splitId());
        out.writeUTF("");
        writeOffsetPosition(streamSplit.getStartingOffset(), out);
        writeOffsetPosition(streamSplit.getEndingOffset(), out);
        writeFinishedSplitsInfo(streamSplit.getFinishedSnapshotSplitInfos(), out);
        writeTableSchemas(streamSplit.getTableSchemas(), out);
        out.writeInt(streamSplit.getTotalFinishedSplitSize());
    }

    public static void writeTableSchemas(
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

    private static void writeFinishedSplitsInfo(
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo, DataOutputSerializer out)
            throws IOException {
        final int size = finishedSplitsInfo.size();
        out.writeInt(size);
        for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo) {
            splitInfo.serialize(out);
        }
    }

    public static void writeOffsetPosition(Offset offset, DataOutputSerializer out)
            throws IOException {
        out.writeBoolean(offset != null);
        if (offset != null) {
            byte[] offsetBytes =
                    OffsetDeserializerSerializer.OffsetSerializer.INSTANCE.serialize(offset);
            out.writeInt(offsetBytes.length);
            out.write(offsetBytes);
        }
    }
}
