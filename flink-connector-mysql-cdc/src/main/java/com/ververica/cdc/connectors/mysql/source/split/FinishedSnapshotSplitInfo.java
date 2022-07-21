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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.rowToSerializedString;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.writeBinlogPosition;

/** The information used to describe a finished snapshot split. */
public class FinishedSnapshotSplitInfo {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final TableId tableId;
    private final String splitId;
    private final Object[] splitStart;
    private final Object[] splitEnd;
    private final BinlogOffset highWatermark;

    public FinishedSnapshotSplitInfo(
            TableId tableId,
            String splitId,
            Object[] splitStart,
            Object[] splitEnd,
            BinlogOffset highWatermark) {
        this.tableId = tableId;
        this.splitId = splitId;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getSplitId() {
        return splitId;
    }

    public Object[] getSplitStart() {
        return splitStart;
    }

    public Object[] getSplitEnd() {
        return splitEnd;
    }

    public BinlogOffset getHighWatermark() {
        return highWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FinishedSnapshotSplitInfo that = (FinishedSnapshotSplitInfo) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(splitId, that.splitId)
                && Arrays.equals(splitStart, that.splitStart)
                && Arrays.equals(splitEnd, that.splitEnd)
                && Objects.equals(highWatermark, that.highWatermark);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId, splitId, highWatermark);
        result = 31 * result + Arrays.hashCode(splitStart);
        result = 31 * result + Arrays.hashCode(splitEnd);
        return result;
    }

    @Override
    public String toString() {
        return "FinishedSnapshotSplitInfo{"
                + "tableId="
                + tableId
                + ", splitId='"
                + splitId
                + '\''
                + ", splitStart="
                + Arrays.toString(splitStart)
                + ", splitEnd="
                + Arrays.toString(splitEnd)
                + ", highWatermark="
                + highWatermark
                + '}';
    }

    // ------------------------------------------------------------------------------------
    // Utils to serialize/deserialize for transmission between Enumerator and SourceReader
    // ------------------------------------------------------------------------------------
    public static byte[] serialize(FinishedSnapshotSplitInfo splitInfo) {
        try {
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeUTF(splitInfo.getTableId().toString());
            out.writeUTF(splitInfo.getSplitId());
            out.writeUTF(rowToSerializedString(splitInfo.getSplitStart()));
            out.writeUTF(rowToSerializedString(splitInfo.getSplitEnd()));
            writeBinlogPosition(splitInfo.getHighWatermark(), out);
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            return result;
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public static FinishedSnapshotSplitInfo deserialize(byte[] serialized) {
        try {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(in);
            in.releaseArrays();
            return new FinishedSnapshotSplitInfo(
                    tableId, splitId, splitStart, splitEnd, highWatermark);

        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
