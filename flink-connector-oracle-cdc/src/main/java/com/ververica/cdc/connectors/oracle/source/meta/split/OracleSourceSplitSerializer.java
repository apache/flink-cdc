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

package com.ververica.cdc.connectors.oracle.source.meta.split;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.utils.SerializerUtils;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An Oracle serializer for the {@link SourceSplitBase}. */
public class OracleSourceSplitSerializer extends SourceSplitSerializer {

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int REDOLOG_SPLIT_FLAG = 2;
    RedoLogOffsetFactory offsetFactory;

    public OracleSourceSplitSerializer(RedoLogOffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public OffsetFactory getOffsetFactory() {
        return offsetFactory;
    }

    @Override
    public Offset readOffsetPosition(int offsetVersion, DataInputDeserializer in)
            throws IOException {
        return super.readOffsetPosition(offsetVersion, in);
    }

    @Override
    public Offset readOffsetPosition(DataInputDeserializer in) throws IOException {
        return super.readOffsetPosition(in);
    }

    @Override
    public void writeOffsetPosition(Offset offset, DataOutputSerializer out) throws IOException {
        super.writeOffsetPosition(offset, out);
    }

    @Override
    public OffsetDeserializer createOffsetDeserializer() {
        return super.createOffsetDeserializer();
    }

    @Override
    public FinishedSnapshotSplitInfo deserialize(byte[] serialized) {
        return super.deserialize(serialized);
    }

    @Override
    public byte[] serialize(SourceSplitBase split) throws IOException {
        return super.serialize(split);
    }

    @Override
    public SourceSplitBase deserialize(int version, byte[] serialized) throws IOException {
        return super.deserialize(version, serialized);
    }

    @Override
    public SourceSplitBase deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        int splitKind = in.readInt();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            TableId tableId = TableId.parse(in.readUTF(), false);
            String splitId = in.readUTF();
            RowType splitKeyType = (RowType) LogicalTypeParser.parse(in.readUTF());
            Object[] splitBoundaryStart = SerializerUtils.serializedStringToRow(in.readUTF());
            Object[] splitBoundaryEnd = SerializerUtils.serializedStringToRow(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);
            Map<TableId, TableChanges.TableChange> tableSchemas = readTableSchemas(version, in);

            return new SnapshotSplit(
                    tableId,
                    splitId,
                    splitKeyType,
                    splitBoundaryStart,
                    splitBoundaryEnd,
                    highWatermark,
                    tableSchemas);
        } else if (splitKind == REDOLOG_SPLIT_FLAG) {
            String splitId = in.readUTF();
            // skip split Key Type
            in.readUTF();
            Offset startingOffset = readOffsetPosition(version, in);
            Offset endingOffset = readOffsetPosition(version, in);
            List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                    readFinishedSplitsInfo(version, in);
            Map<TableId, TableChanges.TableChange> tableChangeMap = readTableSchemas(version, in);
            int totalFinishedSplitSize = finishedSplitsInfo.size();
            if (version == 3) {
                totalFinishedSplitSize = in.readInt();
            }
            in.releaseArrays();
            return new StreamSplit(
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

    private List<FinishedSnapshotSplitInfo> readFinishedSplitsInfo(
            int version, DataInputDeserializer in) throws IOException {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF(), false);
            String splitId = in.readUTF();
            Object[] splitStart = SerializerUtils.serializedStringToRow(in.readUTF());
            Object[] splitEnd = SerializerUtils.serializedStringToRow(in.readUTF());
            OffsetFactory offsetFactory =
                    (OffsetFactory) SerializerUtils.serializedStringToObject(in.readUTF());
            Offset highWatermark = readOffsetPosition(version, in);

            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory));
        }
        return finishedSplitsInfo;
    }

    private static Map<TableId, TableChanges.TableChange> readTableSchemas(
            int version, DataInputDeserializer in) throws IOException {
        DocumentReader documentReader = DocumentReader.defaultReader();
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF(), false);
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
            Document document = documentReader.read(tableChangeStr);
            TableChanges.TableChange tableChange =
                    FlinkJsonTableChangeSerializer.fromDocument(document, false);
            tableSchemas.put(tableId, tableChange);
        }
        return tableSchemas;
    }
}
