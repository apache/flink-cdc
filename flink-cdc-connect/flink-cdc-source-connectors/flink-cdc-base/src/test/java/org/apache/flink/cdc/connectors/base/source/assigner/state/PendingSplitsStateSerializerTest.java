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

package org.apache.flink.cdc.connectors.base.source.assigner.state;

import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PendingSplitsStateSerializer}. */
class PendingSplitsStateSerializerTest {
    @Test
    void testSourceSplitSerializeAndDeserialize() throws IOException {
        SnapshotSplit snapshotSplitBefore = constuctSnapshotSplit();
        SourceSplitSerializer sourceSplitSerializer = constructSourceSplitSerializer();
        SnapshotSplit snapshotSplitAfter =
                (SnapshotSplit)
                        sourceSplitSerializer.deserialize(
                                sourceSplitSerializer.getVersion(),
                                sourceSplitSerializer.serialize(snapshotSplitBefore));

        assertThat(snapshotSplitAfter).isEqualTo(snapshotSplitBefore);
        assertThat(snapshotSplitAfter.getTableSchemas())
                .isEqualTo(snapshotSplitBefore.getTableSchemas());
        StreamSplit streamSplitBefore = constuctStreamSplit();
        StreamSplit streamSplitAfter =
                (StreamSplit)
                        sourceSplitSerializer.deserialize(
                                sourceSplitSerializer.getVersion(),
                                sourceSplitSerializer.serialize(streamSplitBefore));

        assertThat(streamSplitAfter).isEqualTo(streamSplitBefore);
    }

    @Test
    void testOutputIsFinallyCleared() throws Exception {
        PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(constructSourceSplitSerializer());
        StreamPendingSplitsState state = new StreamPendingSplitsState(true);

        final byte[] ser1 = serializer.serialize(state);
        state.serializedFormCache = null;

        PendingSplitsState unsupportedState = new UnsupportedPendingSplitsState();

        Assertions.assertThatThrownBy(() -> serializer.serialize(unsupportedState))
                .isExactlyInstanceOf(IOException.class);

        final byte[] ser2 = serializer.serialize(state);
        assertThat(ser1).isEqualTo(ser2);
    }

    @Test
    void testSerializeSnapshotPendingSplitsState() throws Exception {
        PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(constructSourceSplitSerializer());
        PendingSplitsState state =
                new SnapshotPendingSplitsState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        constructTableSchema(),
                        Collections.emptyMap(),
                        AssignerStatus.INITIAL_ASSIGNING,
                        Collections.emptyList(),
                        false,
                        true,
                        Collections.emptyMap(),
                        new ChunkSplitterState(
                                constructTableId(), ChunkSplitterState.ChunkBound.middleOf(1), 2));

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    private SourceSplitSerializer constructSourceSplitSerializer() {
        return new SourceSplitSerializer() {
            @Override
            public OffsetFactory getOffsetFactory() {
                return new OffsetFactory() {
                    @Override
                    public Offset newOffset(Map<String, String> offset) {
                        return null;
                    }

                    @Override
                    public Offset newOffset(String filename, Long position) {
                        return null;
                    }

                    @Override
                    public Offset newOffset(Long position) {
                        return null;
                    }

                    @Override
                    public Offset createTimestampOffset(long timestampMillis) {
                        return null;
                    }

                    @Override
                    public Offset createInitialOffset() {
                        return null;
                    }

                    @Override
                    public Offset createNoStoppingOffset() {
                        return null;
                    }
                };
            }
        };
    }

    private SnapshotSplit constuctSnapshotSplit() {
        TableId tableId = new TableId("cata`log\"", "s\"che`ma", "ta\"ble.1`");
        return new SnapshotSplit(
                tableId,
                "test",
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {1},
                new Object[] {100},
                null,
                constructTableSchema());
    }

    private StreamSplit constuctStreamSplit() {
        return new StreamSplit(
                "database1.schema1.table1",
                null,
                null,
                new ArrayList<>(),
                constructTableSchema(),
                0,
                false,
                true);
    }

    private HashMap<TableId, TableChanges.TableChange> constructTableSchema() {
        TableId tableId = constructTableId();
        HashMap<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
        Tables tables = new Tables();
        Table table = tables.editOrCreateTable(tableId).create();
        TableChanges.TableChange tableChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        tableSchema.put(tableId, tableChange);
        return tableSchema;
    }

    private TableId constructTableId() {
        return new TableId("cata`log\"", "s\"che`ma", "ta\"ble.1`");
    }

    /** An implementation for {@link PendingSplitsState} which will cause a serialization error. */
    static class UnsupportedPendingSplitsState extends PendingSplitsState {}
}
