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
import org.apache.flink.cdc.connectors.base.source.assigner.state.version5.HybridPendingSplitsStateVersion5;
import org.apache.flink.cdc.connectors.base.source.assigner.state.version5.PendingSplitsStateSerializerVersion5;
import org.apache.flink.cdc.connectors.base.source.assigner.state.version5.SnapshotPendingSplitsStateVersion5;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link PendingSplitsStateSerializer}. */
public class PendingSplitsStateSerializerTest {

    private TableId tableId = TableId.parse("catalog.schema.table1");

    @Test
    public void testPendingSplitsStateSerializerAndDeserialize() throws IOException {
        StreamPendingSplitsState streamPendingSplitsStateBefore =
                new StreamPendingSplitsState(true);
        PendingSplitsStateSerializer pendingSplitsStateSerializer =
                new PendingSplitsStateSerializer(constructSourceSplitSerializer());
        PendingSplitsState streamSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        6, pendingSplitsStateSerializer.serialize(streamPendingSplitsStateBefore));
        Assert.assertEquals(streamPendingSplitsStateBefore, streamSplitsStateAfter);

        SnapshotPendingSplitsState snapshotPendingSplitsStateBefore =
                constructSnapshotPendingSplitsState(AssignerStatus.NEWLY_ADDED_ASSIGNING);
        PendingSplitsState snapshotPendingSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        6,
                        pendingSplitsStateSerializer.serialize(snapshotPendingSplitsStateBefore));
        Assert.assertEquals(snapshotPendingSplitsStateBefore, snapshotPendingSplitsStateAfter);

        HybridPendingSplitsState hybridPendingSplitsStateBefore =
                new HybridPendingSplitsState(snapshotPendingSplitsStateBefore, false);
        PendingSplitsState hybridPendingSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        6, pendingSplitsStateSerializer.serialize(hybridPendingSplitsStateBefore));
        Assert.assertEquals(hybridPendingSplitsStateBefore, hybridPendingSplitsStateAfter);
    }

    @Test
    public void testPendingSplitsStateSerializerCompatibility() throws IOException {
        StreamPendingSplitsState streamPendingSplitsStateBefore =
                new StreamPendingSplitsState(true);
        PendingSplitsStateSerializer pendingSplitsStateSerializer =
                new PendingSplitsStateSerializer(constructSourceSplitSerializer());
        PendingSplitsState streamSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        5,
                        PendingSplitsStateSerializerVersion5.serialize(
                                streamPendingSplitsStateBefore));
        Assert.assertEquals(streamPendingSplitsStateBefore, streamSplitsStateAfter);

        SnapshotPendingSplitsState expectedSnapshotSplitsState =
                constructSnapshotPendingSplitsState(AssignerStatus.INITIAL_ASSIGNING);
        PendingSplitsState snapshotPendingSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        5,
                        PendingSplitsStateSerializerVersion5.serialize(
                                constructSnapshotPendingSplitsStateVersion4(false)));
        Assert.assertEquals(expectedSnapshotSplitsState, snapshotPendingSplitsStateAfter);

        HybridPendingSplitsState expectedHybridPendingSplitsState =
                new HybridPendingSplitsState(
                        constructSnapshotPendingSplitsState(
                                AssignerStatus.INITIAL_ASSIGNING_FINISHED),
                        false);
        PendingSplitsState hybridPendingSplitsStateAfter =
                pendingSplitsStateSerializer.deserializePendingSplitsState(
                        5,
                        PendingSplitsStateSerializerVersion5.serialize(
                                new HybridPendingSplitsStateVersion5(
                                        constructSnapshotPendingSplitsStateVersion4(true), false)));
        Assert.assertEquals(expectedHybridPendingSplitsState, hybridPendingSplitsStateAfter);
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

    private SchemalessSnapshotSplit constuctSchemalessSnapshotSplit() {
        return new SchemalessSnapshotSplit(
                tableId,
                "test",
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                null,
                null,
                null);
    }

    private SnapshotPendingSplitsState constructSnapshotPendingSplitsState(
            AssignerStatus assignerStatus) {
        SchemalessSnapshotSplit schemalessSnapshotSplit = constuctSchemalessSnapshotSplit();
        Map<String, SchemalessSnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(tableId.toQuotedString('`'), schemalessSnapshotSplit);
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(
                tableId,
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE, createTable(tableId)));
        return new SnapshotPendingSplitsState(
                Arrays.asList(tableId),
                Arrays.asList(schemalessSnapshotSplit),
                assignedSplits,
                tableSchemas,
                new HashMap<>(),
                assignerStatus,
                Arrays.asList(TableId.parse("catalog2.schema2.table2")),
                true,
                true);
    }

    private SnapshotPendingSplitsStateVersion5 constructSnapshotPendingSplitsStateVersion4(
            boolean isAssignerFinished) {
        SchemalessSnapshotSplit schemalessSnapshotSplit = constuctSchemalessSnapshotSplit();
        Map<String, SchemalessSnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(tableId.toQuotedString('`'), schemalessSnapshotSplit);
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(
                tableId,
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE, createTable(tableId)));
        return new SnapshotPendingSplitsStateVersion5(
                Arrays.asList(tableId),
                Arrays.asList(schemalessSnapshotSplit),
                assignedSplits,
                tableSchemas,
                new HashMap<>(),
                isAssignerFinished,
                Arrays.asList(TableId.parse("catalog2.schema2.table2")),
                true,
                true);
    }

    private static Table createTable(TableId id) {
        TableEditor editor = Table.editor().tableId(id).setDefaultCharsetName("UTF8");
        editor.setComment("comment");
        editor.addColumn(
                Column.editor()
                        .name("id")
                        .jdbcType(1)
                        .nativeType(2)
                        .length(100)
                        .scale(30)
                        .create());
        editor.addColumn(
                Column.editor()
                        .name("value")
                        .jdbcType(2)
                        .nativeType(3)
                        .length(50)
                        .scale(30)
                        .create());

        editor.setPrimaryKeyNames(Arrays.asList("id"));
        return editor.create();
    }
}
