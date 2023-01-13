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

package com.ververica.cdc.connectors.mysql.source.assigners.state;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for {@link PendingSplitsStateSerializer}. */
@RunWith(Parameterized.class)
public class PendingSplitsStateSerializerTest {

    private static final TableId tableId0 = TableId.parse("test_db.test_table");
    private static final TableId tableId1 = TableId.parse("test_db.test_table1");
    private static final TableId tableId2 = TableId.parse("test_db.test_table2");

    @Parameterized.Parameter public PendingSplitsState state;

    @Parameterized.Parameters(name = "PendingSplitsState = {index}")
    public static Collection<PendingSplitsState> params() {
        return Arrays.asList(
                getTestSnapshotPendingSplitsState(true),
                getTestSnapshotPendingSplitsState(false),
                getTestHybridPendingSplitsState(false),
                getTestHybridPendingSplitsState(true),
                getTestBinlogPendingSplitsState());
    }

    @Test
    public void testsSerializeAndDeserialize() throws Exception {
        assertEquals(state, serializeAndDeserializeSourceEnumState(state));
    }

    @Test
    public void testTableSchemasAfterSerializeAndDeserialize() throws Exception {
        PendingSplitsState pendingSplitsState = serializeAndDeserializeSourceEnumState(state);
        if (pendingSplitsState instanceof SnapshotPendingSplitsState) {
            assertEquals(
                    getTestTableSchema(tableId0, tableId1).keySet(),
                    ((SnapshotPendingSplitsState) pendingSplitsState).getTableSchemas().keySet());
        } else if (pendingSplitsState instanceof HybridPendingSplitsState) {
            assertEquals(
                    getTestTableSchema(tableId0, tableId1).keySet(),
                    ((HybridPendingSplitsState) pendingSplitsState)
                            .getSnapshotPendingSplits()
                            .getTableSchemas()
                            .keySet());
        }
    }

    @Test
    public void testRepeatedSerializationCache() throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE);

        final byte[] ser1 = serializer.serialize(state);
        final byte[] ser2 = serializer.serialize(state);
        final byte[] ser3 = state.serializedFormCache;
        assertSame(ser1, ser2);
        assertSame(ser1, ser3);
    }

    static PendingSplitsState serializeAndDeserializeSourceEnumState(PendingSplitsState state)
            throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE);
        byte[] serialized = serializer.serialize(state);
        return serializer.deserialize(serializer.getVersion(), serialized);
    }

    private static SnapshotPendingSplitsState getTestSnapshotPendingSplitsState(
            boolean checkpointWhenSplitting) {
        // construct the source that captures three tables
        // the first table has 3 snapshot splits and has been assigned finished
        // the second table has 4 snapshot splits and has been assigned 2 splits
        // the third table has not assigned yet
        final List<TableId> alreadyProcessedTables = new ArrayList<>();
        final List<TableId> remainingTables = new ArrayList<>();

        final List<MySqlSchemalessSnapshotSplit> remainingSplits = new ArrayList<>();

        alreadyProcessedTables.add(tableId0);
        alreadyProcessedTables.add(tableId1);

        remainingTables.add(tableId2);

        remainingSplits.add(getTestSchemalessSnapshotSplit(tableId1, 2));
        remainingSplits.add(getTestSchemalessSnapshotSplit(tableId1, 3));

        final Map<String, MySqlSchemalessSnapshotSplit> assignedSnapshotSplits = new HashMap<>();
        Arrays.asList(
                        getTestSchemalessSnapshotSplit(tableId0, 0),
                        getTestSchemalessSnapshotSplit(tableId0, 1),
                        getTestSchemalessSnapshotSplit(tableId0, 2),
                        getTestSchemalessSnapshotSplit(tableId1, 0),
                        getTestSchemalessSnapshotSplit(tableId1, 1))
                .forEach(split -> assignedSnapshotSplits.put(split.splitId(), split));

        Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
        Arrays.asList(
                        getTestSplitInfo(tableId0, 0),
                        getTestSplitInfo(tableId0, 1),
                        getTestSplitInfo(tableId1, 0),
                        getTestSplitInfo(tableId1, 1),
                        getTestSplitInfo(tableId0, 2))
                .forEach(finishedOffsets::putAll);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                getTestTableSchema(tableId0, tableId1);

        if (checkpointWhenSplitting) {
            return new SnapshotPendingSplitsState(
                    alreadyProcessedTables,
                    remainingSplits,
                    assignedSnapshotSplits,
                    tableSchemas,
                    finishedOffsets,
                    AssignerStatus.INITIAL_ASSIGNING,
                    remainingTables,
                    false,
                    true,
                    new ChunkSplitterState(
                            tableId1, ChunkSplitterState.ChunkBound.middleOf("test"), 3));
        } else {
            return new SnapshotPendingSplitsState(
                    alreadyProcessedTables,
                    remainingSplits,
                    assignedSnapshotSplits,
                    tableSchemas,
                    finishedOffsets,
                    AssignerStatus.INITIAL_ASSIGNING,
                    remainingTables,
                    false,
                    true,
                    ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        }
    }

    private static HybridPendingSplitsState getTestHybridPendingSplitsState(
            boolean checkpointWhenSplitting) {
        return new HybridPendingSplitsState(
                getTestSnapshotPendingSplitsState(checkpointWhenSplitting), false);
    }

    private static BinlogPendingSplitsState getTestBinlogPendingSplitsState() {
        return new BinlogPendingSplitsState(true);
    }

    private static MySqlSchemalessSnapshotSplit getTestSchemalessSnapshotSplit(
            TableId tableId, int splitNo) {
        return new MySqlSchemalessSnapshotSplit(
                tableId,
                tableId.toString() + "-" + splitNo,
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {100L + splitNo * 1000L},
                new Object[] {999L + splitNo * 1000L},
                BinlogOffset.builder()
                        .setBinlogFilePosition("mysql-bin.000001", 78L + splitNo * 200L)
                        .setSkipEvents(splitNo)
                        .build());
    }

    private static Map<String, BinlogOffset> getTestSplitInfo(TableId tableId, int splitNo) {
        final String splitId = tableId.toString() + "-" + splitNo;
        final BinlogOffset highWatermark =
                BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", splitNo * 200L);
        return Collections.singletonMap(splitId, highWatermark);
    }

    private static Map<TableId, TableChanges.TableChange> getTestTableSchema(TableId... tableIds) {
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        for (TableId tableId : tableIds) {
            tableSchemas.put(
                    tableId,
                    new TableChanges.TableChange(
                            TableChanges.TableChangeType.CREATE, new TestTableImpl(tableId)));
        }

        return tableSchemas;
    }

    /** An implementation for {@link Table} which is used for tests. */
    private static class TestTableImpl implements Table {

        private final TableId tableId;

        public TestTableImpl(TableId tableId) {
            this.tableId = tableId;
        }

        @Override
        public TableId id() {
            return tableId;
        }

        @Override
        public List<String> primaryKeyColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<String> retrieveColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<Column> columns() {
            return Collections.emptyList();
        }

        @Override
        public Column columnWithName(String name) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String defaultCharsetName() {
            return "UTF-8";
        }

        @Override
        public TableEditor edit() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
