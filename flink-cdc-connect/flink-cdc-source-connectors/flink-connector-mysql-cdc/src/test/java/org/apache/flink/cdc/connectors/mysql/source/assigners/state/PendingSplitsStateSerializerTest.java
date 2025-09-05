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

package org.apache.flink.cdc.connectors.mysql.source.assigners.state;

import org.apache.flink.cdc.connectors.mysql.source.assigners.AssignerStatus;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit.generateSplitId;

/**
 * Tests for {@link
 * org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer}.
 */
class PendingSplitsStateSerializerTest {

    private static final TableId tableId0 = TableId.parse("test_db.test_table");
    private static final TableId tableId1 = TableId.parse("test_db.\"test_table 1\"");
    private static final TableId tableId2 = TableId.parse("test_db.\"test_table 2\"");

    public static Stream<Arguments> params() {
        return Stream.of(
                Arguments.of(getTestSnapshotPendingSplitsState(true)),
                Arguments.of(getTestSnapshotPendingSplitsState(false)),
                Arguments.of(getTestHybridPendingSplitsState(false)),
                Arguments.of(getTestHybridPendingSplitsState(true)),
                Arguments.of(getTestBinlogPendingSplitsState()));
    }

    @ParameterizedTest
    @MethodSource("params")
    void testsSerializeAndDeserialize(PendingSplitsState state) throws Exception {
        Assertions.assertThat(serializeAndDeserializeSourceEnumState(state)).isEqualTo(state);
    }

    @ParameterizedTest
    @MethodSource("params")
    void testTableSchemasAfterSerializeAndDeserialize(PendingSplitsState state) throws Exception {
        PendingSplitsState pendingSplitsState = serializeAndDeserializeSourceEnumState(state);
        if (pendingSplitsState instanceof SnapshotPendingSplitsState) {
            Assertions.assertThat(
                            ((SnapshotPendingSplitsState) pendingSplitsState)
                                    .getTableSchemas()
                                    .keySet())
                    .isEqualTo(getTestTableSchema(tableId0, tableId1).keySet());
        } else if (pendingSplitsState instanceof HybridPendingSplitsState) {
            Assertions.assertThat(
                            ((HybridPendingSplitsState) pendingSplitsState)
                                    .getSnapshotPendingSplits()
                                    .getTableSchemas()
                                    .keySet())
                    .isEqualTo(getTestTableSchema(tableId0, tableId1).keySet());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testRepeatedSerializationCache(PendingSplitsState state) throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE);

        final byte[] ser1 = serializer.serialize(state);
        final byte[] ser2 = serializer.serialize(state);
        final byte[] ser3 = state.serializedFormCache;
        Assertions.assertThat(ser1).isSameAs(ser2).isSameAs(ser3);
    }

    @ParameterizedTest
    @MethodSource("params")
    void testOutputIsFinallyCleared(PendingSplitsState state) throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE);

        final byte[] ser1 = serializer.serialize(state);
        state.serializedFormCache = null;

        PendingSplitsState unsupportedState = new UnsupportedPendingSplitsState();

        Assertions.assertThatThrownBy(() -> serializer.serialize(unsupportedState))
                .isExactlyInstanceOf(IOException.class);

        final byte[] ser2 = serializer.serialize(state);
        Assertions.assertThat(ser1).isEqualTo(ser2);
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
                generateSplitId(tableId, splitNo),
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
        public String comment() {
            return "";
        }

        @Override
        public TableEditor edit() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    /** An implementation for {@link PendingSplitsState} which will cause a serialization error. */
    static class UnsupportedPendingSplitsState extends PendingSplitsState {}
}
