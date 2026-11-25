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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
                Arguments.of(getTestSnapshotPendingSplitsStateWithCheckpointIdToFinish()),
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
    void testCheckpointIdToFinishRoundTrip(PendingSplitsState state) throws Exception {
        PendingSplitsState deserialized = serializeAndDeserializeSourceEnumState(state);
        if (state instanceof SnapshotPendingSplitsState) {
            Assertions.assertThat(
                            ((SnapshotPendingSplitsState) deserialized).getCheckpointIdToFinish())
                    .isEqualTo(((SnapshotPendingSplitsState) state).getCheckpointIdToFinish());
        } else if (state instanceof HybridPendingSplitsState) {
            Assertions.assertThat(
                            ((HybridPendingSplitsState) deserialized)
                                    .getSnapshotPendingSplits()
                                    .getCheckpointIdToFinish())
                    .isEqualTo(
                            ((HybridPendingSplitsState) state)
                                    .getSnapshotPendingSplits()
                                    .getCheckpointIdToFinish());
        }
    }

    @Test
    void testDeserializeV5MissingCheckpointIdToFinish() throws Exception {
        // FLINK-39478: v5 read path predates checkpointIdToFinish; deserializing a payload as v5
        // must not attempt to read the new trailing field and must return null for it.
        SnapshotPendingSplitsState state = getTestSnapshotPendingSplitsState(false);
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);
        byte[] serialized = serializer.serialize(state);
        PendingSplitsState restored = serializer.deserialize(5, serialized);
        Assertions.assertThat(restored).isInstanceOf(SnapshotPendingSplitsState.class);
        Assertions.assertThat(((SnapshotPendingSplitsState) restored).getCheckpointIdToFinish())
                .isNull();
    }

    @ParameterizedTest
    @MethodSource("params")
    void testRepeatedSerializationCache(PendingSplitsState state) throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);

        final byte[] ser1 = serializer.serialize(state);
        final byte[] ser2 = serializer.serialize(state);
        final byte[] ser3 = state.serializedFormCache;
        Assertions.assertThat(ser1).isSameAs(ser2).isSameAs(ser3);
    }

    @ParameterizedTest
    @MethodSource("params")
    void testOutputIsFinallyCleared(PendingSplitsState state) throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);

        final byte[] ser1 = serializer.serialize(state);
        state.serializedFormCache = null;

        PendingSplitsState unsupportedState = new UnsupportedPendingSplitsState();

        Assertions.assertThatThrownBy(() -> serializer.serialize(unsupportedState))
                .isExactlyInstanceOf(IOException.class);

        final byte[] ser2 = serializer.serialize(state);
        Assertions.assertThat(ser1).isEqualTo(ser2);
    }

    @Test
    void testSerializeAndDeserializeReleasedHybridState() throws Exception {
        // The "light" state produced after releasing the snapshot metadata must round-trip, and
        // the persisted released flag (PendingSplitsStateSerializer v7) must survive.
        HybridPendingSplitsState released = getTestReleasedHybridPendingSplitsState();
        PendingSplitsState roundTripped = serializeAndDeserializeSourceEnumState(released);
        Assertions.assertThat(roundTripped).isEqualTo(released);

        SnapshotPendingSplitsState snapshot =
                ((HybridPendingSplitsState) roundTripped).getSnapshotPendingSplits();
        Assertions.assertThat(snapshot.getAssignedSplits()).isEmpty();
        Assertions.assertThat(snapshot.getSplitFinishedOffsets()).isEmpty();
        Assertions.assertThat(snapshot.getTableSchemas()).isEmpty();
        Assertions.assertThat(snapshot.getAlreadyProcessedTables()).isNotEmpty();
        Assertions.assertThat(snapshot.isSnapshotMetaReleased()).isTrue();
        Assertions.assertThat(((HybridPendingSplitsState) roundTripped).isBinlogSplitAssigned())
                .isTrue();
    }

    @Test
    void testDeserializeV5SnapshotStateDefaultsReleasedFlagToFalse() throws Exception {
        // A v5 checkpoint written before FLINK-39775 carries neither the checkpointIdToFinish
        // field nor the released flag. For a snapshot state those are the last two bytes written
        // by the v7 serializer, so a v7 stream without its final two bytes is exactly the v5 wire
        // format. Deserializing it under version 5 must default snapshotMetaReleased to false
        // and checkpointIdToFinish to null, and otherwise round-trip unchanged.
        SnapshotPendingSplitsState state = getTestSnapshotPendingSplitsState(false);
        PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);
        byte[] v7 = serializer.serialize(state);
        byte[] v5 = Arrays.copyOf(v7, v7.length - 2);

        PendingSplitsState restored = serializer.deserialize(5, v5);

        Assertions.assertThat(restored).isInstanceOf(SnapshotPendingSplitsState.class);
        Assertions.assertThat(((SnapshotPendingSplitsState) restored).isSnapshotMetaReleased())
                .isFalse();
        Assertions.assertThat(restored).isEqualTo(state);
    }

    @Test
    void testSerializersAlwaysWriteV7Format() throws Exception {
        // FLINK-39478: the checkpointIdToFinish field must be persisted in every checkpoint, and
        // getVersion() cannot depend on the state being serialized, so both serializers now write
        // version 7 regardless of the release option. Versions 5 and 6 remain readable: reading a
        // v7 payload back under those version stamps defaults the fields they do not carry.
        SnapshotPendingSplitsState state = getTestSnapshotPendingSplitsState(false);
        PendingSplitsStateSerializer releaseOffSerializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, false);
        PendingSplitsStateSerializer releaseOnSerializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);

        Assertions.assertThat(releaseOffSerializer.getVersion()).isEqualTo(7);
        Assertions.assertThat(releaseOnSerializer.getVersion()).isEqualTo(7);

        byte[] releaseOnBytes = releaseOnSerializer.serialize(state);
        // Clear the lazily cached serialized form so the other serializer re-serializes the state.
        state.serializedFormCache = null;
        byte[] releaseOffBytes = releaseOffSerializer.serialize(state);
        Assertions.assertThat(releaseOffBytes).isEqualTo(releaseOnBytes);

        // A v7 payload read back as v5 or v6 defaults the fields those versions do not carry.
        PendingSplitsState asV5 = releaseOffSerializer.deserialize(5, releaseOnBytes);
        Assertions.assertThat(asV5).isEqualTo(state);
        PendingSplitsState asV6 = releaseOffSerializer.deserialize(6, releaseOnBytes);
        Assertions.assertThat(asV6).isEqualTo(state);
        Assertions.assertThat(((SnapshotPendingSplitsState) asV6).getCheckpointIdToFinish())
                .isNull();
    }

    @Test
    void testReleaseDisabledSerializerRoundTripsHybridState() throws Exception {
        // The option-off serializer round-trips a hybrid state at v7 (both new fields are written
        // and read back), leaving the released flag false for a state that never released.
        HybridPendingSplitsState state = getTestHybridPendingSplitsState(false);
        PendingSplitsStateSerializer v5Serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, false);

        byte[] serialized = v5Serializer.serialize(state);
        PendingSplitsState restored =
                v5Serializer.deserialize(v5Serializer.getVersion(), serialized);

        Assertions.assertThat(restored).isEqualTo(state);
        Assertions.assertThat(
                        ((HybridPendingSplitsState) restored)
                                .getSnapshotPendingSplits()
                                .isSnapshotMetaReleased())
                .isFalse();
    }

    static PendingSplitsState serializeAndDeserializeSourceEnumState(PendingSplitsState state)
            throws Exception {
        final PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(MySqlSplitSerializer.INSTANCE, true);
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

        final LinkedHashMap<String, MySqlSchemalessSnapshotSplit> assignedSnapshotSplits =
                new LinkedHashMap<>();
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

    private static SnapshotPendingSplitsState
            getTestSnapshotPendingSplitsStateWithCheckpointIdToFinish() {
        // Reuse the base state and overlay a non-null checkpointIdToFinish, exercising the
        // FLINK-39478 serialization path (version 7).
        SnapshotPendingSplitsState base = getTestSnapshotPendingSplitsState(false);
        return new SnapshotPendingSplitsState(
                base.getAlreadyProcessedTables(),
                base.getRemainingSplits(),
                base.getAssignedSplits(),
                base.getTableSchemas(),
                base.getSplitFinishedOffsets(),
                base.getSnapshotAssignerStatus(),
                base.getRemainingTables(),
                base.isTableIdCaseSensitive(),
                base.isRemainingTablesCheckpointed(),
                base.getChunkSplitterState(),
                42L);
    }

    private static HybridPendingSplitsState getTestHybridPendingSplitsState(
            boolean checkpointWhenSplitting) {
        return new HybridPendingSplitsState(
                getTestSnapshotPendingSplitsState(checkpointWhenSplitting), false);
    }

    private static HybridPendingSplitsState getTestReleasedHybridPendingSplitsState() {
        // After releasing the snapshot metadata, the hybrid state is just a normal state
        // with empty assigned/finished/schema maps and isBinlogSplitAssigned=true.
        final List<TableId> alreadyProcessedTables = new ArrayList<>();
        alreadyProcessedTables.add(tableId0);
        alreadyProcessedTables.add(tableId1);
        SnapshotPendingSplitsState snapshotState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        new ArrayList<>(),
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE,
                        true);
        return new HybridPendingSplitsState(snapshotState, true);
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
