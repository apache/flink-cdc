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
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import io.debezium.relational.TableId;
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

    @Parameterized.Parameter public PendingSplitsState state;

    @Parameterized.Parameters(name = "PendingSplitsState = {index}")
    public static Collection<PendingSplitsState> params() {
        return Arrays.asList(
                getTestSnapshotPendingSplitsState(),
                getTestHybridPendingSplitsState(),
                getTestBinlogPendingSplitsState());
    }

    @Test
    public void testsSerializeAndDeserialize() throws Exception {
        assertEquals(state, serializeAndDeserializeSourceEnumState(state));
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

    private static SnapshotPendingSplitsState getTestSnapshotPendingSplitsState() {
        // construct the source that captures three tables
        // the first table has 3 snapshot splits and has been assigned finished
        // the second table has 4 snapshot splits and has been assigned 2 splits
        // the third table has not assigned yet
        final List<TableId> alreadyProcessedTables = new ArrayList<>();
        final List<TableId> remainingTables = new ArrayList<>();

        final List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();

        final TableId tableId0 = TableId.parse("test_db.test_table");
        final TableId tableId1 = TableId.parse("test_db.test_table1");
        final TableId tableId2 = TableId.parse("test_db.test_table2");

        alreadyProcessedTables.add(tableId0);
        alreadyProcessedTables.add(tableId1);

        remainingTables.add(tableId2);

        remainingSplits.add(getTestSnapshotSplit(tableId1, 2));
        remainingSplits.add(getTestSnapshotSplit(tableId1, 3));

        final Map<String, MySqlSnapshotSplit> assignedSnapshotSplits = new HashMap<>();
        Arrays.asList(
                        getTestSnapshotSplit(tableId0, 0),
                        getTestSnapshotSplit(tableId0, 1),
                        getTestSnapshotSplit(tableId0, 2),
                        getTestSnapshotSplit(tableId1, 0),
                        getTestSnapshotSplit(tableId1, 1))
                .forEach(split -> assignedSnapshotSplits.put(split.splitId(), split));

        Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
        Arrays.asList(
                        getTestSplitInfo(tableId0, 0),
                        getTestSplitInfo(tableId0, 1),
                        getTestSplitInfo(tableId1, 0),
                        getTestSplitInfo(tableId1, 1),
                        getTestSplitInfo(tableId0, 2))
                .forEach(finishedOffsets::putAll);

        return new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                remainingSplits,
                assignedSnapshotSplits,
                finishedOffsets,
                AssignerStatus.INITIAL_ASSIGNING,
                remainingTables,
                false,
                true);
    }

    private static HybridPendingSplitsState getTestHybridPendingSplitsState() {
        return new HybridPendingSplitsState(getTestSnapshotPendingSplitsState(), false);
    }

    private static BinlogPendingSplitsState getTestBinlogPendingSplitsState() {
        return new BinlogPendingSplitsState(true);
    }

    private static MySqlSnapshotSplit getTestSnapshotSplit(TableId tableId, int splitNo) {
        long restartSkipEvent = splitNo;
        return new MySqlSnapshotSplit(
                tableId,
                tableId.toString() + "-" + splitNo,
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {100L + splitNo * 1000},
                new Object[] {999L + splitNo * 1000},
                new BinlogOffset(
                        "mysql-bin.000001", 78L + splitNo * 200, restartSkipEvent, 0L, 0L, null, 0),
                new HashMap<>());
    }

    private static Map<String, BinlogOffset> getTestSplitInfo(TableId tableId, int splitNo) {
        final String splitId = tableId.toString() + "-" + splitNo;
        final BinlogOffset highWatermark =
                new BinlogOffset("mysql-bin.000001", (long) splitNo * 200);
        return Collections.singletonMap(splitId, highWatermark);
    }
}
