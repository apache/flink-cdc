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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator.BINLOG_SPLIT_ID;
import static com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.NO_STOPPING_OFFSET;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializerTest.assertSplitsEqual;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializerTest.getTestTableSchema;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for {@link MySqlSourceEnumStateSerializer}. */
public class MySqlParallelSourceEnumStateSerializerTest {

    @Test
    public void testsSerializeAndDeserialize() throws Exception {
        final MySqlSourceEnumState sourceEnumState = getTestSourceEnumState();

        assertSourceEnumStateEqual(
                sourceEnumState, serializeAndDeserializeSourceEnumState(sourceEnumState));
    }

    @Test
    public void testRepeatedSerializationCache() throws Exception {
        final MySqlSourceEnumState sourceEnumState = getTestSourceEnumState();
        final MySqlSourceEnumStateSerializer sourceEnumStateSerializer =
                new MySqlSourceEnumStateSerializer(MySqlSplitSerializer.INSTANCE);

        final byte[] ser1 = sourceEnumStateSerializer.serialize(sourceEnumState);
        final byte[] ser2 = sourceEnumStateSerializer.serialize(sourceEnumState);
        assertSame(ser1, ser2);
    }

    static MySqlSourceEnumState serializeAndDeserializeSourceEnumState(
            MySqlSourceEnumState sourceEnumState) throws Exception {
        final MySqlSourceEnumStateSerializer mySQLSourceEnumStateSerializer =
                new MySqlSourceEnumStateSerializer(MySqlSplitSerializer.INSTANCE);
        byte[] serialized = mySQLSourceEnumStateSerializer.serialize(sourceEnumState);
        return mySQLSourceEnumStateSerializer.deserialize(1, serialized);
    }

    private MySqlSourceEnumState getTestSourceEnumState() throws Exception {
        // construct the source that captures two tables
        // the first one has 3 snapshot splits and has been assigned finished
        // the second one has 4 snapshot splits and has been assigned 2 splits
        final TableId tableId0 = TableId.parse("test_db.test_table");
        final Collection<TableId> alreadyProcessedTables = new ArrayList<>();
        alreadyProcessedTables.add(tableId0);

        final Collection<MySqlSplit> remainingSplits = new ArrayList<>();
        final TableId tableId1 = TableId.parse("test_db.test_table1");
        alreadyProcessedTables.add(tableId1);
        remainingSplits.add(getTestSnapshotSplit(tableId1, 2));
        remainingSplits.add(getTestSnapshotSplit(tableId1, 3));

        final Map<Integer, List<MySqlSplit>> assignedSnapshotSplits = new HashMap<>();
        List<MySqlSplit> assignedSnapshotSplitsForTask0 = new ArrayList<>();
        assignedSnapshotSplitsForTask0.add(getTestSnapshotSplit(tableId0, 0));
        assignedSnapshotSplitsForTask0.add(getTestSnapshotSplit(tableId0, 1));
        assignedSnapshotSplitsForTask0.add(getTestSnapshotSplit(tableId0, 2));

        List<MySqlSplit> assignedSnapshotSplitsForTask1 = new ArrayList<>();
        assignedSnapshotSplitsForTask1.add(getTestSnapshotSplit(tableId1, 0));
        assignedSnapshotSplitsForTask1.add(getTestSnapshotSplit(tableId1, 1));

        assignedSnapshotSplits.put(0, assignedSnapshotSplitsForTask0);
        assignedSnapshotSplits.put(1, assignedSnapshotSplitsForTask1);

        final Map<Integer, List<Tuple2<String, BinlogOffset>>> finishedSnapshotSplits =
                new HashMap<>();
        List<Tuple2<String, BinlogOffset>> finishedSplitsForTask0 = new ArrayList<>();
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 0));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 1));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId1, 0));
        List<Tuple2<String, BinlogOffset>> finishedSplitsForTask1 = new ArrayList<>();
        finishedSplitsForTask0.add(getTestSplitInfo(tableId1, 1));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 2));
        finishedSnapshotSplits.put(0, finishedSplitsForTask0);
        finishedSnapshotSplits.put(1, finishedSplitsForTask1);

        final Map<Integer, List<MySqlSplit>> assignedBinlogSplits = new HashMap<>();
        List<MySqlSplit> assignedBinlogSplitsForTask0 = new ArrayList<>();
        assignedBinlogSplitsForTask0.add(getTestBinlogSplit(tableId0));
        assignedBinlogSplits.put(0, assignedBinlogSplitsForTask0);

        return new MySqlSourceEnumState(
                remainingSplits,
                alreadyProcessedTables,
                assignedSnapshotSplits,
                assignedBinlogSplits,
                finishedSnapshotSplits);
    }

    private MySqlSplit getTestSnapshotSplit(TableId tableId, int splitNo) {
        return new MySqlSnapshotSplit(
                tableId,
                tableId.toString() + "-" + splitNo,
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {100L + splitNo * 1000},
                new Object[] {999L + splitNo * 1000},
                new BinlogOffset("mysql-bin.000001", 78L + splitNo * 200),
                new HashMap<>());
    }

    private MySqlSplit getTestBinlogSplit(TableId tableId) throws Exception {
        final List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        new BinlogOffset("mysql-bin.000001", 0L)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-1",
                        new Object[] {100},
                        new Object[] {200},
                        new BinlogOffset("mysql-bin.000001", 200L)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-2",
                        new Object[] {200},
                        null,
                        new BinlogOffset("mysql-bin.000001", 400L)));

        final Map<TableId, TableChange> databaseHistory = new HashMap<>();
        databaseHistory.put(tableId, getTestTableSchema());
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("card_no", new VarCharType()))),
                new BinlogOffset("mysql-bin.000001", 0L),
                NO_STOPPING_OFFSET,
                finishedSplitsInfo,
                databaseHistory);
    }

    private Tuple2<String, BinlogOffset> getTestSplitInfo(TableId tableId, int splitNo) {
        final String splitId = tableId.toString() + "-" + splitNo;
        final BinlogOffset highWatermark =
                new BinlogOffset("mysql-bin.000001", (long) splitNo * 200);
        return Tuple2.of(splitId, highWatermark);
    }

    static void assertSourceEnumStateEqual(
            MySqlSourceEnumState expected, MySqlSourceEnumState actual) {
        assertArrayEquals(
                expected.getAlreadyProcessedTables().toArray(),
                actual.getAlreadyProcessedTables().toArray());

        List<MySqlSplit> expectedSplits = new ArrayList<>(expected.getRemainingSplits());
        List<MySqlSplit> actualSplits = new ArrayList<>(actual.getRemainingSplits());
        assertSplitsEquals(expectedSplits, actualSplits);

        assertEquals(
                expected.getAssignedSnapshotSplits().size(),
                actual.getAssignedSnapshotSplits().size());
        for (Map.Entry<Integer, List<MySqlSplit>> entry :
                expected.getAssignedSnapshotSplits().entrySet()) {
            assertSplitsEquals(
                    entry.getValue(), actual.getAssignedSnapshotSplits().get(entry.getKey()));
        }

        assertEquals(
                expected.getFinishedSnapshotSplits().size(),
                actual.getFinishedSnapshotSplits().size());
        for (Map.Entry<Integer, List<Tuple2<String, BinlogOffset>>> entry :
                expected.getFinishedSnapshotSplits().entrySet()) {
            assertEquals(
                    entry.getValue().toString(),
                    actual.getFinishedSnapshotSplits().get(entry.getKey()).toString());
        }
    }

    private static void assertSplitsEquals(
            List<MySqlSplit> expectedSplits, List<MySqlSplit> actualSplits) {
        assertEquals(expectedSplits.size(), actualSplits.size());
        for (int i = 0; i < expectedSplits.size(); i++) {
            assertSplitsEqual(expectedSplits.get(i), actualSplits.get(i));
        }
    }
}
