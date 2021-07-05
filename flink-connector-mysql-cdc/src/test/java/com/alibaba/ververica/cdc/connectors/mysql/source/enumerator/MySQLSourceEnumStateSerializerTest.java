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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitKind;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializer;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition.INITIAL_OFFSET;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializerTest.assertSplitsEqual;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializerTest.getTestHistoryRecord;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for {@link MySQLSourceEnumStateSerializer}. */
public class MySQLSourceEnumStateSerializerTest {

    @Test
    public void testsSerializeAndDeserialize() throws Exception {
        final MySQLSourceEnumState sourceEnumState = getTestSourceEnumState();

        assertSourceEnumStateEqual(
                sourceEnumState, serializeAndDeserializeSourceEnumState(sourceEnumState));
    }

    @Test
    public void testRepeatedSerializationCache() throws Exception {
        final MySQLSourceEnumState sourceEnumState = getTestSourceEnumState();
        final MySQLSourceEnumStateSerializer sourceEnumStateSerializer =
                new MySQLSourceEnumStateSerializer(MySQLSplitSerializer.INSTANCE);

        final byte[] ser1 = sourceEnumStateSerializer.serialize(sourceEnumState);
        final byte[] ser2 = sourceEnumStateSerializer.serialize(sourceEnumState);
        assertSame(ser1, ser2);
    }

    static MySQLSourceEnumState serializeAndDeserializeSourceEnumState(
            MySQLSourceEnumState sourceEnumState) throws Exception {
        final MySQLSourceEnumStateSerializer mySQLSourceEnumStateSerializer =
                new MySQLSourceEnumStateSerializer(MySQLSplitSerializer.INSTANCE);
        byte[] serialized = mySQLSourceEnumStateSerializer.serialize(sourceEnumState);
        return mySQLSourceEnumStateSerializer.deserialize(1, serialized);
    }

    private MySQLSourceEnumState getTestSourceEnumState() throws Exception {
        // construct the source that captures two tables
        // the first one has 3 snapshot splits and has been assigned finished
        // the second one has 4 snapshot splits and has been assigned 2 splits
        final TableId tableId0 = TableId.parse("test_db.test_table");
        final Collection<TableId> alreadyProcessedTables = new ArrayList<>();
        alreadyProcessedTables.add(tableId0);

        final Collection<MySQLSplit> remainingSplits = new ArrayList<>();
        final TableId tableId1 = TableId.parse("test_db.test_table1");
        remainingSplits.add(getTestSplit(tableId1, 2));
        remainingSplits.add(getTestSplit(tableId1, 3));

        final Map<Integer, List<MySQLSplit>> assignedSplits = new HashMap<>();
        List<MySQLSplit> assignedSplitsForTask0 = new ArrayList<>();
        assignedSplitsForTask0.add(getTestSplit(tableId0, 0));
        assignedSplitsForTask0.add(getTestSplit(tableId0, 1));
        assignedSplitsForTask0.add(getTestSplit(tableId0, 2));
        assignedSplitsForTask0.add(getTestBinlogSplit(tableId0));

        List<MySQLSplit> assignedSplitsForTask1 = new ArrayList<>();
        assignedSplitsForTask1.add(getTestSplit(tableId1, 0));
        assignedSplitsForTask1.add(getTestSplit(tableId1, 1));

        assignedSplits.put(0, assignedSplitsForTask0);
        assignedSplits.put(1, assignedSplitsForTask1);

        final Map<Integer, List<Tuple2<String, BinlogPosition>>> finishedSnapshotSplits =
                new HashMap<>();
        List<Tuple2<String, BinlogPosition>> finishedSplitsForTask0 = new ArrayList<>();
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 0));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 1));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId1, 0));
        List<Tuple2<String, BinlogPosition>> finishedSplitsForTask1 = new ArrayList<>();
        finishedSplitsForTask0.add(getTestSplitInfo(tableId1, 1));
        finishedSplitsForTask0.add(getTestSplitInfo(tableId0, 2));
        finishedSnapshotSplits.put(0, finishedSplitsForTask0);
        finishedSnapshotSplits.put(1, finishedSplitsForTask1);

        return new MySQLSourceEnumState(
                remainingSplits, alreadyProcessedTables, assignedSplits, finishedSnapshotSplits);
    }

    private MySQLSplit getTestSplit(TableId tableId, int splitNo) {
        return new MySQLSplit(
                MySQLSplitKind.SNAPSHOT,
                tableId,
                tableId.toString() + "-" + splitNo,
                new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {100L + splitNo * 1000},
                new Object[] {999L + splitNo * 1000},
                new BinlogPosition("mysql-bin.000001", 3L + splitNo * 200),
                new BinlogPosition("mysql-bin.000001", 78L + splitNo * 200),
                true,
                INITIAL_OFFSET,
                new ArrayList<>(),
                new HashMap<>());
    }

    private MySQLSplit getTestBinlogSplit(TableId tableId) throws Exception {
        final List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo =
                new ArrayList<>();
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        new BinlogPosition("mysql-bin.000001", 0L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-1",
                        new Object[] {100},
                        new Object[] {200},
                        new BinlogPosition("mysql-bin.000001", 200L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-2",
                        new Object[] {200},
                        null,
                        new BinlogPosition("mysql-bin.000001", 400L)));

        final Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();
        databaseHistory.put(tableId, getTestHistoryRecord());
        return new MySQLSplit(
                MySQLSplitKind.BINLOG,
                tableId,
                "binlog-split-0",
                new RowType(Arrays.asList(new RowType.RowField("card_no", new VarCharType()))),
                null,
                null,
                null,
                null,
                true,
                new BinlogPosition("mysql-bin.000001", 0L),
                finishedSplitsInfo,
                databaseHistory);
    }

    private Tuple2<String, BinlogPosition> getTestSplitInfo(TableId tableId, int splitNo) {
        final String splitId = tableId.toString() + "-" + splitNo;
        final BinlogPosition highWatermark =
                new BinlogPosition("mysql-bin.000001", 0L + splitNo * 200);
        return Tuple2.of(splitId, highWatermark);
    }

    static void assertSourceEnumStateEqual(
            MySQLSourceEnumState expected, MySQLSourceEnumState actual) {
        assertArrayEquals(
                expected.getAlreadyProcessedTables().toArray(),
                actual.getAlreadyProcessedTables().toArray());

        List<MySQLSplit> expectedSplits = new ArrayList<>(expected.getRemainingSplits());
        List<MySQLSplit> actualSplits = new ArrayList<>(actual.getRemainingSplits());
        assertSplitsEquals(expectedSplits, actualSplits);

        assertEquals(expected.getAssignedSplits().size(), actual.getAssignedSplits().size());
        for (Map.Entry<Integer, List<MySQLSplit>> entry : expected.getAssignedSplits().entrySet()) {
            assertSplitsEquals(entry.getValue(), actual.getAssignedSplits().get(entry.getKey()));
        }

        assertEquals(
                expected.getFinishedSnapshotSplits().size(),
                actual.getFinishedSnapshotSplits().size());
        for (Map.Entry<Integer, List<Tuple2<String, BinlogPosition>>> entry :
                expected.getFinishedSnapshotSplits().entrySet()) {
            assertEquals(
                    entry.getValue().toString(),
                    actual.getFinishedSnapshotSplits().get(entry.getKey()).toString());
        }
    }

    private static void assertSplitsEquals(
            List<MySQLSplit> expectedSplits, List<MySQLSplit> actualSplits) {
        assertEquals(expectedSplits.size(), actualSplits.size());
        for (int i = 0; i < expectedSplits.size(); i++) {
            assertSplitsEqual(expectedSplits.get(i), actualSplits.get(i));
        }
    }
}
