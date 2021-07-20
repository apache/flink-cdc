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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.INITIAL_OFFSET;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializerTest.assertSplitsEqual;
import static com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializerTest.getTestHistoryRecord;

/** Tests for {@link MySQLSplitState}. */
public class MySQLSplitStateTest {

    @Test
    public void testFromToSplit() {
        final MySQLSplit split =
                new MySQLSplit(
                        MySQLSplitKind.SNAPSHOT,
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        new BinlogOffset("mysql-bin.000001", 3L),
                        new BinlogOffset("mysql-bin.000002", 78L),
                        true,
                        INITIAL_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>());
        final MySQLSplitState mySQLSplitState = new MySQLSplitState(split);
        assertSplitsEqual(split, mySQLSplitState.toMySQLSplit());
    }

    @Test
    public void testRecordSnapshotSplitState() {
        final MySQLSplit split =
                new MySQLSplit(
                        MySQLSplitKind.SNAPSHOT,
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        null,
                        null,
                        false,
                        INITIAL_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>());
        final MySQLSplitState mySQLSplitState = new MySQLSplitState(split);
        mySQLSplitState.setLowWatermarkState(new BinlogOffset("mysql-bin.000001", 3L));
        mySQLSplitState.setHighWatermarkState(new BinlogOffset("mysql-bin.000002", 78L));
        mySQLSplitState.setSnapshotReadFinishedState(true);

        final MySQLSplit expected =
                new MySQLSplit(
                        MySQLSplitKind.SNAPSHOT,
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        new BinlogOffset("mysql-bin.000001", 3L),
                        new BinlogOffset("mysql-bin.000002", 78L),
                        true,
                        INITIAL_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>());
        assertSplitsEqual(expected, mySQLSplitState.toMySQLSplit());
    }

    @Test
    public void testRecordBinlogSplitState() throws Exception {

        final MySQLSplit split =
                getTestBinlogSplitWithOffset(new BinlogOffset("mysql-bin.000001", 4L));

        final MySQLSplitState mySQLSplitState = new MySQLSplitState(split);
        mySQLSplitState.setOffsetState(new BinlogOffset("mysql-bin.000001", 100L));

        assertSplitsEqual(
                getTestBinlogSplitWithOffset(new BinlogOffset("mysql-bin.000001", 100L)),
                mySQLSplitState.toMySQLSplit());

        mySQLSplitState.setOffsetState(new BinlogOffset("mysql-bin.000001", 400L));
        assertSplitsEqual(
                getTestBinlogSplitWithOffset(new BinlogOffset("mysql-bin.000001", 400L)),
                mySQLSplitState.toMySQLSplit());
    }

    private MySQLSplit getTestBinlogSplitWithOffset(BinlogOffset offset) throws Exception {
        final TableId tableId = TableId.parse("test_db.test_table");
        final List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> finishedSplitsInfo =
                new ArrayList<>();
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        new BinlogOffset("mysql-bin.000001", 4L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-1",
                        new Object[] {100},
                        new Object[] {200},
                        new BinlogOffset("mysql-bin.000001", 200L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-2",
                        new Object[] {200},
                        new Object[] {300},
                        new BinlogOffset("mysql-bin.000001", 600L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-3",
                        new Object[] {300},
                        null,
                        new BinlogOffset("mysql-bin.000001", 800L)));

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
                offset,
                finishedSplitsInfo,
                databaseHistory);
    }
}
