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

package com.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializerTest.getTestTableSchema;
import static org.junit.Assert.assertEquals;

/** Tests for {@link MySqlSplitState}. */
public class MySqlSplitStateTest {

    @Test
    public void testFromToSplit() {
        final MySqlSnapshotSplit split =
                new MySqlSnapshotSplit(
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000002", 78L),
                        new HashMap<>());
        final MySqlSnapshotSplitState mySqlSplitState = new MySqlSnapshotSplitState(split);
        assertEquals(split, mySqlSplitState.toMySqlSplit());
    }

    @Test
    public void testRecordSnapshotSplitState() {
        final MySqlSnapshotSplit split =
                new MySqlSnapshotSplit(
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        null,
                        new HashMap<>());
        final MySqlSnapshotSplitState mySqlSplitState = new MySqlSnapshotSplitState(split);
        mySqlSplitState.setHighWatermark(
                BinlogOffset.ofBinlogFilePosition("mysql-bin.000002", 78L));

        final MySqlSnapshotSplit expected =
                new MySqlSnapshotSplit(
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000002", 78L),
                        new HashMap<>());
        assertEquals(expected, mySqlSplitState.toMySqlSplit());
    }

    @Test
    public void testRecordBinlogSplitState() throws Exception {

        final MySqlBinlogSplit split =
                getTestBinlogSplitWithOffset(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4));

        final MySqlBinlogSplitState mySqlSplitState = new MySqlBinlogSplitState(split);
        mySqlSplitState.setStartingOffset(
                BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 100));

        assertEquals(
                getTestBinlogSplitWithOffset(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 100)),
                mySqlSplitState.toMySqlSplit());

        mySqlSplitState.setStartingOffset(
                BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 400));
        assertEquals(
                getTestBinlogSplitWithOffset(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 400)),
                mySqlSplitState.toMySqlSplit());
    }

    private MySqlBinlogSplit getTestBinlogSplitWithOffset(BinlogOffset startingOffset)
            throws Exception {
        final TableId tableId = TableId.parse("test_db.test_table");
        final List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-1",
                        new Object[] {100},
                        new Object[] {200},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 200)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-2",
                        new Object[] {200},
                        new Object[] {300},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 600)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-3",
                        new Object[] {300},
                        null,
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 800)));

        final Map<TableId, TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(tableId, getTestTableSchema());

        return new MySqlBinlogSplit(
                "binlog-split",
                startingOffset,
                BinlogOffset.ofNonStopping(),
                finishedSplitsInfo,
                tableSchemas,
                finishedSplitsInfo.size());
    }
}
