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
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toSuspendedBinlogSplit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for {@link MySqlSplitSerializer}. */
public class MySqlSplitSerializerTest {

    @Test
    public void testSnapshotSplit() throws Exception {
        final MySqlSplit split =
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
        assertEquals(split, serializeAndDeserializeSplit(split));
    }

    @Test
    public void testBinlogSplit() throws Exception {
        final TableId tableId = TableId.parse("test_db.test_table");
        final List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4L)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-1",
                        new Object[] {100},
                        new Object[] {200},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 200L)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-2",
                        new Object[] {200},
                        new Object[] {300},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 600L)));
        finishedSplitsInfo.add(
                new FinishedSnapshotSplitInfo(
                        tableId,
                        tableId + "-3",
                        new Object[] {300},
                        null,
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 800L)));

        final Map<TableId, TableChange> databaseHistory = new HashMap<>();
        databaseHistory.put(tableId, getTestTableSchema());

        final MySqlSplit split =
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4L),
                        BinlogOffset.ofNonStopping(),
                        finishedSplitsInfo,
                        databaseHistory,
                        finishedSplitsInfo.size());
        assertEquals(split, serializeAndDeserializeSplit(split));

        final MySqlSplit suspendedBinlogSplit = toSuspendedBinlogSplit(split.asBinlogSplit());
        assertEquals(suspendedBinlogSplit, serializeAndDeserializeSplit(suspendedBinlogSplit));

        final MySqlSplit unCompletedBinlogSplit =
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4L),
                        BinlogOffset.ofNonStopping(),
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        assertEquals(unCompletedBinlogSplit, serializeAndDeserializeSplit(unCompletedBinlogSplit));
    }

    @Test
    public void testRepeatedSerializationCache() throws Exception {
        final MySqlSplit split =
                new MySqlSnapshotSplit(
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-0",
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new BigIntType()))),
                        null,
                        new Object[] {99L},
                        null,
                        new HashMap<>());
        final byte[] ser1 = MySqlSplitSerializer.INSTANCE.serialize(split);
        final byte[] ser2 = MySqlSplitSerializer.INSTANCE.serialize(split);
        assertSame(ser1, ser2);
    }

    private MySqlSplit serializeAndDeserializeSplit(MySqlSplit split) throws Exception {
        final MySqlSplitSerializer sqlSplitSerializer = new MySqlSplitSerializer();
        byte[] serialized = sqlSplitSerializer.serialize(split);
        return sqlSplitSerializer.deserialize(sqlSplitSerializer.getVersion(), serialized);
    }

    public static TableChange getTestTableSchema() throws Exception {
        // the json string of a TableChange
        final String tableChangeJsonStr =
                "{\"type\":\"CREATE\",\"id\":\"\\\"test_db\\\".\\\"test_table\\\"\","
                        + "\"table\":{\"defaultCharsetName\":\"latin1\",\"primaryKeyColumnNames\":"
                        + "[\"card_no\",\"level\"],\"columns\":[{\"name\":\"card_no\",\"jdbcType\":-5,"
                        + "\"typeName\":\"BIGINT\",\"typeExpression\":\"BIGINT\",\"charsetName\":null,"
                        + "\"length\":20,\"position\":1,\"optional\":false,\"autoIncremented\":false,"
                        + "\"generated\":false},{\"name\":\"level\",\"jdbcType\":12,\"typeName\":"
                        + "\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\","
                        + "\"length\":10,\"position\":2,\"optional\":false,\"autoIncremented\":false,"
                        + "\"generated\":false},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\","
                        + "\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":255,"
                        + "\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":"
                        + "false},{\"name\":\"note\",\"jdbcType\":12,\"typeName\":\"VARCHAR\","
                        + "\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":1024,"
                        + "\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false}]}}";
        final Document doc = DocumentReader.defaultReader().read(tableChangeJsonStr);
        return FlinkJsonTableChangeSerializer.fromDocument(doc, true);
    }
}
