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

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition.INITIAL_OFFSET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for {@link MySQLSplitSerializer}. */
public class MySQLSplitSerializerTest {

    @Test
    public void testSnapshotSplit() throws Exception {
        final MySQLSplit split =
                new MySQLSplit(
                        MySQLSplitKind.SNAPSHOT,
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-1",
                        new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                        new Object[] {100L},
                        new Object[] {999L},
                        new BinlogPosition("mysql-bin.000001", 3L),
                        new BinlogPosition("mysql-bin.000002", 78L),
                        true,
                        INITIAL_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>());
        assertSplitsEqual(split, serializeAndDeserializeSplit(split));
    }

    @Test
    public void testBinlogSplit() throws Exception {
        final TableId tableId = TableId.parse("test_db.test_table");
        final List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo =
                new ArrayList<>();
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-0",
                        null,
                        new Object[] {100},
                        new BinlogPosition("mysql-bin.000001", 4L)));
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
                        new Object[] {300},
                        new BinlogPosition("mysql-bin.000001", 600L)));
        finishedSplitsInfo.add(
                Tuple5.of(
                        tableId,
                        tableId + "-3",
                        new Object[] {300},
                        null,
                        new BinlogPosition("mysql-bin.000001", 800L)));

        final Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();
        databaseHistory.put(tableId, getTestHistoryRecord());

        final MySQLSplit split =
                new MySQLSplit(
                        MySQLSplitKind.BINLOG,
                        tableId,
                        "binlog-split-0",
                        new RowType(
                                Arrays.asList(new RowType.RowField("card_no", new VarCharType()))),
                        null,
                        null,
                        null,
                        null,
                        true,
                        new BinlogPosition("mysql-bin.000001", 4L),
                        finishedSplitsInfo,
                        databaseHistory);
        assertSplitsEqual(split, serializeAndDeserializeSplit(split));
    }

    @Test
    public void testRepeatedSerializationCache() throws Exception {
        final MySQLSplit split =
                new MySQLSplit(
                        MySQLSplitKind.SNAPSHOT,
                        TableId.parse("test_db.test_table"),
                        "test_db.test_table-0",
                        new RowType(Arrays.asList(new RowType.RowField("id", new BigIntType()))),
                        null,
                        new Object[] {99L},
                        new BinlogPosition("mysql-bin.000001", 3L),
                        new BinlogPosition("mysql-bin.000002", 78L),
                        true,
                        INITIAL_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>());
        final byte[] ser1 = MySQLSplitSerializer.INSTANCE.serialize(split);
        final byte[] ser2 = MySQLSplitSerializer.INSTANCE.serialize(split);
        assertSame(ser1, ser2);
    }

    private MySQLSplit serializeAndDeserializeSplit(MySQLSplit split) throws Exception {
        final MySQLSplitSerializer sqlSplitSerializer = new MySQLSplitSerializer();
        byte[] serialized = sqlSplitSerializer.serialize(split);
        return sqlSplitSerializer.deserializeV1(serialized);
    }

    public static SchemaRecord getTestHistoryRecord() throws Exception {
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
        final Document tableChange = DocumentReader.defaultReader().read(tableChangeJsonStr);
        return new SchemaRecord(tableChange);
    }

    public static void assertSplitsEqual(MySQLSplit expected, MySQLSplit actual) {
        assertEquals(expected.getSplitKind(), actual.getSplitKind());
        assertEquals(expected.getTableId(), actual.getTableId());
        assertEquals(expected.getSplitId(), actual.getSplitId());
        assertEquals(expected.getSplitBoundaryType(), actual.getSplitBoundaryType());
        assertArrayEquals(expected.getSplitBoundaryStart(), actual.getSplitBoundaryStart());
        assertArrayEquals(expected.getSplitBoundaryEnd(), actual.getSplitBoundaryEnd());
        assertEquals(expected.getOffset(), actual.getOffset());
        assertEquals(
                expected.getFinishedSplitsInfo().toString(),
                actual.getFinishedSplitsInfo().toString());
        assertEquals(
                expected.getDatabaseHistory().toString(), actual.getDatabaseHistory().toString());
    }
}
