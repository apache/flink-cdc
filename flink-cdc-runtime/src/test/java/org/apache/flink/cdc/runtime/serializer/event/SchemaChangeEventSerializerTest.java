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

package org.apache.flink.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link SchemaChangeEventSerializer}. */
class SchemaChangeEventSerializerTest extends SerializerTestBase<SchemaChangeEvent> {

    private static final byte[] PRE_REORDER_TRUNCATE_TABLE_BYTES =
            new byte[] {0, 0, 0, 6, 0, 0, 0, 1, 0, 5, 't', 'a', 'b', 'l', 'e'};

    @Test
    void testDeserializePreReorderTruncateTableBytes() throws IOException {
        TruncateTableEvent event = new TruncateTableEvent(TableId.tableId("table"));
        DataInputDeserializer tagInput =
                new DataInputDeserializer(PRE_REORDER_TRUNCATE_TABLE_BYTES);

        assertThat(tagInput.readInt()).isEqualTo(6);
        assertThat(
                        SchemaChangeEventSerializer.INSTANCE.deserialize(
                                new DataInputDeserializer(PRE_REORDER_TRUNCATE_TABLE_BYTES)))
                .isEqualTo(event);

        DataOutputSerializer reserialized = new DataOutputSerializer(64);
        SchemaChangeEventSerializer.INSTANCE.serialize(event, reserialized);
        assertThat(reserialized.getCopyOfBuffer())
                .containsExactly(PRE_REORDER_TRUNCATE_TABLE_BYTES);
    }

    @Override
    protected TypeSerializer<SchemaChangeEvent> createSerializer() {
        return SchemaChangeEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<SchemaChangeEvent> getTypeClass() {
        return SchemaChangeEvent.class;
    }

    @Override
    protected SchemaChangeEvent[] getTestData() {
        Map<String, DataType> alterTypeMap = new HashMap<>();
        alterTypeMap.put("col1", DataTypes.BYTES());
        alterTypeMap.put("col2", DataTypes.TIME());
        Schema schema =
                Schema.newBuilder()
                        .comment("test")
                        .physicalColumn("col1", DataTypes.BIGINT())
                        .physicalColumn("col2", DataTypes.TIME(), "comment")
                        .metadataColumn("m1", DataTypes.BOOLEAN())
                        .metadataColumn("m2", DataTypes.DATE(), "mKey")
                        .metadataColumn("m3", DataTypes.TIMESTAMP_LTZ(), "mKey", "comment")
                        .option("option", "fake")
                        .primaryKey(Collections.singletonList("col1"))
                        .build();
        Map<String, String> renameMap = new HashMap<>();
        renameMap.put("c1", "c2");
        return new SchemaChangeEvent[] {
            new AddColumnEvent(
                    TableId.tableId("table"),
                    Arrays.asList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP())),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("testCol2", DataTypes.DOUBLE(), "desc"),
                                    AddColumnEvent.ColumnPosition.AFTER,
                                    "testCol1"))),
            new AlterColumnTypeEvent(TableId.tableId("namespace", "schema", "table"), alterTypeMap),
            new CreateTableEvent(TableId.tableId("schema", "table"), schema),
            new DropColumnEvent(
                    TableId.tableId("schema", "table"), Arrays.asList("m1", "m2", "m3")),
            new DropTableEvent(TableId.tableId("namespace", "schema", "drop_table")),
            new RenameColumnEvent(TableId.tableId("table"), renameMap),
            new TruncateTableEvent(TableId.tableId("namespace", "schema", "truncate_table")),
            new ReorderColumnEvent(
                    TableId.tableId("namespace", "schema", "table"),
                    Arrays.asList(
                            ReorderColumnEvent.ColumnMove.first("testCol1"),
                            ReorderColumnEvent.ColumnMove.last("testCol2"),
                            ReorderColumnEvent.ColumnMove.before("testCol3", "anchor1"),
                            ReorderColumnEvent.ColumnMove.after("testCol4", "anchor2")))
        };
    }
}
