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
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A test for the {@link SchemaChangeEventSerializer}. */
class SchemaChangeEventSerializerTest extends SerializerTestBase<SchemaChangeEvent> {
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
            new RenameColumnEvent(TableId.tableId("table"), renameMap)
        };
    }
}
