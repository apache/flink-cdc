/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A test for the {@link SchemaChangeEventSerializer}. */
public class SchemaChangeEventSerializerTest extends SerializerTestBase<SchemaChangeEvent> {
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
                                    Column.physicalColumn("testCol1", DataTypes.TIMESTAMP())))),
            new AlterColumnTypeEvent(TableId.tableId("namespace", "schema", "table"), alterTypeMap),
            new CreateTableEvent(TableId.tableId("schema", "table"), schema),
            new DropColumnEvent(
                    TableId.tableId("schema", "table"),
                    Arrays.asList(
                            Column.metadataColumn("m1", DataTypes.TIMESTAMP()),
                            Column.metadataColumn("m2", DataTypes.DOUBLE(), "mKey"),
                            Column.metadataColumn("m3", DataTypes.DOUBLE(), "mKey", "desc"))),
            new RenameColumnEvent(TableId.tableId("table"), renameMap)
        };
    }
}
