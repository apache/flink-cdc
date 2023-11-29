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

package com.ververica.cdc.common.utils;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A test for the {@link SchemaUtils}. */
public class SchemaUtilsTest {

    @Test
    public void testApplySchemaChangeEvent() {
        TableId tableId = TableId.parse("default.default.table1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();

        // add column in last position
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING())));
        AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add new column before existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col4", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.BEFORE,
                        Column.physicalColumn("col3", DataTypes.STRING())));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add new column after existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col5", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.AFTER,
                        Column.physicalColumn("col4", DataTypes.STRING())));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col5", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add column in first position
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col0", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.FIRST,
                        null));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col5", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // drop columns
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        tableId,
                        Arrays.asList(
                                Column.physicalColumn("col3", DataTypes.STRING()),
                                Column.physicalColumn("col5", DataTypes.STRING())));
        schema = SchemaUtils.applySchemaChangeEvent(schema, dropColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .build());

        // rename columns
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col4", "newCol4");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, renameColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("newCol2", DataTypes.STRING())
                        .physicalColumn("newCol4", DataTypes.STRING())
                        .build());

        // alter column types
        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol2", DataTypes.VARCHAR(10));
        typeMapping.put("newCol4", DataTypes.VARCHAR(10));
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, typeMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, alterColumnTypeEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("newCol2", DataTypes.VARCHAR(10))
                        .physicalColumn("newCol4", DataTypes.VARCHAR(10))
                        .build());
    }
}
