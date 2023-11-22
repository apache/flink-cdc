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

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.VarCharType;
import org.apache.doris.flink.cfg.DorisOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DorisDatabaseTest {

    private MetadataApplier metadataApplier;

    /** default TableId for schema evolution testing. */
    private final TableId table1 = TableId.parse("default.default.table1");

    /**
     * Initial schema structures and records of database:
     *
     * <p>default.default.table1
     *
     * <p>｜----｜----｜
     *
     * <p>｜col1｜col2｜
     *
     * <p>｜----｜----｜
     *
     * <p>｜1 ｜1 ｜
     *
     * <p>｜2 ｜2 ｜
     *
     * <p>｜3 ｜3 ｜
     *
     * <p>default.table2
     *
     * <p>｜----｜----｜
     *
     * <p>｜col1｜col2｜
     *
     * <p>｜----｜----｜
     *
     * <p>table3
     *
     * <p>｜----｜----｜
     *
     * <p>｜col1｜col2｜
     *
     * <p>｜----｜----｜.
     */
    @Before
    public void before() {
        DorisDatabase.clear();
         Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        DorisDatabase.applySchemaChangeEvent(createTableEvent);
        createTableEvent = new CreateTableEvent(TableId.parse("default.table2"), schema);
        DorisDatabase.applySchemaChangeEvent(createTableEvent);
        createTableEvent = new CreateTableEvent(TableId.parse("table3"), schema);
        DorisDatabase.applySchemaChangeEvent(createTableEvent);
    }

    @Test
    public void testApplySchemaChangeEvent() {
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", new CharType()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        DorisDatabase.applySchemaChangeEvent(addColumnEvent);
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .physicalColumn("col3", new CharType())
                        .build();
        Schema tableSchema = DorisDatabase.getTableSchema(table1);
        Assert.assertEquals(schema, tableSchema);

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        DorisDatabase.applySchemaChangeEvent(renameColumnEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol2", new CharType())
                        .physicalColumn("newCol3", new CharType())
                        .build();
        Assert.assertEquals(schema, DorisDatabase.getTableSchema(table1));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", new CharType())));
        DorisDatabase.applySchemaChangeEvent(dropColumnEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol3", new CharType())
                        .build();
        Assert.assertEquals(schema, DorisDatabase.getTableSchema(table1));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol3", new VarCharType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(table1, typeMapping);
        DorisDatabase.applySchemaChangeEvent(alterColumnTypeEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol3", new VarCharType())
                        .build();
        Assert.assertEquals(schema, DorisDatabase.getTableSchema(table1));
    }

    @After
    public void after() {
        DorisDatabase.clear();
    }

}
