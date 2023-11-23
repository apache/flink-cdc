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

package com.ververica.cdc.connectors.values;

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
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.VarCharType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests to cover the ability of data consumption and schema evolution from ValuesDatabase. */
public class ValuesDatabaseTest {

    private MetadataApplier metadataApplier;

    private MetadataAccessor metadataAccessor;

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
        ValuesDatabase.clear();
        metadataApplier = new ValuesDatabase.ValuesMetadataApplier();
        metadataAccessor = new ValuesDatabase.ValuesMetadataAccessor();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        metadataApplier.applySchemaChange(createTableEvent);
        createTableEvent = new CreateTableEvent(TableId.parse("default.table2"), schema);
        metadataApplier.applySchemaChange(createTableEvent);
        createTableEvent = new CreateTableEvent(TableId.parse("table3"), schema);
        metadataApplier.applySchemaChange(createTableEvent);
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(new CharType(), new CharType()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1")));
        ValuesDatabase.applyDataChangeEvent(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(new CharType(), new CharType()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")));
        ValuesDatabase.applyDataChangeEvent(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        RowType.of(new CharType(), new CharType()),
                        GenericRecordData.of(
                                GenericStringData.fromString("3"),
                                GenericStringData.fromString("3")));
        ValuesDatabase.applyDataChangeEvent(insertEvent3);
    }

    @After
    public void after() {
        ValuesDatabase.clear();
    }

    @Test
    public void testValuesMetadataAccessor() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .primaryKey("col1")
                        .build();
        Assert.assertEquals(2, metadataAccessor.listNamespaces().size());
        Assert.assertEquals(2, metadataAccessor.listSchemas(null).size());
        Assert.assertEquals(1, metadataAccessor.listTables(null, "default").size());
        Assert.assertEquals(schema, metadataAccessor.getTableSchema(table1));
    }

    @Test
    public void testApplySchemaChangeEvent() {
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", new CharType()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        metadataApplier.applySchemaChange(addColumnEvent);
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .physicalColumn("col3", new CharType())
                        .primaryKey("col1")
                        .build();
        Assert.assertEquals(schema, metadataAccessor.getTableSchema(table1));

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        metadataApplier.applySchemaChange(renameColumnEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol2", new CharType())
                        .physicalColumn("newCol3", new CharType())
                        .primaryKey("col1")
                        .build();
        Assert.assertEquals(schema, metadataAccessor.getTableSchema(table1));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", new CharType())));
        metadataApplier.applySchemaChange(dropColumnEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol3", new CharType())
                        .primaryKey("col1")
                        .build();
        Assert.assertEquals(schema, metadataAccessor.getTableSchema(table1));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol3", new VarCharType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(table1, typeMapping);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol3", new VarCharType())
                        .primaryKey("col1")
                        .build();
        Assert.assertEquals(schema, metadataAccessor.getTableSchema(table1));
    }

    @Test
    public void testApplyDataChangeEvent() {
        List<String> results = new ArrayList<>();
        results.add("default.default.table1:col1=1;col2=1");
        results.add("default.default.table1:col1=2;col2=2");
        results.add("default.default.table1:col1=3;col2=3");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));

        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        table1,
                        RowType.of(new CharType(), new CharType()),
                        GenericRecordData.of(
                                GenericStringData.fromString("1"),
                                GenericStringData.fromString("1")));
        ValuesDatabase.applyDataChangeEvent(deleteEvent);
        results.clear();
        results.add("default.default.table1:col1=2;col2=2");
        results.add("default.default.table1:col1=3;col2=3");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));

        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        table1,
                        RowType.of(new CharType(), new CharType()),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("2")),
                        GenericRecordData.of(
                                GenericStringData.fromString("2"),
                                GenericStringData.fromString("x")));
        ValuesDatabase.applyDataChangeEvent(updateEvent);
        results.clear();
        results.add("default.default.table1:col1=2;col2=x");
        results.add("default.default.table1:col1=3;col2=3");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));
    }

    @Test
    public void testSchemaChangeWithExistedData() {
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", new CharType()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        metadataApplier.applySchemaChange(addColumnEvent);
        List<String> results = new ArrayList<>();
        results.add("default.default.table1:col1=1;col2=1;col3=");
        results.add("default.default.table1:col1=2;col2=2;col3=");
        results.add("default.default.table1:col1=3;col2=3;col3=");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        metadataApplier.applySchemaChange(renameColumnEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol2=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol2=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol2=3;newCol3=");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table1,
                        Collections.singletonList(
                                Column.physicalColumn("newCol2", new CharType())));
        metadataApplier.applySchemaChange(dropColumnEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol3=");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol3", new VarCharType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(table1, typeMapping);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol3=");
        Assert.assertEquals(results, ValuesDatabase.getResults(table1));
    }
}
