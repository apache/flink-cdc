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

package org.apache.flink.cdc.connectors.values;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests to cover the ability of data consumption and schema evolution from ValuesDatabase. */
class ValuesDatabaseTest {

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
    @BeforeEach
    public void before() throws SchemaEvolveException {
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

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        ValuesDatabase.applyDataChangeEvent(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        ValuesDatabase.applyDataChangeEvent(insertEvent2);
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        ValuesDatabase.applyDataChangeEvent(insertEvent3);
    }

    @AfterEach
    public void after() {
        ValuesDatabase.clear();
    }

    @Test
    void testValuesMetadataAccessor() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("col2", new CharType())
                        .primaryKey("col1")
                        .build();
        Assertions.assertThat(metadataAccessor.listNamespaces()).hasSize(2);
        Assertions.assertThat(metadataAccessor.listSchemas(null)).hasSize(2);
        Assertions.assertThat(metadataAccessor.listTables(null, "default")).hasSize(1);
        Assertions.assertThat(metadataAccessor.getTableSchema(table1)).isEqualTo(schema);
    }

    @Test
    void testApplySchemaChangeEvent() throws SchemaEvolveException {
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
        Assertions.assertThat(metadataAccessor.getTableSchema(table1)).isEqualTo(schema);

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
        Assertions.assertThat(metadataAccessor.getTableSchema(table1)).isEqualTo(schema);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("newCol2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new CharType())
                        .physicalColumn("newCol3", new CharType())
                        .primaryKey("col1")
                        .build();
        Assertions.assertThat(metadataAccessor.getTableSchema(table1)).isEqualTo(schema);

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
        Assertions.assertThat(metadataAccessor.getTableSchema(table1)).isEqualTo(schema);
    }

    @Test
    void testApplyDataChangeEvent() {
        List<String> results = new ArrayList<>();
        results.add("default.default.table1:col1=1;col2=1");
        results.add("default.default.table1:col1=2;col2=2");
        results.add("default.default.table1:col1=3;col2=3");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        ValuesDatabase.applyDataChangeEvent(deleteEvent);
        results.clear();
        results.add("default.default.table1:col1=2;col2=2");
        results.add("default.default.table1:col1=3;col2=3");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);

        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("x")
                                }));
        ValuesDatabase.applyDataChangeEvent(updateEvent);
        results.clear();
        results.add("default.default.table1:col1=2;col2=x");
        results.add("default.default.table1:col1=3;col2=3");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);
    }

    @Test
    void testSchemaChangeWithExistedData() throws SchemaEvolveException {
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
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col3", "newCol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(table1, nameMapping);
        metadataApplier.applySchemaChange(renameColumnEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol2=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol2=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol2=3;newCol3=");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("newCol2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol3=");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol3", new VarCharType());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(table1, typeMapping);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        results.clear();
        results.add("default.default.table1:col1=1;newCol3=");
        results.add("default.default.table1:col1=2;newCol3=");
        results.add("default.default.table1:col1=3;newCol3=");
        Assertions.assertThat(ValuesDatabase.getResults(table1)).isEqualTo(results);
    }
}
