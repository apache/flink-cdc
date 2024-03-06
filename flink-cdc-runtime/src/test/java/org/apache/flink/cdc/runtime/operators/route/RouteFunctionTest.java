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

package org.apache.flink.cdc.runtime.operators.route;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.testutils.assertions.EventAssertions.assertThat;

class RouteFunctionTest {
    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final TableId NEW_CUSTOMERS =
            TableId.tableId("my_new_company", "my_new_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

    @Test
    void testDataChangeEventRouting() throws Exception {
        RouteFunction router =
                RouteFunction.newBuilder()
                        .addRoute("my_company.\\.+.customers", NEW_CUSTOMERS)
                        .build();
        router.open(new Configuration());
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));

        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Alice"), 12345678L}));
        assertThat(router.map(insertEvent))
                .asDataChangeEvent()
                .hasTableId(NEW_CUSTOMERS)
                .hasOperationType(OperationType.INSERT)
                .withAfterRecordData()
                .hasArity(3)
                .withSchema(CUSTOMERS_SCHEMA)
                .hasFields(1, new BinaryStringData("Alice"), 12345678L);

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Alice"), 12345678L}),
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Alice"), 87654321L}));
        DataChangeEvent mappedUpdateEvent = (DataChangeEvent) router.map(updateEvent);
        assertThat(mappedUpdateEvent)
                .hasTableId(NEW_CUSTOMERS)
                .hasOperationType(OperationType.UPDATE);
        assertThat(mappedUpdateEvent.before())
                .withSchema(CUSTOMERS_SCHEMA)
                .hasFields(1, new BinaryStringData("Alice"), 12345678L);
        assertThat(mappedUpdateEvent.after())
                .withSchema(CUSTOMERS_SCHEMA)
                .hasFields(1, new BinaryStringData("Alice"), 87654321L);

        // Replace
        DataChangeEvent replaceEvent =
                DataChangeEvent.replaceEvent(
                        CUSTOMERS,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Bob"), 87654321L}));
        assertThat(router.map(replaceEvent))
                .asDataChangeEvent()
                .hasTableId(NEW_CUSTOMERS)
                .hasOperationType(OperationType.REPLACE)
                .withAfterRecordData()
                .hasArity(3)
                .withSchema(CUSTOMERS_SCHEMA)
                .hasFields(1, new BinaryStringData("Bob"), 87654321L);

        // Delete
        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        CUSTOMERS,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Bob"), 87654321L}));
        assertThat(router.map(deleteEvent))
                .asDataChangeEvent()
                .hasTableId(NEW_CUSTOMERS)
                .hasOperationType(OperationType.DELETE)
                .withBeforeRecordData()
                .hasArity(3)
                .withSchema(CUSTOMERS_SCHEMA)
                .hasFields(1, new BinaryStringData("Bob"), 87654321L);
    }

    @Test
    void testSchemaChangeEventRouting() throws Exception {
        RouteFunction router =
                RouteFunction.newBuilder()
                        .addRoute("\\.+_company.\\.+_branch.customers", NEW_CUSTOMERS)
                        .build();
        router.open(new Configuration());

        // CreateTableEvent
        CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
        assertThat(router.map(createTableEvent))
                .asSchemaChangeEvent()
                .hasTableId(NEW_CUSTOMERS)
                .asCreateTableEvent()
                .hasSchema(CUSTOMERS_SCHEMA);

        // AddColumnEvent
        AddColumnEvent.ColumnWithPosition newColumn =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("address", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.LAST,
                        null);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(CUSTOMERS, Collections.singletonList(newColumn));
        assertThat(router.map(addColumnEvent))
                .asSchemaChangeEvent()
                .asAddColumnEvent()
                .hasTableId(NEW_CUSTOMERS)
                .containsAddedColumns(newColumn);

        // DropColumnEvent
        PhysicalColumn droppedColumn = Column.physicalColumn("address", DataTypes.STRING());
        List<String> droppedColumns = Collections.singletonList(droppedColumn.getName());
        DropColumnEvent dropColumnEvent = new DropColumnEvent(CUSTOMERS, droppedColumns);
        assertThat(router.map(dropColumnEvent))
                .asSchemaChangeEvent()
                .asDropColumnEvent()
                .containsDroppedColumns(droppedColumn.getName())
                .hasTableId(NEW_CUSTOMERS);

        // RenameColumnEvent
        Map<String, String> columnRenaming = ImmutableMap.of("phone", "mobile");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(CUSTOMERS, columnRenaming);
        assertThat(router.map(renameColumnEvent))
                .asSchemaChangeEvent()
                .asRenameColumnEvent()
                .containsNameMapping(columnRenaming)
                .hasTableId(NEW_CUSTOMERS);

        // AlterColumnTypeEvent
        Map<String, DataType> typeMapping = ImmutableMap.of("mobile", DataTypes.STRING());
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(CUSTOMERS, typeMapping);
        assertThat(router.map(alterColumnTypeEvent))
                .asSchemaChangeEvent()
                .asAlterColumnTypeEvent()
                .containsTypeMapping(typeMapping)
                .hasTableId(NEW_CUSTOMERS);
    }
}
