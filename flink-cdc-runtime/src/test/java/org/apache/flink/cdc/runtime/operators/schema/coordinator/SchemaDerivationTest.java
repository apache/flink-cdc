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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.testutils.assertions.EventAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link SchemaDerivation}. */
class SchemaDerivationTest {

    private static final TableId TABLE_1 = TableId.tableId("mydb", "myschema", "mytable1");
    private static final TableId TABLE_2 = TableId.tableId("mydb", "myschema", "mytable2");
    private static final TableId MERGED_TABLE = TableId.tableId("mydb", "myschema", "mytables");

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("name", DataTypes.STRING()))
                    .column(Column.physicalColumn("age", DataTypes.INT()))
                    .build();

    private static final Schema COMPATIBLE_SCHEMA =
            Schema.newBuilder()
                    .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("name", DataTypes.STRING()))
                    .column(Column.physicalColumn("age", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("gender", DataTypes.STRING()))
                    .build();

    private static final Schema INCOMPATIBLE_SCHEMA =
            Schema.newBuilder()
                    .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("name", DataTypes.STRING()))
                    .column(Column.physicalColumn("age", DataTypes.STRING()))
                    .column(Column.physicalColumn("gender", DataTypes.STRING()))
                    .build();

    private static final List<RouteRule> ROUTES =
            Collections.singletonList(
                    new RouteRule("mydb.myschema.mytable[0-9]", MERGED_TABLE.toString(), null));

    @Test
    void testOneToOneMapping() {
        SchemaDerivation schemaDerivation =
                new SchemaDerivation(new SchemaManager(), ROUTES, new HashMap<>());

        // Create table
        List<SchemaChangeEvent> derivedChangesAfterCreateTable =
                schemaDerivation.applySchemaChange(new CreateTableEvent(TABLE_1, SCHEMA));
        assertThat(derivedChangesAfterCreateTable).hasSize(1);
        assertThat(derivedChangesAfterCreateTable.get(0))
                .asCreateTableEvent()
                .hasTableId(MERGED_TABLE)
                .hasSchema(SCHEMA);

        // Add column
        AddColumnEvent.ColumnWithPosition newCol1 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col1", DataTypes.STRING(), null));
        AddColumnEvent.ColumnWithPosition newCol2 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col2", DataTypes.STRING(), null));
        AddColumnEvent.ColumnWithPosition newCol3 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col3", DataTypes.STRING(), null, "abc"));
        List<AddColumnEvent.ColumnWithPosition> newColumns =
                Arrays.asList(newCol1, newCol2, newCol3);
        List<SchemaChangeEvent> derivedChangesAfterAddColumn =
                schemaDerivation.applySchemaChange(new AddColumnEvent(TABLE_1, newColumns));
        assertThat(derivedChangesAfterAddColumn).hasSize(1);
        assertThat(derivedChangesAfterAddColumn.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(newCol1, newCol2, newCol3);

        // Alter column type
        ImmutableMap<String, DataType> typeMapping = ImmutableMap.of("age", DataTypes.BIGINT());
        List<SchemaChangeEvent> derivedChangesAfterAlterTableType =
                schemaDerivation.applySchemaChange(new AlterColumnTypeEvent(TABLE_1, typeMapping));
        assertThat(derivedChangesAfterAlterTableType).hasSize(1);
        assertThat(derivedChangesAfterAlterTableType.get(0))
                .asAlterColumnTypeEvent()
                .hasTableId(MERGED_TABLE)
                .containsTypeMapping(typeMapping);

        // Drop column
        List<String> droppedColumns = Arrays.asList("new_col1", "new_col2");
        List<SchemaChangeEvent> derivedChangesAfterDropColumn =
                schemaDerivation.applySchemaChange(new DropColumnEvent(TABLE_1, droppedColumns));
        assertThat(derivedChangesAfterDropColumn).hasSize(1);
        assertThat(derivedChangesAfterDropColumn.get(0))
                .asDropColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsDroppedColumns("new_col1", "new_col2");

        // Rename column
        Map<String, String> renamedColumns = ImmutableMap.of("name", "last_name");
        List<SchemaChangeEvent> derivedChangesAfterRenameColumn =
                schemaDerivation.applySchemaChange(new RenameColumnEvent(TABLE_1, renamedColumns));
        assertThat(derivedChangesAfterRenameColumn).hasSize(1);
        assertThat(derivedChangesAfterRenameColumn.get(0))
                .asRenameColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsNameMapping(renamedColumns);
    }

    @Test
    void testMergingTablesWithExactSameSchema() {
        SchemaManager schemaManager = new SchemaManager();
        SchemaDerivation schemaDerivation =
                new SchemaDerivation(schemaManager, ROUTES, new HashMap<>());

        // Create table 1
        List<SchemaChangeEvent> derivedChangesAfterCreateTable =
                schemaDerivation.applySchemaChange(new CreateTableEvent(TABLE_1, SCHEMA));
        assertThat(derivedChangesAfterCreateTable).hasSize(1);
        assertThat(derivedChangesAfterCreateTable.get(0))
                .asCreateTableEvent()
                .hasTableId(MERGED_TABLE)
                .hasSchema(SCHEMA);
        derivedChangesAfterCreateTable.forEach(schemaManager::applyEvolvedSchemaChange);

        // Create table 2
        assertThat(schemaDerivation.applySchemaChange(new CreateTableEvent(TABLE_2, SCHEMA)))
                .isEmpty();

        // Add column for table 1
        AddColumnEvent.ColumnWithPosition newCol1 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col1", DataTypes.STRING(), null));
        AddColumnEvent.ColumnWithPosition newCol2 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col2", DataTypes.STRING(), null));
        List<AddColumnEvent.ColumnWithPosition> newColumns = Arrays.asList(newCol1, newCol2);
        List<SchemaChangeEvent> derivedChangesAfterAddColumn =
                schemaDerivation.applySchemaChange(new AddColumnEvent(TABLE_1, newColumns));
        assertThat(derivedChangesAfterAddColumn).hasSize(1);
        assertThat(derivedChangesAfterAddColumn.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(newCol1, newCol2);
        derivedChangesAfterAddColumn.forEach(schemaManager::applyEvolvedSchemaChange);

        // Add column for table 2
        assertThat(schemaDerivation.applySchemaChange(new AddColumnEvent(TABLE_2, newColumns)))
                .isEmpty();

        // Alter column type for table 1
        ImmutableMap<String, DataType> typeMapping = ImmutableMap.of("age", DataTypes.BIGINT());
        List<SchemaChangeEvent> derivedChangesAfterAlterColumnType =
                schemaDerivation.applySchemaChange(new AlterColumnTypeEvent(TABLE_1, typeMapping));
        assertThat(derivedChangesAfterAlterColumnType).hasSize(1);
        assertThat(derivedChangesAfterAlterColumnType.get(0))
                .asAlterColumnTypeEvent()
                .hasTableId(MERGED_TABLE)
                .containsTypeMapping(typeMapping);
        derivedChangesAfterAlterColumnType.forEach(schemaManager::applyEvolvedSchemaChange);

        // Alter column type for table 2
        assertThat(
                        schemaDerivation.applySchemaChange(
                                new AlterColumnTypeEvent(TABLE_2, typeMapping)))
                .isEmpty();

        // Drop column for table 1
        List<String> droppedColumns = Arrays.asList("new_col1", "new_col2");
        assertThat(schemaDerivation.applySchemaChange(new DropColumnEvent(TABLE_1, droppedColumns)))
                .isEmpty();
        // Drop column for table 2
        assertThat(schemaDerivation.applySchemaChange(new DropColumnEvent(TABLE_2, droppedColumns)))
                .isEmpty();

        // Rename column for table 1
        Map<String, String> renamedColumns = ImmutableMap.of("name", "last_name");
        List<SchemaChangeEvent> derivedChangesAfterRenameColumn =
                schemaDerivation.applySchemaChange(new RenameColumnEvent(TABLE_1, renamedColumns));
        assertThat(derivedChangesAfterRenameColumn).hasSize(1);
        assertThat(derivedChangesAfterRenameColumn.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(
                        new AddColumnEvent.ColumnWithPosition(
                                new PhysicalColumn("last_name", DataTypes.STRING(), null)));
        derivedChangesAfterRenameColumn.forEach(schemaManager::applyEvolvedSchemaChange);

        // Rename column for table 2
        assertThat(
                        schemaDerivation.applySchemaChange(
                                new RenameColumnEvent(TABLE_2, renamedColumns)))
                .isEmpty();
    }

    @Test
    void testMergingTableWithDifferentSchemas() {
        SchemaManager schemaManager = new SchemaManager();
        SchemaDerivation schemaDerivation =
                new SchemaDerivation(schemaManager, ROUTES, new HashMap<>());
        // Create table 1
        List<SchemaChangeEvent> derivedChangesAfterCreateTable =
                schemaDerivation.applySchemaChange(new CreateTableEvent(TABLE_1, SCHEMA));
        assertThat(derivedChangesAfterCreateTable).hasSize(1);
        assertThat(derivedChangesAfterCreateTable.get(0))
                .asCreateTableEvent()
                .hasTableId(MERGED_TABLE)
                .hasSchema(SCHEMA);
        derivedChangesAfterCreateTable.forEach(schemaManager::applyEvolvedSchemaChange);

        // Create table 2
        List<SchemaChangeEvent> derivedChangesAfterCreateTable2 =
                schemaDerivation.applySchemaChange(
                        new CreateTableEvent(TABLE_2, COMPATIBLE_SCHEMA));
        assertThat(derivedChangesAfterCreateTable2).hasSize(2);
        assertThat(derivedChangesAfterCreateTable2)
                .containsExactlyInAnyOrder(
                        new AddColumnEvent(
                                MERGED_TABLE,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                new PhysicalColumn(
                                                        "gender", DataTypes.STRING(), null)))),
                        new AlterColumnTypeEvent(
                                MERGED_TABLE, ImmutableMap.of("age", DataTypes.BIGINT())));
        derivedChangesAfterCreateTable2.forEach(schemaManager::applyEvolvedSchemaChange);

        // Add column for table 1
        AddColumnEvent.ColumnWithPosition newCol1 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col1", DataTypes.VARCHAR(255), null));
        AddColumnEvent.ColumnWithPosition newCol2 =
                new AddColumnEvent.ColumnWithPosition(
                        new PhysicalColumn("new_col2", DataTypes.VARCHAR(255), null));
        List<AddColumnEvent.ColumnWithPosition> newColumns = Arrays.asList(newCol1, newCol2);
        List<SchemaChangeEvent> derivedChangesAfterAddColumn =
                schemaDerivation.applySchemaChange(new AddColumnEvent(TABLE_1, newColumns));
        assertThat(derivedChangesAfterAddColumn).hasSize(1);
        assertThat(derivedChangesAfterAddColumn.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(newCol1, newCol2);
        derivedChangesAfterAddColumn.forEach(schemaManager::applyEvolvedSchemaChange);

        // Add column for table 2
        List<SchemaChangeEvent> derivedChangesAfterAddColumnForTable2 =
                schemaDerivation.applySchemaChange(
                        new AddColumnEvent(
                                TABLE_2,
                                Arrays.asList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                new PhysicalColumn(
                                                        "new_col1", DataTypes.STRING(), null)),
                                        new AddColumnEvent.ColumnWithPosition(
                                                new PhysicalColumn(
                                                        "new_col2", DataTypes.STRING(), null)))));
        assertThat(derivedChangesAfterAddColumnForTable2).hasSize(1);
        assertThat(derivedChangesAfterAddColumnForTable2.get(0))
                .asAlterColumnTypeEvent()
                .containsTypeMapping(
                        ImmutableMap.of(
                                "new_col1", DataTypes.STRING(), "new_col2", DataTypes.STRING()));
        derivedChangesAfterAddColumnForTable2.forEach(schemaManager::applyEvolvedSchemaChange);

        // Alter column type for table 1
        ImmutableMap<String, DataType> typeMapping = ImmutableMap.of("age", DataTypes.BIGINT());
        List<SchemaChangeEvent> derivedChangesAfterAlterColumnType =
                schemaDerivation.applySchemaChange(new AlterColumnTypeEvent(TABLE_1, typeMapping));
        assertThat(derivedChangesAfterAlterColumnType).isEmpty();
        // Alter column type for table 2
        List<SchemaChangeEvent> derivedChangesAfterAlterColumnTypeForTable2 =
                schemaDerivation.applySchemaChange(
                        new AlterColumnTypeEvent(
                                TABLE_2, ImmutableMap.of("age", DataTypes.TINYINT())));
        assertThat(derivedChangesAfterAlterColumnTypeForTable2).isEmpty();

        // Drop column for table 1
        List<String> droppedColumns = Arrays.asList("new_col1", "new_col2");
        assertThat(schemaDerivation.applySchemaChange(new DropColumnEvent(TABLE_1, droppedColumns)))
                .isEmpty();
        // Drop column for table 2
        assertThat(schemaDerivation.applySchemaChange(new DropColumnEvent(TABLE_2, droppedColumns)))
                .isEmpty();

        // Rename column for table 1
        Map<String, String> renamedColumns = ImmutableMap.of("name", "last_name");
        List<SchemaChangeEvent> derivedChangesAfterRenameColumn =
                schemaDerivation.applySchemaChange(new RenameColumnEvent(TABLE_1, renamedColumns));
        assertThat(derivedChangesAfterRenameColumn).hasSize(1);
        assertThat(derivedChangesAfterRenameColumn.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(
                        new AddColumnEvent.ColumnWithPosition(
                                new PhysicalColumn("last_name", DataTypes.STRING(), null)));
        derivedChangesAfterRenameColumn.forEach(schemaManager::applyEvolvedSchemaChange);

        // Rename column for table 2
        List<SchemaChangeEvent> derivedChangesAfterRenameColumnForTable2 =
                schemaDerivation.applySchemaChange(
                        new RenameColumnEvent(TABLE_2, ImmutableMap.of("name", "first_name")));
        assertThat(derivedChangesAfterRenameColumnForTable2).hasSize(1);
        assertThat(derivedChangesAfterRenameColumnForTable2.get(0))
                .asAddColumnEvent()
                .hasTableId(MERGED_TABLE)
                .containsAddedColumns(
                        new AddColumnEvent.ColumnWithPosition(
                                new PhysicalColumn("first_name", DataTypes.STRING(), null)));
        derivedChangesAfterRenameColumnForTable2.forEach(schemaManager::applyEvolvedSchemaChange);

        assertThat(schemaManager.getLatestEvolvedSchema(MERGED_TABLE))
                .contains(
                        Schema.newBuilder()
                                .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                                .column(Column.physicalColumn("name", DataTypes.STRING()))
                                .column(Column.physicalColumn("age", DataTypes.BIGINT()))
                                .column(Column.physicalColumn("gender", DataTypes.STRING()))
                                .column(Column.physicalColumn("new_col1", DataTypes.STRING()))
                                .column(Column.physicalColumn("new_col2", DataTypes.STRING()))
                                .column(Column.physicalColumn("last_name", DataTypes.STRING()))
                                .column(Column.physicalColumn("first_name", DataTypes.STRING()))
                                .build());
    }

    @Test
    void testIncompatibleTypes() {
        SchemaManager schemaManager = new SchemaManager();
        SchemaDerivation schemaDerivation =
                new SchemaDerivation(schemaManager, ROUTES, new HashMap<>());
        // Create table 1
        List<SchemaChangeEvent> derivedChangesAfterCreateTable =
                schemaDerivation.applySchemaChange(new CreateTableEvent(TABLE_1, SCHEMA));
        assertThat(derivedChangesAfterCreateTable).hasSize(1);
        assertThat(derivedChangesAfterCreateTable.get(0))
                .asCreateTableEvent()
                .hasTableId(MERGED_TABLE)
                .hasSchema(SCHEMA);
        derivedChangesAfterCreateTable.forEach(schemaManager::applyEvolvedSchemaChange);

        // Create table 2
        assertThatThrownBy(
                        () ->
                                schemaDerivation.applySchemaChange(
                                        new CreateTableEvent(TABLE_2, INCOMPATIBLE_SCHEMA)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Incompatible types found for column `age`: \"INT\" and \"STRING\"");
    }

    @Test
    void testSerde() throws Exception {
        Map<TableId, Set<TableId>> derivationMapping = new HashMap<>();
        Set<TableId> originalTableIds = new HashSet<>();
        originalTableIds.add(TABLE_1);
        originalTableIds.add(TABLE_2);
        derivationMapping.put(MERGED_TABLE, originalTableIds);
        SchemaDerivation schemaDerivation =
                new SchemaDerivation(new SchemaManager(), ROUTES, derivationMapping);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            SchemaDerivation.serializeDerivationMapping(schemaDerivation, out);
            byte[] serialized = baos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                Map<TableId, Set<TableId>> deserialized =
                        SchemaDerivation.deserializerDerivationMapping(in);
                assertThat(deserialized).isEqualTo(derivationMapping);
            }
        }
    }
}
