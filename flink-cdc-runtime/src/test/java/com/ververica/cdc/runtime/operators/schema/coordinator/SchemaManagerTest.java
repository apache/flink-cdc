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

package com.ververica.cdc.runtime.operators.schema.coordinator;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link SchemaManager}. */
class SchemaManagerTest {
    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

    private static final TableId PRODUCTS = TableId.tableId("PRODUCTS");
    private static final Schema PRODUCTS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testHandlingCreateTableEvent() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        assertThat(schemaManager.getLatestSchema(CUSTOMERS)).isPresent().contains(CUSTOMERS_SCHEMA);

        // Cannot apply CreateTableEvent multiple times
        assertThatThrownBy(
                        () ->
                                schemaManager.applySchemaChange(
                                        new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                        CUSTOMERS);
    }

    @Test
    void testHandlingAddColumnEvent() {
        SchemaManager schemaManager = new SchemaManager();
        List<AddColumnEvent.ColumnWithPosition> newColumns =
                Arrays.asList(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("append_last", DataTypes.BIGINT())),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("append_first", DataTypes.BIGINT()),
                                AddColumnEvent.ColumnPosition.FIRST,
                                null),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("append_after_id", DataTypes.BIGINT()),
                                AddColumnEvent.ColumnPosition.AFTER,
                                Column.physicalColumn("id", DataTypes.INT())),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("append_before_phone", DataTypes.BIGINT()),
                                AddColumnEvent.ColumnPosition.BEFORE,
                                Column.physicalColumn("phone", DataTypes.STRING())));

        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(new AddColumnEvent(CUSTOMERS, newColumns));
        assertThat(schemaManager.getLatestSchema(CUSTOMERS))
                .contains(
                        Schema.newBuilder()
                                .physicalColumn("append_first", DataTypes.BIGINT())
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("append_after_id", DataTypes.BIGINT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("append_before_phone", DataTypes.BIGINT())
                                .physicalColumn("phone", DataTypes.BIGINT())
                                .physicalColumn("append_last", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testHandlingAlterColumnTypeEvent() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(
                new AlterColumnTypeEvent(CUSTOMERS, ImmutableMap.of("phone", DataTypes.STRING())));
        assertThat(schemaManager.getLatestSchema(CUSTOMERS))
                .contains(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("phone", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testHandlingDropColumnEvent() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(
                new DropColumnEvent(
                        CUSTOMERS,
                        Arrays.asList(
                                Column.physicalColumn("name", DataTypes.STRING()),
                                Column.physicalColumn("phone", DataTypes.BIGINT()))));
        assertThat(schemaManager.getLatestSchema(CUSTOMERS))
                .contains(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testHandlingRenameColumnEvent() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
        assertThat(schemaManager.getLatestSchema(CUSTOMERS))
                .contains(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("new_name", DataTypes.STRING())
                                .physicalColumn("phone", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testGettingHistoricalSchema() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
        assertThat(schemaManager.getSchema(CUSTOMERS, 1))
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("new_name", DataTypes.STRING())
                                .physicalColumn("phone", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testVersionCleanup() {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
        schemaManager.applySchemaChange(
                new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("new_phone", "new_phone_2")));
        assertThatThrownBy(() -> schemaManager.getSchema(CUSTOMERS, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Schema version %s does not exist for table \"%s\"", 0, CUSTOMERS);
    }

    @Test
    void testSerde() throws Exception {
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.applySchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applySchemaChange(new CreateTableEvent(PRODUCTS, PRODUCTS_SCHEMA));
        byte[] serialized = SchemaManager.SERIALIZER.serialize(schemaManager);
        SchemaManager deserialized = SchemaManager.SERIALIZER.deserialize(0, serialized);
        assertThat(deserialized).isEqualTo(schemaManager);
    }
}
