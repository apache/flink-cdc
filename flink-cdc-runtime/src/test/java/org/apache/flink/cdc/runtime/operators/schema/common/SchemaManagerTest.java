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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
        schemaManager.applyEvolvedSchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        assertThat(schemaManager.getLatestEvolvedSchema(CUSTOMERS)).contains(CUSTOMERS_SCHEMA);

        assertThatCode(
                        () ->
                                schemaManager.applyEvolvedSchemaChange(
                                        new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)))
                .doesNotThrowAnyException();
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
                                "id"),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("append_before_phone", DataTypes.BIGINT()),
                                AddColumnEvent.ColumnPosition.BEFORE,
                                "phone"),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn(
                                        "col_with_default", DataTypes.BIGINT(), null, "10")));

        schemaManager.applyEvolvedSchemaChange(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
        schemaManager.applyEvolvedSchemaChange(new AddColumnEvent(CUSTOMERS, newColumns));
        assertThat(schemaManager.getLatestEvolvedSchema(CUSTOMERS))
                .contains(
                        Schema.newBuilder()
                                .physicalColumn("append_first", DataTypes.BIGINT())
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("append_after_id", DataTypes.BIGINT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("append_before_phone", DataTypes.BIGINT())
                                .physicalColumn("phone", DataTypes.BIGINT())
                                .physicalColumn("append_last", DataTypes.BIGINT())
                                .physicalColumn("col_with_default", DataTypes.BIGINT(), null, "10")
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testHandlingAlterColumnTypeEvent() {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new AlterColumnTypeEvent(
                            CUSTOMERS, ImmutableMap.of("phone", DataTypes.STRING())));
            assertThat(schemaManager.getLatestOriginalSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build());
        }

        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(
                    new AlterColumnTypeEvent(
                            CUSTOMERS, ImmutableMap.of("phone", DataTypes.STRING())));
            assertThat(schemaManager.getLatestEvolvedSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build());
        }
    }

    @Test
    void testHandlingDropColumnEvent() {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new DropColumnEvent(CUSTOMERS, Arrays.asList("name", "phone")));
            assertThat(schemaManager.getLatestOriginalSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .primaryKey("id")
                                    .build());
        }
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(
                    new DropColumnEvent(CUSTOMERS, Arrays.asList("name", "phone")));
            assertThat(schemaManager.getLatestEvolvedSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .primaryKey("id")
                                    .build());
        }
    }

    @Test
    void testHandlingRenameColumnEvent() {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            assertThat(schemaManager.getLatestOriginalSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("new_name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.BIGINT())
                                    .primaryKey("id")
                                    .build());
        }
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            assertThat(schemaManager.getLatestEvolvedSchema(CUSTOMERS))
                    .contains(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("new_name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.BIGINT())
                                    .primaryKey("id")
                                    .build());
        }
    }

    @Test
    void testGettingHistoricalSchema() {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
            assertThat(schemaManager.getOriginalSchema(CUSTOMERS, 1))
                    .isEqualTo(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("new_name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.BIGINT())
                                    .primaryKey("id")
                                    .build());
        }
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
            assertThat(schemaManager.getEvolvedSchema(CUSTOMERS, 1))
                    .isEqualTo(
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .physicalColumn("new_name", DataTypes.STRING())
                                    .physicalColumn("phone", DataTypes.BIGINT())
                                    .primaryKey("id")
                                    .build());
        }
    }

    @Test
    void testVersionCleanup() {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
            schemaManager.applyOriginalSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("new_phone", "new_phone_2")));
            assertThatThrownBy(() -> schemaManager.getOriginalSchema(CUSTOMERS, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Schema version %s does not exist for table \"%s\"", 0, CUSTOMERS);
        }
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("name", "new_name")));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("phone", "new_phone")));
            schemaManager.applyEvolvedSchemaChange(
                    new RenameColumnEvent(CUSTOMERS, ImmutableMap.of("new_phone", "new_phone_2")));
            assertThatThrownBy(() -> schemaManager.getEvolvedSchema(CUSTOMERS, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Schema version %s does not exist for table \"%s\"", 0, CUSTOMERS);
        }
    }

    @Test
    void testSerde() throws Exception {
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyOriginalSchemaChange(
                    new CreateTableEvent(PRODUCTS, PRODUCTS_SCHEMA));
            byte[] serialized = SchemaManager.SERIALIZER.serialize(schemaManager);
            SchemaManager deserialized =
                    SchemaManager.SERIALIZER.deserialize(
                            SchemaManager.Serializer.CURRENT_VERSION, serialized);
            assertThat(deserialized).isEqualTo(schemaManager);
        }
        {
            SchemaManager schemaManager = new SchemaManager();
            schemaManager.applyEvolvedSchemaChange(
                    new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA));
            schemaManager.applyEvolvedSchemaChange(new CreateTableEvent(PRODUCTS, PRODUCTS_SCHEMA));
            byte[] serialized = SchemaManager.SERIALIZER.serialize(schemaManager);
            SchemaManager deserialized =
                    SchemaManager.SERIALIZER.deserialize(
                            SchemaManager.Serializer.CURRENT_VERSION, serialized);
            assertThat(deserialized).isEqualTo(schemaManager);
        }
    }
}
