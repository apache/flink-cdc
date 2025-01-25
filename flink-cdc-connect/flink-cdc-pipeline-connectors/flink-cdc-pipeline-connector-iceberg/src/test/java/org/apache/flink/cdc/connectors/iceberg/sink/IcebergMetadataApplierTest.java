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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for {@link IcebergMetadataApplier}. */
public class IcebergMetadataApplierTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testApplySchemaChange() {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");

        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        String defaultTableId = "test.iceberg_table";
        TableId tableId = TableId.parse(defaultTableId);

        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "name",
                                        DataTypes.VARCHAR(255).notNull(),
                                        "column for name",
                                        "John Smith")
                                .physicalColumn("age", DataTypes.TINYINT(), "column for age", "91")
                                .physicalColumn(
                                        "description",
                                        DataTypes.STRING(),
                                        "column for descriptions",
                                        "not important")
                                .primaryKey("id")
                                .partitionKey("id", "name")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        org.apache.iceberg.Schema schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.of(
                                        1, false, "id", Types.LongType.get(), "column for id"),
                                Types.NestedField.of(
                                        2,
                                        false,
                                        "name",
                                        Types.StringType.get(),
                                        "column for name"),
                                Types.NestedField.of(
                                        3, true, "age", Types.IntegerType.get(), "column for age"),
                                Types.NestedField.of(
                                        4,
                                        true,
                                        "description",
                                        Types.StringType.get(),
                                        "column for descriptions")),
                        new HashSet<>(Arrays.asList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

        // Add column.
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "newIntColumn",
                                                DataTypes.INT(),
                                                "comment for newIntColumn",
                                                "not important"))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.of(
                                        1, false, "id", Types.LongType.get(), "column for id"),
                                Types.NestedField.of(
                                        2,
                                        false,
                                        "name",
                                        Types.StringType.get(),
                                        "column for name"),
                                Types.NestedField.of(
                                        3, true, "age", Types.IntegerType.get(), "column for age"),
                                Types.NestedField.of(
                                        4,
                                        true,
                                        "description",
                                        Types.StringType.get(),
                                        "column for descriptions"),
                                Types.NestedField.of(
                                        5,
                                        true,
                                        "newIntColumn",
                                        Types.IntegerType.get(),
                                        "comment for newIntColumn")),
                        new HashSet<>(Arrays.asList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

        // Drop Column.
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Arrays.asList("description"));
        icebergMetadataApplier.applySchemaChange(dropColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.of(
                                        1, false, "id", Types.LongType.get(), "column for id"),
                                Types.NestedField.of(
                                        2,
                                        false,
                                        "name",
                                        Types.StringType.get(),
                                        "column for name"),
                                Types.NestedField.of(
                                        3, true, "age", Types.IntegerType.get(), "column for age"),
                                Types.NestedField.of(
                                        5,
                                        true,
                                        "newIntColumn",
                                        Types.IntegerType.get(),
                                        "comment for newIntColumn")),
                        new HashSet<>(Arrays.asList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

        // Rename Column.
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(tableId, ImmutableMap.of("newIntColumn", "renamedIntColumn"));
        icebergMetadataApplier.applySchemaChange(renameColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.of(
                                        1, false, "id", Types.LongType.get(), "column for id"),
                                Types.NestedField.of(
                                        2,
                                        false,
                                        "name",
                                        Types.StringType.get(),
                                        "column for name"),
                                Types.NestedField.of(
                                        3, true, "age", Types.IntegerType.get(), "column for age"),
                                Types.NestedField.of(
                                        5,
                                        true,
                                        "renamedIntColumn",
                                        Types.IntegerType.get(),
                                        "comment for newIntColumn")),
                        new HashSet<>(Arrays.asList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

        // Alter Column Type.
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        tableId, ImmutableMap.of("renamedIntColumn", DataTypes.BIGINT()));
        icebergMetadataApplier.applySchemaChange(alterColumnTypeEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.of(
                                        1, false, "id", Types.LongType.get(), "column for id"),
                                Types.NestedField.of(
                                        2,
                                        false,
                                        "name",
                                        Types.StringType.get(),
                                        "column for name"),
                                Types.NestedField.of(
                                        3, true, "age", Types.IntegerType.get(), "column for age"),
                                Types.NestedField.of(
                                        5,
                                        true,
                                        "renamedIntColumn",
                                        Types.LongType.get(),
                                        "comment for newIntColumn")),
                        new HashSet<>(Arrays.asList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();
    }
}
