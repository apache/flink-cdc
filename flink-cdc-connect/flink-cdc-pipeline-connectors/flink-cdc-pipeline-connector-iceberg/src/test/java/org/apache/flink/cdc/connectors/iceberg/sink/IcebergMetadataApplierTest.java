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
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HiveContainer;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for {@link IcebergMetadataApplier}. */
public class IcebergMetadataApplierTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testApplySchemaChangeHiveCatalog() throws InterruptedException {
        File warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString());
        if (!warehouse.exists()) {
            warehouse.mkdirs();
        }
        warehouse.setExecutable(true, false);
        HiveContainer hiveContainer = new HiveContainer(warehouse.getAbsolutePath());
        Startables.deepStart(Stream.of(hiveContainer)).join();
        Thread.sleep(20000);
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hive");
        catalogOptions.put("warehouse", warehouse.toString());
        catalogOptions.put("cache-enabled", "false");
        catalogOptions.put("uri", hiveContainer.getMetastoreUri());
        runSchemaChangeTest(catalogOptions);
    }

    @Test
    public void testApplySchemaChangeHadoopCatalog() throws InterruptedException {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        runSchemaChangeTest(catalogOptions);
    }

    public void runSchemaChangeTest(Map<String, String> catalogOptions) {
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
                                .physicalColumn(
                                        "tinyIntCol",
                                        DataTypes.TINYINT(),
                                        "column for tinyIntCol",
                                        "1")
                                .physicalColumn(
                                        "description",
                                        DataTypes.STRING(),
                                        "column for descriptions",
                                        "not important")
                                .physicalColumn(
                                        "bool_column",
                                        DataTypes.BOOLEAN(),
                                        "column for bool",
                                        "false")
                                .physicalColumn(
                                        "float_column",
                                        DataTypes.FLOAT(),
                                        "column for float",
                                        "1.0")
                                .physicalColumn(
                                        "double_column",
                                        DataTypes.DOUBLE(),
                                        "column for double",
                                        "1.0")
                                .physicalColumn(
                                        "decimal_column",
                                        DataTypes.DECIMAL(10, 2),
                                        "column for decimal",
                                        "1.0")
                                .primaryKey("id")
                                .partitionKey("id", "name")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        // Verify schema structure (column names, types, nullability).
        assertThat(table.schema().columns()).hasSize(8);
        assertThat(table.schema().findField("id").type()).isInstanceOf(Types.LongType.class);
        assertThat(table.schema().findField("id").isOptional()).isFalse();
        assertThat(table.schema().findField("name").type()).isInstanceOf(Types.StringType.class);
        assertThat(table.schema().findField("name").isOptional()).isFalse();
        assertThat(table.schema().findField("tinyIntCol").type())
                .isInstanceOf(Types.IntegerType.class);
        assertThat(table.schema().findField("description").type())
                .isInstanceOf(Types.StringType.class);
        assertThat(table.schema().findField("bool_column").type())
                .isInstanceOf(Types.BooleanType.class);
        assertThat(table.schema().findField("float_column").type())
                .isInstanceOf(Types.FloatType.class);
        assertThat(table.schema().findField("double_column").type())
                .isInstanceOf(Types.DoubleType.class);
        assertThat(table.schema().findField("decimal_column").type())
                .isEqualTo(Types.DecimalType.of(10, 2));
        assertThat(table.schema().identifierFieldIds())
                .isEqualTo(new HashSet<>(Collections.singletonList(1)));

        // Verify default values after create table.
        // "id" has "AUTO_DECREMENT()" which is unparseable for BIGINT, so no default
        assertThat(table.schema().findField("id").initialDefault()).isNull();
        assertThat(table.schema().findField("id").writeDefault()).isNull();
        // "name" has "John Smith" which is a valid string default
        assertThat(table.schema().findField("name").writeDefault()).isEqualTo("John Smith");
        // "tinyIntCol" has "1" which maps to Integer
        assertThat(table.schema().findField("tinyIntCol").writeDefault()).isEqualTo(1);
        // "description" has "not important" which is a valid string default
        assertThat(table.schema().findField("description").writeDefault())
                .isEqualTo("not important");
        // "bool_column" has "false"
        assertThat(table.schema().findField("bool_column").writeDefault()).isEqualTo(false);
        // "float_column" has "1.0"
        assertThat(table.schema().findField("float_column").writeDefault()).isEqualTo(1.0f);
        // "double_column" has "1.0"
        assertThat(table.schema().findField("double_column").writeDefault()).isEqualTo(1.0d);
        // "decimal_column" has "1.0"
        assertThat(table.schema().findField("decimal_column").writeDefault())
                .isEqualTo(new java.math.BigDecimal("1.00"));

        // Add column with default value.
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "newIntColumn",
                                                DataTypes.INT(),
                                                "comment for newIntColumn",
                                                "42"))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        assertThat(table.schema().columns()).hasSize(9);
        assertThat(table.schema().findField("newIntColumn").type())
                .isInstanceOf(Types.IntegerType.class);
        assertThat(table.schema().findField("newIntColumn").doc())
                .isEqualTo("comment for newIntColumn");

        // Verify default value for added column.
        assertThat(table.schema().findField("newIntColumn").writeDefault()).isEqualTo(42);

        // Drop Column.
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Collections.singletonList("description"));
        icebergMetadataApplier.applySchemaChange(dropColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        assertThat(table.schema().columns()).hasSize(8);
        assertThat(table.schema().findField("description")).isNull();

        // Rename Column.
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(tableId, ImmutableMap.of("newIntColumn", "renamedIntColumn"));
        icebergMetadataApplier.applySchemaChange(renameColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        assertThat(table.schema().columns()).hasSize(8);
        assertThat(table.schema().findField("newIntColumn")).isNull();
        assertThat(table.schema().findField("renamedIntColumn").type())
                .isInstanceOf(Types.IntegerType.class);

        // Alter Column Type.
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        tableId, ImmutableMap.of("renamedIntColumn", DataTypes.BIGINT()));
        icebergMetadataApplier.applySchemaChange(alterColumnTypeEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        assertThat(table.schema().columns()).hasSize(8);
        assertThat(table.schema().findField("renamedIntColumn").type())
                .isInstanceOf(Types.LongType.class);
    }
}
