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
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
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
        org.apache.iceberg.Schema schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.builder()
                                        .withId(1)
                                        .asRequired()
                                        .withName("id")
                                        .ofType(Types.LongType.get())
                                        .withDoc("column for id")
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(2)
                                        .asRequired()
                                        .withName("name")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for name")
                                        .withWriteDefault(Literal.of("John Smith"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(3)
                                        .asOptional()
                                        .withName("tinyIntCol")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("column for tinyIntCol")
                                        .withWriteDefault(Literal.of(1))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(4)
                                        .asOptional()
                                        .withName("description")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for descriptions")
                                        .withWriteDefault(Literal.of("not important"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(5)
                                        .asOptional()
                                        .withName("bool_column")
                                        .ofType(Types.BooleanType.get())
                                        .withDoc("column for bool")
                                        .withWriteDefault(Literal.of(false))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(6)
                                        .asOptional()
                                        .withName("float_column")
                                        .ofType(Types.FloatType.get())
                                        .withDoc("column for float")
                                        .withWriteDefault(Literal.of(1.0f))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(7)
                                        .asOptional()
                                        .withName("double_column")
                                        .ofType(Types.DoubleType.get())
                                        .withDoc("column for double")
                                        .withWriteDefault(Literal.of(1.0d))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(8)
                                        .asOptional()
                                        .withName("decimal_column")
                                        .ofType(Types.DecimalType.of(10, 2))
                                        .withDoc("column for decimal")
                                        .withWriteDefault(Literal.of(new BigDecimal("1.00")))
                                        .build()),
                        new HashSet<>(Collections.singletonList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

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
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.builder()
                                        .withId(1)
                                        .asRequired()
                                        .withName("id")
                                        .ofType(Types.LongType.get())
                                        .withDoc("column for id")
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(2)
                                        .asRequired()
                                        .withName("name")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for name")
                                        .withWriteDefault(Literal.of("John Smith"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(3)
                                        .asOptional()
                                        .withName("tinyIntCol")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("column for tinyIntCol")
                                        .withWriteDefault(Literal.of(1))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(4)
                                        .asOptional()
                                        .withName("description")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for descriptions")
                                        .withWriteDefault(Literal.of("not important"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(5)
                                        .asOptional()
                                        .withName("bool_column")
                                        .ofType(Types.BooleanType.get())
                                        .withDoc("column for bool")
                                        .withWriteDefault(Literal.of(false))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(6)
                                        .asOptional()
                                        .withName("float_column")
                                        .ofType(Types.FloatType.get())
                                        .withDoc("column for float")
                                        .withWriteDefault(Literal.of(1.0f))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(7)
                                        .asOptional()
                                        .withName("double_column")
                                        .ofType(Types.DoubleType.get())
                                        .withDoc("column for double")
                                        .withWriteDefault(Literal.of(1.0d))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(8)
                                        .asOptional()
                                        .withName("decimal_column")
                                        .ofType(Types.DecimalType.of(10, 2))
                                        .withDoc("column for decimal")
                                        .withWriteDefault(Literal.of(new BigDecimal("1.00")))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(9)
                                        .asOptional()
                                        .withName("newIntColumn")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("comment for newIntColumn")
                                        .withWriteDefault(Literal.of(42))
                                        .build()),
                        new HashSet<>(Collections.singletonList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();

        // Drop Column.
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Collections.singletonList("description"));
        icebergMetadataApplier.applySchemaChange(dropColumnEvent);
        table = catalog.loadTable(TableIdentifier.parse(defaultTableId));
        schema =
                new org.apache.iceberg.Schema(
                        0,
                        Arrays.asList(
                                Types.NestedField.builder()
                                        .withId(1)
                                        .asRequired()
                                        .withName("id")
                                        .ofType(Types.LongType.get())
                                        .withDoc("column for id")
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(2)
                                        .asRequired()
                                        .withName("name")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for name")
                                        .withWriteDefault(Literal.of("John Smith"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(3)
                                        .asOptional()
                                        .withName("tinyIntCol")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("column for tinyIntCol")
                                        .withWriteDefault(Literal.of(1))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(5)
                                        .asOptional()
                                        .withName("bool_column")
                                        .ofType(Types.BooleanType.get())
                                        .withDoc("column for bool")
                                        .withWriteDefault(Literal.of(false))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(6)
                                        .asOptional()
                                        .withName("float_column")
                                        .ofType(Types.FloatType.get())
                                        .withDoc("column for float")
                                        .withWriteDefault(Literal.of(1.0f))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(7)
                                        .asOptional()
                                        .withName("double_column")
                                        .ofType(Types.DoubleType.get())
                                        .withDoc("column for double")
                                        .withWriteDefault(Literal.of(1.0d))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(8)
                                        .asOptional()
                                        .withName("decimal_column")
                                        .ofType(Types.DecimalType.of(10, 2))
                                        .withDoc("column for decimal")
                                        .withWriteDefault(Literal.of(new BigDecimal("1.00")))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(9)
                                        .asOptional()
                                        .withName("newIntColumn")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("comment for newIntColumn")
                                        .withWriteDefault(Literal.of(42))
                                        .build()),
                        new HashSet<>(Collections.singletonList(1)));
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
                                Types.NestedField.builder()
                                        .withId(1)
                                        .asRequired()
                                        .withName("id")
                                        .ofType(Types.LongType.get())
                                        .withDoc("column for id")
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(2)
                                        .asRequired()
                                        .withName("name")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for name")
                                        .withWriteDefault(Literal.of("John Smith"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(3)
                                        .asOptional()
                                        .withName("tinyIntCol")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("column for tinyIntCol")
                                        .withWriteDefault(Literal.of(1))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(5)
                                        .asOptional()
                                        .withName("bool_column")
                                        .ofType(Types.BooleanType.get())
                                        .withDoc("column for bool")
                                        .withWriteDefault(Literal.of(false))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(6)
                                        .asOptional()
                                        .withName("float_column")
                                        .ofType(Types.FloatType.get())
                                        .withDoc("column for float")
                                        .withWriteDefault(Literal.of(1.0f))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(7)
                                        .asOptional()
                                        .withName("double_column")
                                        .ofType(Types.DoubleType.get())
                                        .withDoc("column for double")
                                        .withWriteDefault(Literal.of(1.0d))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(8)
                                        .asOptional()
                                        .withName("decimal_column")
                                        .ofType(Types.DecimalType.of(10, 2))
                                        .withDoc("column for decimal")
                                        .withWriteDefault(Literal.of(new BigDecimal("1.00")))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(9)
                                        .asOptional()
                                        .withName("renamedIntColumn")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("comment for newIntColumn")
                                        .withWriteDefault(Literal.of(42))
                                        .build()),
                        new HashSet<>(Collections.singletonList(1)));
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
                                Types.NestedField.builder()
                                        .withId(1)
                                        .asRequired()
                                        .withName("id")
                                        .ofType(Types.LongType.get())
                                        .withDoc("column for id")
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(2)
                                        .asRequired()
                                        .withName("name")
                                        .ofType(Types.StringType.get())
                                        .withDoc("column for name")
                                        .withWriteDefault(Literal.of("John Smith"))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(3)
                                        .asOptional()
                                        .withName("tinyIntCol")
                                        .ofType(Types.IntegerType.get())
                                        .withDoc("column for tinyIntCol")
                                        .withWriteDefault(Literal.of(1))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(5)
                                        .asOptional()
                                        .withName("bool_column")
                                        .ofType(Types.BooleanType.get())
                                        .withDoc("column for bool")
                                        .withWriteDefault(Literal.of(false))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(6)
                                        .asOptional()
                                        .withName("float_column")
                                        .ofType(Types.FloatType.get())
                                        .withDoc("column for float")
                                        .withWriteDefault(Literal.of(1.0f))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(7)
                                        .asOptional()
                                        .withName("double_column")
                                        .ofType(Types.DoubleType.get())
                                        .withDoc("column for double")
                                        .withWriteDefault(Literal.of(1.0d))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(8)
                                        .asOptional()
                                        .withName("decimal_column")
                                        .ofType(Types.DecimalType.of(10, 2))
                                        .withDoc("column for decimal")
                                        .withWriteDefault(Literal.of(new BigDecimal("1.00")))
                                        .build(),
                                Types.NestedField.builder()
                                        .withId(9)
                                        .asOptional()
                                        .withName("renamedIntColumn")
                                        .ofType(Types.LongType.get())
                                        .withDoc("comment for newIntColumn")
                                        .withWriteDefault(Literal.of(42L))
                                        .build()),
                        new HashSet<>(Collections.singletonList(1)));
        assertThat(table.schema().sameSchema(schema)).isTrue();
    }
}
