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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonMetadataApplier}. */
class PaimonMetadataApplierTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private Catalog catalog;

    private Options catalogOptions;

    public static final String TEST_DATABASE = "test";

    private static final String HADOOP_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hadoop-conf-dir"))
                    .getPath();

    private static final String HIVE_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hive-conf-dir"))
                    .getPath();

    private void initialize(String metastore)
            throws Catalog.DatabaseNotEmptyException, Catalog.DatabaseNotExistException {
        catalogOptions = new Options();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        if ("hive".equals(metastore)) {
            catalogOptions.setString("hadoop-conf-dir", HADOOP_CONF_DIR);
            catalogOptions.setString("hive-conf-dir", HIVE_CONF_DIR);
        }
        catalogOptions.setString("metastore", metastore);
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        this.catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.catalog.dropDatabase(TEST_DATABASE, true, true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    void testApplySchemaChange(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        MetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table1"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT())));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "col3",
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                null,
                                "col3DefValue")));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(
                                        2, "col3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newcol2");
        nameMapping.put("col3", "newcol3");
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TableId.parse("test.table1"), nameMapping);
        metadataApplier.applySchemaChange(renameColumnEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "newcol2", DataTypes.INT()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newcol2", org.apache.flink.cdc.common.types.DataTypes.STRING());
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(TableId.parse("test.table1"), typeMapping);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "newcol2", DataTypes.STRING()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        TableId.parse("test.table1"), Collections.singletonList("newcol2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        // id of DataField should keep the same as before dropping column
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        // Add column with variant type.
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "variantCol",
                                org.apache.flink.cdc.common.types.DataTypes.VARIANT(),
                                null)));
        addColumnEvent = new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue"),
                                new DataField(3, "variantCol", DataTypes.VARIANT(), null, null)));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        // Create table with partition column.
        createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table_with_partition"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .physicalColumn(
                                        "dt",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .primaryKey("col1")
                                .partitionKey("dt")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(2, "dt", DataTypes.INT().notNull())));
        Table tableWithPartition =
                catalog.getTable(Identifier.fromString("test.table_with_partition"));
        Assertions.assertThat(tableWithPartition.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(tableWithPartition.primaryKeys())
                .isEqualTo(Arrays.asList("col1", "dt"));
        // Create table with upper case.
        catalogOptions.setString("allow-upper-case", "true");
        metadataApplier = new PaimonMetadataApplier(catalogOptions);
        createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table_with_upper_case"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "COL1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("COL1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "COL1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT())));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString("test.table_with_upper_case"))
                                .rowType())
                .isEqualTo(tableSchema);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testCreateTableWithoutPrimaryKey(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table1"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString("test.table1"));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(3, "col4", DataTypes.STRING())));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEmpty();
        Assertions.assertThat(table.partitionKeys()).isEmpty();
        Assertions.assertThat(table.options()).containsEntry("bucket", "-1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    void testCreateTableWithOptions(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "-1");
        Map<TableId, List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(TableId.parse("test.table1"), Arrays.asList("col3", "col4"));
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, partitionMaps);
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table1"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString("test.table1"));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING().notNull()),
                                new DataField(3, "col4", DataTypes.STRING().notNull())));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEqualTo(Arrays.asList("col1", "col3", "col4"));
        Assertions.assertThat(table.partitionKeys()).isEqualTo(Arrays.asList("col3", "col4"));
        Assertions.assertThat(table.options()).containsEntry("bucket", "-1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    void testCreateTableWithAllDataTypes(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        MetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table1"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "boolean",
                                        org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                                .physicalColumn(
                                        "binary",
                                        org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                                .physicalColumn(
                                        "varbinary",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(10))
                                .physicalColumn(
                                        "bytes",
                                        org.apache.flink.cdc.common.types.DataTypes.BYTES())
                                .physicalColumn(
                                        "tinyint",
                                        org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                                .physicalColumn(
                                        "smallint",
                                        org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                                .physicalColumn(
                                        "int", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .physicalColumn(
                                        "float",
                                        org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                                .physicalColumn(
                                        "double",
                                        org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                                .physicalColumn(
                                        "decimal",
                                        org.apache.flink.cdc.common.types.DataTypes.DECIMAL(6, 3))
                                .physicalColumn(
                                        "char", org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                                .physicalColumn(
                                        "varchar",
                                        org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                                .physicalColumn(
                                        "string",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "date", org.apache.flink.cdc.common.types.DataTypes.DATE())
                                .physicalColumn(
                                        "time", org.apache.flink.cdc.common.types.DataTypes.TIME())
                                .physicalColumn(
                                        "time_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIME(6))
                                .physicalColumn(
                                        "timestamp",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                                .physicalColumn(
                                        "timestamp_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(3))
                                .physicalColumn(
                                        "timestamp_ltz",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                                .physicalColumn(
                                        "timestamp_ltz_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(
                                                3))
                                .physicalColumn(
                                        "variant",
                                        org.apache.flink.cdc.common.types.DataTypes.VARIANT())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "boolean", DataTypes.BOOLEAN()),
                                new DataField(2, "binary", DataTypes.BINARY(3)),
                                new DataField(3, "varbinary", DataTypes.VARBINARY(10)),
                                new DataField(4, "bytes", DataTypes.BYTES()),
                                new DataField(5, "tinyint", DataTypes.TINYINT()),
                                new DataField(6, "smallint", DataTypes.SMALLINT()),
                                new DataField(7, "int", DataTypes.INT()),
                                new DataField(8, "float", DataTypes.FLOAT()),
                                new DataField(9, "double", DataTypes.DOUBLE()),
                                new DataField(10, "decimal", DataTypes.DECIMAL(6, 3)),
                                new DataField(11, "char", DataTypes.CHAR(5)),
                                new DataField(12, "varchar", DataTypes.VARCHAR(10)),
                                new DataField(13, "string", DataTypes.STRING()),
                                new DataField(14, "date", DataTypes.DATE()),
                                new DataField(15, "time", DataTypes.TIME(0)),
                                new DataField(16, "time_with_precision", DataTypes.TIME(6)),
                                new DataField(17, "timestamp", DataTypes.TIMESTAMP(6)),
                                new DataField(
                                        18, "timestamp_with_precision", DataTypes.TIMESTAMP(3)),
                                new DataField(
                                        19,
                                        "timestamp_ltz",
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                                new DataField(
                                        20,
                                        "timestamp_ltz_with_precision",
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                new DataField(21, "variant", DataTypes.VARIANT())));
        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    void testAddColumnWithPosition(String metastore)
            throws Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    Catalog.TableNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        MetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table1"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "col3",
                                org.apache.flink.cdc.common.types.DataTypes
                                        .STRING()))); // default last position.
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(2, "col3", DataTypes.STRING())));

        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);

        addedColumns.clear();

        addedColumns.add(
                AddColumnEvent.before(
                        Column.physicalColumn(
                                "col4_first_before",
                                org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col1"));
        addedColumns.add(
                AddColumnEvent.first(
                        Column.physicalColumn(
                                "col4_first",
                                org.apache.flink.cdc.common.types.DataTypes.STRING())));
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "col5_last",
                                org.apache.flink.cdc.common.types.DataTypes.STRING())));
        addedColumns.add(
                AddColumnEvent.before(
                        Column.physicalColumn(
                                "col6_before",
                                org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col2"));
        addedColumns.add(
                AddColumnEvent.after(
                        Column.physicalColumn(
                                "col7_after", org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col2"));

        addColumnEvent = new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(4, "col4_first", DataTypes.STRING()),
                                new DataField(3, "col4_first_before", DataTypes.STRING()),
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(6, "col6_before", DataTypes.STRING()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(7, "col7_after", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(5, "col5_last", DataTypes.STRING())));

        Assertions.assertThat(catalog.getTable(Identifier.fromString("test.table1")).rowType())
                .isEqualTo(tableSchema);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testCreateTableWithComment(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.table_with_comment"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull(),
                                        "comment of col1")
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col2")
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col3")
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col4")
                                .comment("comment of table_with_comment")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString("test.table_with_comment"));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0, "col1", DataTypes.STRING().notNull(), "comment of col1"),
                                new DataField(1, "col2", DataTypes.STRING(), "comment of col2"),
                                new DataField(2, "col3", DataTypes.STRING(), "comment of col3"),
                                new DataField(3, "col4", DataTypes.STRING(), "comment of col4")));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEmpty();
        Assertions.assertThat(table.partitionKeys()).isEmpty();
        Assertions.assertThat(table.options()).containsEntry("bucket", "-1");
        Assertions.assertThat(table.comment()).contains("comment of table_with_comment");
    }

    @Test
    public void testMysqlDefaultTimestampValueConversionInAddColumn()
            throws SchemaEvolveException,
                    Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException {
        initialize("filesystem");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.timestamp_test"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "name",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "created_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(),
                                null,
                                SchemaChangeProvider.INVALID_OR_MISSING_DATATIME)));
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "updated_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(),
                                null,
                                SchemaChangeProvider.INVALID_OR_MISSING_DATATIME)));

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.timestamp_test"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        Table table = catalog.getTable(Identifier.fromString("test.timestamp_test"));

        Assertions.assertThat(table).isNotNull();
    }

    /** Microsecond variant: '0000-00-00 00:00:00.000000'. */
    private static final String INVALID_DATETIME_WITH_MICROS = "0000-00-00 00:00:00.000000";

    @Test
    public void testMysqlDefaultTimestampValueWithMicrosInAddColumn()
            throws SchemaEvolveException,
                    Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException {
        initialize("filesystem");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.timestamp_micros_test"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "name",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "created_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(6),
                                null,
                                INVALID_DATETIME_WITH_MICROS)));
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "updated_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(6),
                                null,
                                INVALID_DATETIME_WITH_MICROS)));

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.timestamp_micros_test"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        Table table = catalog.getTable(Identifier.fromString("test.timestamp_micros_test"));

        Assertions.assertThat(table).isNotNull();
    }

    // ========== BLOB type support tests ==========
    // NOTE: Paimon BLOB type has the following constraints that prevent integration tests:
    // 1. BLOB type requires data-evolution.enabled = true
    // 2. BLOB type requires row-tracking.enabled = true
    // 3. row-tracking.enabled cannot be combined with primary keys
    // This means BLOB columns can only exist in append-only tables (no primary keys).
    // Since CDC pipeline typically works with primary key tables, actual BLOB type creation
    // tests are disabled. The type conversion logic (VARBINARY/BINARY to BLOB) is correctly
    // implemented in PaimonMetadataApplier.convertBinaryToBlobIfNeeded() and
    // SchemaChangeProvider.convertBinaryToBlobIfNeeded() methods.

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateTableWithoutBlobFieldConfig(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        MetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.no_blob_config_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "varbinary_content",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))
                                .physicalColumn(
                                        "binary_content",
                                        org.apache.flink.cdc.common.types.DataTypes.BINARY(10))
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.no_blob_config_table"));
        RowType rowType = table.rowType();

        assertThat(rowType.getField("varbinary_content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.VARBINARY);
        assertThat(rowType.getField("binary_content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BINARY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testAddNormalVarbinaryColumnWithoutBlobConfig(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.normal_varbinary_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "varbinary_col",
                                org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.normal_varbinary_table"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        Table table = catalog.getTable(Identifier.fromString("test.normal_varbinary_table"));
        RowType rowType = table.rowType();

        assertThat(rowType.getField("varbinary_col").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.VARBINARY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateTableWithEmptyBlobFieldConfig(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.empty_blob_config_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "content",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.empty_blob_config_table"));
        RowType rowType = table.rowType();

        assertThat(rowType.getField("content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.VARBINARY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateTableWithNonExistentBlobFieldConfig(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "nonexistent_blob_col");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.nonexistent_blob_config_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "actual_varbinary",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.nonexistent_blob_config_table"));
        RowType rowType = table.rowType();

        assertThat(rowType.getField("actual_varbinary").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.VARBINARY);
        assertThat(rowType.getField("id").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.INTEGER);
    }

    // ========== BLOB type integration tests with append-only tables (no primary keys) ==========
    // These tests verify actual BLOB type creation in Paimon tables.
    // BLOB type requires:
    // 1. row-tracking.enabled = true
    // 2. data-evolution.enabled = true
    // These options cannot be combined with primary keys.

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithBlobField(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure blob-field to convert VARBINARY to BLOB
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "content");
        // Required for BLOB type
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table (no primary keys)
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.blob_append_only_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "content",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))
                                // No primaryKey() - append-only table
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.blob_append_only_table"));
        RowType rowType = table.rowType();

        // Verify content field is converted to BLOB type
        assertThat(rowType.getField("content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        assertThat(rowType.getField("id").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.INTEGER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithMultipleBlobFields(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure multiple blob fields
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "blob_content,image_data");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table with multiple VARBINARY columns
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.multi_blob_append_only_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "blob_content",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))
                                .physicalColumn(
                                        "image_data",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(1000))
                                .physicalColumn(
                                        "normal_varbinary",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(50))
                                // No primaryKey() - append-only table
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.multi_blob_append_only_table"));
        RowType rowType = table.rowType();

        // Verify blob_content and image_data are converted to BLOB
        assertThat(rowType.getField("blob_content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        assertThat(rowType.getField("image_data").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        // normal_varbinary is not in blob-field config, should remain VARBINARY
        assertThat(rowType.getField("normal_varbinary").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.VARBINARY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testAddBlobColumnToAppendOnlyTable(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "new_blob_col");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table first
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.add_blob_column_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        // Add VARBINARY column that should be converted to BLOB
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "new_blob_col",
                                org.apache.flink.cdc.common.types.DataTypes.VARBINARY(100))));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.add_blob_column_table"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        Table table = catalog.getTable(Identifier.fromString("test.add_blob_column_table"));
        RowType rowType = table.rowType();

        // Verify the added column is BLOB type
        assertThat(rowType.getField("new_blob_col").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithBinaryToBlob(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure blob-field for BINARY type conversion
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "fixed_content");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table with BINARY column
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.binary_to_blob_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "fixed_content",
                                        org.apache.flink.cdc.common.types.DataTypes.BINARY(10))
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.binary_to_blob_table"));
        RowType rowType = table.rowType();

        // Verify BINARY is also converted to BLOB type
        assertThat(rowType.getField("fixed_content").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithVarcharToBlob(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure blob-field for VARCHAR type conversion (path string case)
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "blob_path");
        tableOptions.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table with VARCHAR column (representing blob path)
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.varchar_to_blob_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "blob_path",
                                        org.apache.flink.cdc.common.types.DataTypes.VARCHAR(500))
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.varchar_to_blob_table"));
        RowType rowType = table.rowType();

        // Verify VARCHAR is converted to BLOB type
        assertThat(rowType.getField("blob_path").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        assertThat(rowType.getField("id").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.INTEGER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithStringToBlob(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure blob-field for STRING type conversion (path string case)
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "blob_uri");
        tableOptions.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table with STRING column (representing blob URI)
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.string_to_blob_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "blob_uri",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.string_to_blob_table"));
        RowType rowType = table.rowType();

        // Verify STRING is converted to BLOB type
        assertThat(rowType.getField("blob_uri").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        assertThat(rowType.getField("id").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.INTEGER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    void testCreateAppendOnlyTableWithCharToBlob(String metastore)
            throws Catalog.TableNotExistException,
                    Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException,
                    SchemaEvolveException {
        initialize(metastore);
        Map<String, String> tableOptions = new HashMap<>();
        // Configure blob-field for CHAR type conversion (path string case)
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "blob_path");
        tableOptions.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        MetadataApplier metadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());

        // Create append-only table with CHAR column (representing blob path)
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.parse("test.char_to_blob_table"),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "blob_path",
                                        org.apache.flink.cdc.common.types.DataTypes.CHAR(500))
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        Table table = catalog.getTable(Identifier.fromString("test.char_to_blob_table"));
        RowType rowType = table.rowType();

        // Verify CHAR is converted to BLOB type
        assertThat(rowType.getField("blob_path").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.BLOB);
        assertThat(rowType.getField("id").type().getTypeRoot())
                .isEqualTo(org.apache.paimon.types.DataTypeRoot.INTEGER);
    }
}
