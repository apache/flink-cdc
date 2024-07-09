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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
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

/** Tests for {@link PaimonMetadataApplier}. */
public class PaimonMetadataApplierTest {

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
        this.catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.catalog.dropDatabase(TEST_DATABASE, true, true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testApplySchemaChange(String metastore)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException {
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
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "col3", org.apache.flink.cdc.common.types.DataTypes.STRING())));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(2, "col3", DataTypes.STRING())));
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());

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
                                new DataField(2, "newcol3", DataTypes.STRING())));
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());

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
                                new DataField(2, "newcol3", DataTypes.STRING())));
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        TableId.parse("test.table1"), Collections.singletonList("newcol2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        // id of DataField should keep the same as before dropping column
        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(2, "newcol3", DataTypes.STRING())));
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testCreateTableWithOptions(String metastore)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException {
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
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(3, "col4", DataTypes.STRING())));
        Assertions.assertEquals(tableSchema, table.rowType());
        Assertions.assertEquals(Collections.singletonList("col1"), table.primaryKeys());
        Assertions.assertEquals(Arrays.asList("col3", "col4"), table.partitionKeys());
        Assertions.assertEquals("-1", table.options().get("bucket"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testCreateTableWithAllDataTypes(String metastore)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException {
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
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))));
        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testAddColumnWithPosition(String metastore)
            throws Catalog.DatabaseNotEmptyException, Catalog.DatabaseNotExistException,
                    Catalog.TableNotExistException {
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

        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());

        addedColumns.clear();
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
                                new DataField(3, "col4_first", DataTypes.STRING()),
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(5, "col6_before", DataTypes.STRING()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(6, "col7_after", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(4, "col5_last", DataTypes.STRING())));

        Assertions.assertEquals(
                tableSchema, catalog.getTable(Identifier.fromString("test.table1")).rowType());
    }
}
