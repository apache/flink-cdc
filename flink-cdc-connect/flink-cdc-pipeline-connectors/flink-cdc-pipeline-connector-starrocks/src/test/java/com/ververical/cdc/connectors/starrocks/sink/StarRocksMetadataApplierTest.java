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

package com.ververical.cdc.connectors.starrocks.sink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.connectors.starrocks.sink.SchemaChangeConfig;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksMetadataApplier;
import com.ververica.cdc.connectors.starrocks.sink.TableCreateConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for {@link StarRocksMetadataApplier}. */
public class StarRocksMetadataApplierTest {

    private MockStarRocksCatalog catalog;
    private StarRocksMetadataApplier metadataApplier;

    @Before
    public void setup() {
        Configuration configuration =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(TABLE_SCHEMA_CHANGE_TIMEOUT.key(), "100s")
                                .put(TABLE_CREATE_NUM_BUCKETS.key(), "10")
                                .put("table.create.properties.replication_num", "5")
                                .build());
        SchemaChangeConfig schemaChangeConfig = SchemaChangeConfig.from(configuration);
        TableCreateConfig tableCreateConfig = TableCreateConfig.from(configuration);
        this.catalog = new MockStarRocksCatalog();
        this.metadataApplier =
                new StarRocksMetadataApplier(catalog, tableCreateConfig, schemaChangeConfig);
    }

    @Test
    public void testCreateTable() throws Exception {
        TableId tableId = TableId.parse("test.tbl1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType())
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new TimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        List<StarRocksColumn> columns = new ArrayList<>();
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col1")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col2")
                        .setOrdinalPosition(1)
                        .setDataType("boolean")
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col3")
                        .setOrdinalPosition(2)
                        .setDataType("datetime")
                        .setNullable(true)
                        .build());
        StarRocksTable expectTable =
                new StarRocksTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                        .setColumns(columns)
                        .setTableKeys(schema.primaryKeys())
                        .setDistributionKeys(schema.primaryKeys())
                        .setNumBuckets(10)
                        .setTableProperties(Collections.singletonMap("replication_num", "5"))
                        .build();
        assertEquals(expectTable, actualTable);
    }

    @Test
    public void testAddColumn() throws Exception {
        TableId tableId = TableId.parse("test.tbl2");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col2", new DecimalType(20, 5))),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col3", new SmallIntType()))));
        metadataApplier.applySchemaChange(addColumnEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        List<StarRocksColumn> columns = new ArrayList<>();
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col1")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col2")
                        .setOrdinalPosition(1)
                        .setDataType("decimal")
                        .setColumnSize(20)
                        .setDecimalDigits(5)
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col3")
                        .setOrdinalPosition(2)
                        .setDataType("smallint")
                        .setNullable(true)
                        .build());
        StarRocksTable expectTable =
                new StarRocksTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                        .setColumns(columns)
                        .setTableKeys(schema.primaryKeys())
                        .setDistributionKeys(schema.primaryKeys())
                        .setNumBuckets(10)
                        .setTableProperties(Collections.singletonMap("replication_num", "5"))
                        .build();
        assertEquals(expectTable, actualTable);
    }

    @Test
    public void testDropColumn() throws Exception {
        TableId tableId = TableId.parse("test.tbl3");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType())
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new TimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        tableId,
                        Arrays.asList(
                                Column.physicalColumn("col2", new BooleanType()),
                                Column.physicalColumn("col3", new TimestampType())));
        metadataApplier.applySchemaChange(dropColumnEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        assertNotNull(actualTable);

        List<StarRocksColumn> columns = new ArrayList<>();
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("col1")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(true)
                        .build());
        StarRocksTable expectTable =
                new StarRocksTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                        .setColumns(columns)
                        .setTableKeys(schema.primaryKeys())
                        .setDistributionKeys(schema.primaryKeys())
                        .setNumBuckets(10)
                        .setTableProperties(Collections.singletonMap("replication_num", "5"))
                        .build();
        assertEquals(expectTable, actualTable);
    }
}
