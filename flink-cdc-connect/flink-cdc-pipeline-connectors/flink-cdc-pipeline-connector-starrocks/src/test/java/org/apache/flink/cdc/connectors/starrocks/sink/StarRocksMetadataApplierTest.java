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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT;

/** Tests for {@link StarRocksMetadataApplier}. */
class StarRocksMetadataApplierTest {

    private MockStarRocksCatalog catalog;
    private StarRocksMetadataApplier metadataApplier;

    @BeforeEach
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
    void testCreateTable() throws Exception {
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
        Assertions.assertThat(actualTable).isNotNull();

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
        Assertions.assertThat(actualTable).isEqualTo(expectTable);
    }

    @Test
    void testAddColumn() throws Exception {
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
        Assertions.assertThat(actualTable).isNotNull();

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
        Assertions.assertThat(actualTable).isEqualTo(expectTable);
    }

    @Test
    void testDropColumn() throws Exception {
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
                new DropColumnEvent(tableId, Arrays.asList("col2", "col3"));
        metadataApplier.applySchemaChange(dropColumnEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();

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
        Assertions.assertThat(actualTable).isEqualTo(expectTable);
    }
}
