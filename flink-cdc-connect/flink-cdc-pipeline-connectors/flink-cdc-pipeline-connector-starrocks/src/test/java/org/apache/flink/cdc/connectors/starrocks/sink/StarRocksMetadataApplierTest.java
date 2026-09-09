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
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
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
    void testReplayHistoricalSchemaAgainstWiderExistingTable() {
        TableId tableId = TableId.parse("test.replay_tbl");
        Schema currentSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new BigIntType(false))
                        .physicalColumn("new_col", new IntType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, currentSchema));

        Schema historicalSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType(false))
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, historicalSchema));
        metadataApplier.applySchemaChange(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("new_col", new IntType())))));
        metadataApplier.applySchemaChange(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("id", new BigIntType(false))));

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();
        Assertions.assertThat(actualTable.getColumn("id").getDataType())
                .isEqualToIgnoringCase("bigint");
        Assertions.assertThat(actualTable.getColumn("new_col")).isNotNull();
    }

    @Test
    void testRejectNarrowingAlterColumnType() {
        TableId tableId = TableId.parse("test.narrow_alter_tbl");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new BigIntType(false))
                        .physicalColumn("number", new DoubleType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));

        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new AlterColumnTypeEvent(
                                                tableId,
                                                Collections.singletonMap(
                                                        "id", new IntType(false)))))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        exception ->
                                Assertions.assertThat(exception.getExceptionMessage())
                                        .contains("Cannot safely widen"));

        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new AlterColumnTypeEvent(
                                                tableId,
                                                Collections.singletonMap(
                                                        "number", new FloatType()))))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        exception ->
                                Assertions.assertThat(exception.getExceptionMessage())
                                        .contains("Cannot safely widen"));

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();
        Assertions.assertThat(actualTable.getColumn("id").getDataType())
                .isEqualToIgnoringCase("bigint");
        Assertions.assertThat(actualTable.getColumn("number").getDataType())
                .isEqualToIgnoringCase("double");
    }

    @Test
    void testRejectCreateTableWhenExistingTableMissesInferredColumn() {
        TableId tableId = TableId.parse("test.narrow_existing_tbl");
        Schema existingSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType(false))
                        .physicalColumn("name", new IntType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, existingSchema));

        Schema firstMessageSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType(false))
                        .physicalColumn("name", new IntType())
                        .physicalColumn("email", new IntType())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new CreateTableEvent(tableId, firstMessageSchema)))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        exception ->
                                Assertions.assertThat(exception.getExceptionMessage())
                                        .contains("missing or incompatible")
                                        .contains("email"));

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();
        Assertions.assertThat(actualTable.getColumn("email")).isNull();
    }

    @Test
    void testRejectReplayWhenPrimaryKeysDiffer() {
        TableId tableId = TableId.parse("test.incompatible_replay_tbl");
        Schema currentSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType(false))
                        .physicalColumn("other_id", new IntType(false))
                        .primaryKey("other_id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, currentSchema));

        Schema historicalSchema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType(false))
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new CreateTableEvent(tableId, historicalSchema)))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        exception ->
                                Assertions.assertThat(exception.getExceptionMessage())
                                        .contains("primary keys"));
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

    @Test
    void testCreateTableWithTimeType() throws Exception {
        TableId tableId = TableId.parse("test.time_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType())
                        .physicalColumn("start_time", new TimeType())
                        .physicalColumn(
                                "end_time", new TimeType(3)) // TIME with millisecond precision
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();

        List<StarRocksColumn> columns = new ArrayList<>();
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("id")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("start_time")
                        .setOrdinalPosition(1)
                        .setDataType("varchar")
                        .setNullable(true)
                        .setColumnSize(8)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("end_time")
                        .setOrdinalPosition(2)
                        .setDataType("varchar")
                        .setNullable(true)
                        .setColumnSize(12)
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
    void testAddTimeTypeColumn() throws Exception {
        TableId tableId = TableId.parse("test.add_time_column");
        Schema schema =
                Schema.newBuilder().physicalColumn("id", new IntType()).primaryKey("id").build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        // Add TIME type column through schema evolution
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("duration", new TimeType())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("precision_time", new TimeType(3)))));
        metadataApplier.applySchemaChange(addColumnEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();

        List<StarRocksColumn> columns = new ArrayList<>();
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("id")
                        .setOrdinalPosition(0)
                        .setDataType("int")
                        .setNullable(true)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("duration")
                        .setOrdinalPosition(1)
                        .setDataType("varchar")
                        .setNullable(true)
                        .setColumnSize(8)
                        .build());
        columns.add(
                new StarRocksColumn.Builder()
                        .setColumnName("precision_time")
                        .setOrdinalPosition(2)
                        .setDataType("varchar")
                        .setNullable(true)
                        .setColumnSize(12)
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
    void testTimeTypeWithDifferentPrecisions() throws Exception {
        TableId tableId = TableId.parse("test.time_precision_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType())
                        .physicalColumn("time_default", new TimeType()) // Default precision
                        .physicalColumn("time_0", new TimeType(0)) // Second precision
                        .physicalColumn("time_3", new TimeType(3)) // Millisecond precision
                        .physicalColumn("time_max", new TimeType(3)) // Example precision 3
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        metadataApplier.applySchemaChange(createTableEvent);

        StarRocksTable actualTable =
                catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        Assertions.assertThat(actualTable).isNotNull();

        // Verify all TIME columns are correctly mapped to StarRocks VARCHAR type
        // since StarRocks doesn't support TIME type
        List<String> timeColumns = Arrays.asList("time_default", "time_0", "time_3", "time_max");
        for (StarRocksColumn column : actualTable.getColumns()) {
            if (timeColumns.contains(column.getColumnName())) {
                Assertions.assertThat(column.getDataType().toLowerCase()).isEqualTo("varchar");
            }
        }
    }
}
