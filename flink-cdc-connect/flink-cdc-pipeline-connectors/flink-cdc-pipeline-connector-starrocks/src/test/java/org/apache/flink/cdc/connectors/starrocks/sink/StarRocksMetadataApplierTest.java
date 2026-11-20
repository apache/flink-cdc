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
import org.apache.flink.cdc.common.event.ColumnPosition;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT;

/** Tests for {@link StarRocksMetadataApplier}. */
class StarRocksMetadataApplierTest {

    private MockStarRocksCatalog catalog;
    private StarRocksMetadataApplier metadataApplier;
    private SchemaChangeConfig schemaChangeConfig;
    private TableCreateConfig tableCreateConfig;

    @BeforeEach
    public void setup() {
        Configuration configuration =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(TABLE_SCHEMA_CHANGE_TIMEOUT.key(), "100s")
                                .put(TABLE_CREATE_NUM_BUCKETS.key(), "10")
                                .put("table.create.properties.replication_num", "5")
                                .build());
        this.schemaChangeConfig = SchemaChangeConfig.from(configuration);
        this.tableCreateConfig = TableCreateConfig.from(configuration);
        this.catalog = new MockStarRocksCatalog();
        this.metadataApplier =
                new StarRocksMetadataApplier(catalog, tableCreateConfig, schemaChangeConfig);
    }

    @Test
    void testReorderColumnSupportsMovesAndPreservesMetadata() throws Exception {
        TableId tableId = TableId.parse("test.reorder");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType().notNull())
                        .physicalColumn("a", new VarCharType(10).notNull(), "a's comment", "'a'")
                        .physicalColumn("b", new BigIntType(), "b comment", "42")
                        .physicalColumn("c", new DecimalType(10, 2))
                        .physicalColumn("d", new BooleanType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));

        Assertions.assertThat(
                        metadataApplier.acceptsSchemaEvolutionType(
                                SchemaChangeEventType.REORDER_COLUMN))
                .isTrue();

        ReorderColumnEvent reorderEvent =
                new ReorderColumnEvent(
                        tableId,
                        Arrays.asList(
                                move("c", ColumnPosition.AFTER, "id"),
                                move("b", ColumnPosition.LAST, null)));
        metadataApplier.applySchemaChange(reorderEvent);
        assertColumnOrder(tableId, "id", "c", "a", "d", "b");

        StarRocksTable reordered = getTable(tableId);
        Assertions.assertThat(reordered.getColumn("a").getDataType()).isEqualTo("VARCHAR");
        Assertions.assertThat(reordered.getColumn("a").isNullable()).isFalse();
        Assertions.assertThat(reordered.getColumn("a").getDefaultValue()).hasValue("'a'");
        Assertions.assertThat(reordered.getColumn("a").getColumnComment()).hasValue("a's comment");
        Assertions.assertThat(reordered.getTableKeys()).hasValue(schema.primaryKeys());
        Assertions.assertThat(reordered.getDistributionKeys()).hasValue(schema.primaryKeys());
        Assertions.assertThat(reordered.getProperties()).containsEntry("replication_num", "5");

        metadataApplier.applySchemaChange(reorderEvent);
        assertColumnOrder(tableId, "id", "c", "a", "d", "b");
    }

    @Test
    void testReplayingLegalMultiMoveDoesNotIssueAdditionalPositionChanges() throws Exception {
        CountingMockStarRocksCatalog countingCatalog = new CountingMockStarRocksCatalog();
        useCatalog(countingCatalog);
        TableId tableId = createReorderTable();
        ReorderColumnEvent reorderEvent = legalMultiMove(tableId);

        metadataApplier.applySchemaChange(reorderEvent);
        int callsAfterFirstApply = countingCatalog.alterColumnPositionCalls;
        Assertions.assertThat(callsAfterFirstApply).isPositive();

        metadataApplier.applySchemaChange(reorderEvent);
        Assertions.assertThat(countingCatalog.alterColumnPositionCalls)
                .isEqualTo(callsAfterFirstApply);
    }

    @Test
    void testReorderSucceedsWhenPositionChangeCompletesBeforeCatalogReportsFailure()
            throws Exception {
        CountingMockStarRocksCatalog failingCatalog = new CountingMockStarRocksCatalog();
        failingCatalog.failAfterSuccessfulPositionChange = true;
        useCatalog(failingCatalog);
        TableId tableId = createReorderTable();
        ReorderColumnEvent reorderEvent =
                new ReorderColumnEvent(
                        tableId, Collections.singletonList(move("c", ColumnPosition.AFTER, "id")));

        metadataApplier.applySchemaChange(reorderEvent);

        assertColumnOrder(tableId, "id", "c", "a", "b", "d");
    }

    @Test
    void testReorderFailureAfterPartialChangeCanBeRetriedToConverge() throws Exception {
        CountingMockStarRocksCatalog failingCatalog = new CountingMockStarRocksCatalog();
        failingCatalog.failOnPositionChangeCall = 1;
        useCatalog(failingCatalog);
        TableId tableId = createReorderTable();
        ReorderColumnEvent reorderEvent = legalMultiMove(tableId);

        Assertions.assertThatThrownBy(() -> metadataApplier.applySchemaChange(reorderEvent))
                .isInstanceOf(SchemaEvolveException.class);
        assertColumnOrder(tableId, "id", "c", "a", "b", "d");

        failingCatalog.failOnPositionChangeCall = -1;
        metadataApplier.applySchemaChange(reorderEvent);
        assertColumnOrder(tableId, "id", "c", "a", "d", "b");
    }

    @Test
    void testReorderColumnSupportsFirstBeforeAndLast() throws Exception {
        TableId tableId = TableId.parse("test.reorder_positions");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType().notNull())
                        .physicalColumn("a", new IntType())
                        .physicalColumn("b", new IntType())
                        .physicalColumn("c", new IntType())
                        .physicalColumn("d", new IntType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));

        metadataApplier.applySchemaChange(
                new ReorderColumnEvent(
                        tableId,
                        Collections.singletonList(move("id", ColumnPosition.FIRST, null))));
        assertColumnOrder(tableId, "id", "a", "b", "c", "d");

        metadataApplier.applySchemaChange(
                new ReorderColumnEvent(
                        tableId, Collections.singletonList(move("a", ColumnPosition.LAST, null))));
        assertColumnOrder(tableId, "id", "b", "c", "d", "a");

        metadataApplier.applySchemaChange(
                new ReorderColumnEvent(
                        tableId, Collections.singletonList(move("c", ColumnPosition.BEFORE, "b"))));
        assertColumnOrder(tableId, "id", "c", "b", "d", "a");
    }

    @Test
    void testReorderColumnRejectsIncompatiblePrimaryKeyOrdersBeforeChangingTable()
            throws Exception {
        TableId tableId = TableId.parse("test.reorder_keys");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType().notNull())
                        .physicalColumn("key2", new IntType().notNull())
                        .physicalColumn("value", new IntType())
                        .primaryKey("id", "key2")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));

        assertRejectedWithoutChangingTable(tableId, move("value", ColumnPosition.FIRST, null));
        assertRejectedWithoutChangingTable(tableId, move("id", ColumnPosition.AFTER, "value"));
        assertRejectedWithoutChangingTable(tableId, move("key2", ColumnPosition.FIRST, null));
    }

    private void assertRejectedWithoutChangingTable(
            TableId tableId, ReorderColumnEvent.ColumnMove move) {
        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new ReorderColumnEvent(
                                                tableId, Collections.singletonList(move))))
                .isInstanceOf(SchemaEvolveException.class);
        assertColumnOrder(tableId, "id", "key2", "value");
    }

    private StarRocksTable getTable(TableId tableId) {
        return catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElseThrow();
    }

    private void useCatalog(MockStarRocksCatalog catalog) {
        this.catalog = catalog;
        this.metadataApplier =
                new StarRocksMetadataApplier(catalog, tableCreateConfig, schemaChangeConfig);
    }

    private TableId createReorderTable() {
        TableId tableId = TableId.parse("test.reorder_retry");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", new IntType().notNull())
                        .physicalColumn("a", new IntType())
                        .physicalColumn("b", new IntType())
                        .physicalColumn("c", new IntType())
                        .physicalColumn("d", new IntType())
                        .primaryKey("id")
                        .build();
        metadataApplier.applySchemaChange(new CreateTableEvent(tableId, schema));
        return tableId;
    }

    private static ReorderColumnEvent legalMultiMove(TableId tableId) {
        return new ReorderColumnEvent(
                tableId,
                Arrays.asList(
                        move("c", ColumnPosition.AFTER, "id"),
                        move("b", ColumnPosition.LAST, null)));
    }

    private void assertColumnOrder(TableId tableId, String... expected) {
        Assertions.assertThat(
                        getTable(tableId).getColumns().stream()
                                .map(StarRocksColumn::getColumnName)
                                .collect(Collectors.toList()))
                .containsExactly(expected);
    }

    private static ReorderColumnEvent.ColumnMove move(
            String columnName, ColumnPosition position, String referenceColumnName) {
        return new ReorderColumnEvent.ColumnMove(columnName, position, referenceColumnName);
    }

    private static class CountingMockStarRocksCatalog extends MockStarRocksCatalog {
        private int alterColumnPositionCalls;
        private int failOnPositionChangeCall = -1;
        private boolean failAfterSuccessfulPositionChange;

        @Override
        public void alterColumnPosition(
                String databaseName,
                String tableName,
                StarRocksColumn column,
                boolean isKey,
                String precedingColumn,
                long timeoutSecond)
                throws StarRocksCatalogException {
            super.alterColumnPosition(
                    databaseName, tableName, column, isKey, precedingColumn, timeoutSecond);
            alterColumnPositionCalls++;
            if (failAfterSuccessfulPositionChange
                    || alterColumnPositionCalls == failOnPositionChangeCall) {
                throw new StarRocksCatalogException("Injected position-change failure");
            }
        }
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
                                        Column.physicalColumn(
                                                "col3",
                                                new SmallIntType(),
                                                "comment \"quoted\" \\ path",
                                                "default \"quoted\" \\ path"))));
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
                        .setColumnComment("comment \\\"quoted\\\" \\\\ path")
                        .setDefaultValue("default \\\"quoted\\\" \\\\ path")
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
