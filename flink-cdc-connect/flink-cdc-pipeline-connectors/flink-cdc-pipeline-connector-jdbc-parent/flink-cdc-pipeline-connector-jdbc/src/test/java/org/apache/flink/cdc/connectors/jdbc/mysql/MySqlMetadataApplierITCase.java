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

package org.apache.flink.cdc.connectors.jdbc.mysql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** IT cases for MySQL Metadata Applier. */
class MySqlMetadataApplierITCase extends MySqlSinkTestBase {

    private static final TableId TABLE_ID =
            TableId.tableId(
                    MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName(),
                    "metadata_applier_test_table");

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    static void before() {
        env.setParallelism(MySqlSinkTestBase.DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @BeforeEach
    void initializeDatabase() {
        MySqlSinkTestBase.executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;",
                        MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName()));
        MySqlSinkTestBase.LOG.info(
                "Database {} created.", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName());
    }

    @AfterEach
    void destroyDatabase() {
        MySqlSinkTestBase.executeSql(
                String.format(
                        "DROP DATABASE %s;", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName()));
        MySqlSinkTestBase.LOG.info(
                "Database {} destroyed.", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName());
    }

    private List<Event> generateAddColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn("extra_date", DataTypes.DATE(), null)))),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_bool", DataTypes.BOOLEAN(), null)))),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_decimal",
                                                DataTypes.DECIMAL(17, 0),
                                                null)))));
    }

    private List<Event> generateDropColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new DropColumnEvent(tableId, Collections.singletonList("number")));
    }

    private List<Event> generateRenameColumnEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new RenameColumnEvent(tableId, Collections.singletonMap("number", "kazu")),
                new RenameColumnEvent(tableId, Collections.singletonMap("name", "namae")));
    }

    private List<Event> generateAlterColumnTypeEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("name", DataTypes.VARCHAR(19))));
    }

    private List<Event> generateNarrowingAlterColumnTypeEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                // Double -> Float is a narrowing cast, should fail
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("number", DataTypes.FLOAT())));
    }

    private List<Event> generateAddColumnWithPosition(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("col1", DataTypes.VARCHAR(10), null))
                        .column(new PhysicalColumn("col2", DataTypes.VARCHAR(10), null))
                        .column(new PhysicalColumn("col3", DataTypes.VARCHAR(10), null))
                        .build();

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                AddColumnEvent.first(Column.physicalColumn("col4_first", DataTypes.VARCHAR(10))));
        addedColumns.add(
                AddColumnEvent.last(Column.physicalColumn("col5_last", DataTypes.VARCHAR(10))));

        addedColumns.add(
                AddColumnEvent.after(
                        Column.physicalColumn("col7_after", DataTypes.VARCHAR(10)), "col2"));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema), new AddColumnEvent(tableId, addedColumns));
    }

    private List<Event> generateTruncateTableEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17, 3.1415926, BinaryStringData.fromString("Alice")
                                })),
                new TruncateTableEvent(tableId));
    }

    private List<Event> generateDropTableEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17, 3.1415926, BinaryStringData.fromString("Alice")
                                })),
                new DropTableEvent(tableId));
    }

    @Test
    void testMySqlAddColumn() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateAddColumnEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null",
                        "extra_date | date | YES |  | null",
                        "extra_bool | tinyint(1) | YES |  | null",
                        "extra_decimal | decimal(17,0) | YES |  | null");
    }

    @Test
    void testMySqlDropColumn() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateDropColumnEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null", "name | varchar(17) | YES |  | null");
    }

    @Test
    void testMySqlRenameColumn() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateRenameColumnEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "kazu | double | YES |  | null",
                        "namae | varchar(17) | YES |  | null");
    }

    @Test
    void testMySqlAlterColumnType() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateAlterColumnTypeEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(19) | YES |  | null");
    }

    @Test
    void testMySqlNarrowingAlterColumnType() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateNarrowingAlterColumnTypeEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | float | YES |  | null",
                        "name | varchar(17) | YES |  | null");
    }

    @Test
    void testAddColumnWithPosition() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateAddColumnWithPosition(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "col4_first | varchar(10) | YES |  | null",
                        "col1 | varchar(10) | YES |  | null",
                        "col2 | varchar(10) | YES |  | null",
                        "col7_after | varchar(10) | YES |  | null",
                        "col3 | varchar(10) | YES |  | null",
                        "col5_last | varchar(10) | YES |  | null");
    }

    @Test
    void testMySqlTruncateTable() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateTruncateTableEvents(TABLE_ID));
        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3)).isEmpty();
    }

    @Test
    void testMySqlDropTable() throws Exception {
        runJobThatSinksToMySqlWithEvents(generateDropTableEvents(TABLE_ID));

        Assertions.assertThatThrownBy(() -> inspectTableSchema(TABLE_ID))
                .isExactlyInstanceOf(SQLSyntaxErrorException.class)
                .hasMessage("Table '" + TABLE_ID + "' doesn't exist");
    }
}
