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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link MySqlDataSource}. */
class MySqlTableIdCaseInsensitveITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/tablename-sensitive/my.cnf");
    private static final String MIXED_CASE_ORDERS = "mixed_case_orders";

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        RestartStrategyUtils.configureNoRestartStrategy(env);
    }

    @ParameterizedTest
    @ValueSource(strings = {"products", "uppercase_products"})
    public void testParseAlterStatementWhenTableNameAndColumnIsUpper(String tableName)
            throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\." + tableName)
                        .startupOptions(StartupOptions.latest())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), tableName);
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId, tableName));
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expected.addAll(executeAlterAndProvideExpected(tableId, statement, tableName));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` ADD `COLS1` VARCHAR(45);",
                            inventoryDatabase.getDatabaseName(), tableName));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    getExpectedRenamedColumnName(
                                                            tableName, "COLS1"),
                                                    DataTypes.VARCHAR(45))))));
        }
        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"products", "uppercase_products"})
    public void testSnapshotModeWhenTableNameAndColumnIsUpper(String tableName) throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\." + tableName)
                        .startupOptions(StartupOptions.snapshot())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), tableName);
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId, tableName));
        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testInitialModePreservesMixedCaseColumnsAndData() throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        createMixedCaseOrdersTable();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\." + MIXED_CASE_ORDERS)
                        .startupOptions(StartupOptions.initial())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(10_000);

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(getMixedCaseOrdersRowType());
        List<Event> expected = new ArrayList<>();
        expected.add(getMixedCaseOrdersCreateTableEvent(tableId));
        expected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1,
                                    101,
                                    BinaryStringData.fromString("PARK-001"),
                                    BinaryStringData.fromString("ORDER-001"),
                                    BinaryStringData.fromString("CARD-001")
                                })));

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`%s` (`ID`, `park_id`, `park_code`, `order_no`, `CARD_ID`) "
                                    + "VALUES (2, 202, 'PARK-002', 'ORDER-002', 'CARD-002');",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
        }
        expected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    2,
                                    202,
                                    BinaryStringData.fromString("PARK-002"),
                                    BinaryStringData.fromString("ORDER-002"),
                                    BinaryStringData.fromString("CARD-002")
                                })));

        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMixedCaseColumnsRemainStableAcrossSchemaChanges() throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        createMixedCaseOrdersTable();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\." + MIXED_CASE_ORDERS)
                        .startupOptions(StartupOptions.latest())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(getMixedCaseOrdersRowType());
        List<Event> expected = new ArrayList<>();
        expected.add(getMixedCaseOrdersCreateTableEvent(tableId));

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`%s` (`ID`, `park_id`, `park_code`, `order_no`, `CARD_ID`) "
                                    + "VALUES (2, 202, 'PARK-002', 'ORDER-002', 'CARD-002');",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        2,
                                        202,
                                        BinaryStringData.fromString("PARK-002"),
                                        BinaryStringData.fromString("ORDER-002"),
                                        BinaryStringData.fromString("CARD-002")
                                    })));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` RENAME COLUMN `park_code` TO `PARK_CODE`;",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(
                    new RenameColumnEvent(
                            tableId, Collections.singletonMap("park_code", "PARK_CODE")));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` CHANGE COLUMN `PARK_CODE` `park_code` VARCHAR(32) NULL DEFAULT NULL;",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(
                    new AlterColumnTypeEvent(
                            tableId, Collections.singletonMap("PARK_CODE", DataTypes.VARCHAR(32))));
            expected.add(
                    new RenameColumnEvent(
                            tableId, Collections.singletonMap("PARK_CODE", "park_code")));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` ADD COLUMN `trace_no` VARCHAR(32) NULL AFTER `CARD_ID`;",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "trace_no", DataTypes.VARCHAR(32)),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            "CARD_ID"))));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` CHANGE COLUMN `CARD_ID` `CARD_NO` VARCHAR(128) NULL DEFAULT NULL;",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(
                    new AlterColumnTypeEvent(
                            tableId, Collections.singletonMap("CARD_ID", DataTypes.VARCHAR(128))));
            expected.add(
                    new RenameColumnEvent(tableId, Collections.singletonMap("CARD_ID", "CARD_NO")));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`%s` DROP COLUMN `CARD_NO`;",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("CARD_NO")));
        }

        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId, String tableName) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn(
                                getExpectedColumnName(tableName, "id"), DataTypes.INT().notNull())
                        .physicalColumn(
                                getExpectedColumnName(tableName, "name"),
                                DataTypes.VARCHAR(255).notNull(),
                                null,
                                "flink")
                        .physicalColumn(
                                getExpectedColumnName(tableName, "description"),
                                DataTypes.VARCHAR(512))
                        .physicalColumn(
                                getExpectedColumnName(tableName, "weight"), DataTypes.FLOAT())
                        .primaryKey(
                                Collections.singletonList(getExpectedColumnName(tableName, "id")))
                        .build());
    }

    private CreateTableEvent getMixedCaseOrdersCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT().notNull())
                        .physicalColumn("park_id", DataTypes.INT())
                        .physicalColumn("park_code", DataTypes.VARCHAR(32))
                        .physicalColumn("order_no", DataTypes.VARCHAR(32))
                        .physicalColumn("CARD_ID", DataTypes.VARCHAR(32))
                        .primaryKey(Collections.singletonList("ID"))
                        .build());
    }

    /**
     * * The final schema of table products is as follows.
     *
     * <pre>
     * CREATE TABLE products (
     *   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
     *   name VARCHAR(255) NOT NULL DEFAULT 'flink',
     *   weight FLOAT,
     *   col1 VARCHAR(45),
     *   col2 VARCHAR(55)
     * );
     * </pre>
     */
    private List<Event> executeAlterAndProvideExpected(
            TableId tableId, Statement statement, String tableName) throws SQLException {
        List<Event> expected = new ArrayList<>();
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` CHANGE COLUMN `DESCRIPTION` `DESC` VARCHAR(255) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedColumnName(tableName, "description"),
                                DataTypes.VARCHAR(255))));
        expected.add(
                new RenameColumnEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedColumnName(tableName, "description"),
                                getExpectedRenamedColumnName(tableName, "DESC"))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` CHANGE COLUMN `desc` `desc2` VARCHAR(400) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedRenamedColumnName(tableName, "DESC"),
                                DataTypes.VARCHAR(400))));
        expected.add(
                new RenameColumnEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedRenamedColumnName(tableName, "DESC"), "desc2")));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` ADD COLUMN `DESC1` VARCHAR(45) NULL AFTER `weight`;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                getExpectedRenamedColumnName(tableName, "DESC1"),
                                                DataTypes.VARCHAR(45)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        getExpectedColumnName(tableName, "weight")))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` ADD COLUMN `col1` VARCHAR(45) NULL AFTER `weight`, ADD COLUMN `COL2` VARCHAR(55) NULL AFTER `desc1`;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col1", DataTypes.VARCHAR(45)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        getExpectedColumnName(tableName, "weight")))));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                getExpectedRenamedColumnName(tableName, "COL2"),
                                                DataTypes.VARCHAR(55)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        getExpectedRenamedColumnName(tableName, "DESC1")))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` ADD COLUMN `TRACE_CASE` VARCHAR(45) NULL AFTER `weight`, ADD COLUMN `trace_tail` VARCHAR(55) NULL AFTER `trace_case`;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                getExpectedRenamedColumnName(
                                                        tableName, "TRACE_CASE"),
                                                DataTypes.VARCHAR(45)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        getExpectedColumnName(tableName, "weight")))));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("trace_tail", DataTypes.VARCHAR(55)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        getExpectedRenamedColumnName(tableName, "TRACE_CASE")))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` DROP COLUMN `desc2`, CHANGE COLUMN `desc1` `desc1` VARCHAR(65) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("desc2")));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedRenamedColumnName(tableName, "DESC1"),
                                DataTypes.VARCHAR(65))));

        // Only available in mysql 8.0
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` RENAME COLUMN `desc1` TO `desc3`;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new RenameColumnEvent(
                        tableId,
                        Collections.singletonMap(
                                getExpectedRenamedColumnName(tableName, "DESC1"), "desc3")));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` MODIFY COLUMN `DESC3` VARCHAR(255) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("desc3", DataTypes.VARCHAR(255))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`%s` DROP COLUMN `desc3`;",
                        inventoryDatabase.getDatabaseName(), tableName));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("desc3")));

        // Should not catch SchemaChangeEvent of tables other than `products`
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`orders` ADD COLUMN `desc1` VARCHAR(45) NULL;",
                        inventoryDatabase.getDatabaseName()));
        return expected;
    }

    private String getExpectedColumnName(String tableName, String lowerCaseName) {
        return isUppercaseProductsTable(tableName) ? lowerCaseName.toUpperCase() : lowerCaseName;
    }

    private String getExpectedRenamedColumnName(String tableName, String sqlDefinedName) {
        return isUppercaseProductsTable(tableName) ? sqlDefinedName : sqlDefinedName.toLowerCase();
    }

    private boolean isUppercaseProductsTable(String tableName) {
        return "uppercase_products".equals(tableName);
    }

    private RowType getMixedCaseOrdersRowType() {
        return RowType.of(
                new DataType[] {
                    DataTypes.INT().notNull(),
                    DataTypes.INT(),
                    DataTypes.VARCHAR(32),
                    DataTypes.VARCHAR(32),
                    DataTypes.VARCHAR(32)
                },
                new String[] {"ID", "park_id", "park_code", "order_no", "CARD_ID"});
    }

    private void createMixedCaseOrdersTable() throws SQLException {
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`%s` ("
                                    + "`ID` INT NOT NULL,"
                                    + "`park_id` INT NULL,"
                                    + "`park_code` VARCHAR(32) NULL,"
                                    + "`order_no` VARCHAR(32) NULL,"
                                    + "`CARD_ID` VARCHAR(32) NULL,"
                                    + "PRIMARY KEY (`ID`));",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`%s` (`ID`, `park_id`, `park_code`, `order_no`, `CARD_ID`) "
                                    + "VALUES (1, 101, 'PARK-001', 'ORDER-001', 'CARD-001');",
                            inventoryDatabase.getDatabaseName(), MIXED_CASE_ORDERS));
        }
    }
}
