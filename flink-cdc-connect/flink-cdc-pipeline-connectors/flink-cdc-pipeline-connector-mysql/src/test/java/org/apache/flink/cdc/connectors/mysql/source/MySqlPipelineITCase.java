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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.lifecycle.Startables;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.INCLUDE_COMMENTS_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link MySqlDataSource}. */
class MySqlPipelineITCase extends MySqlSourceTestBase {

    protected static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0);

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
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    void testInitialStartupMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\.products")
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

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.FLOAT(),
                                DataTypes.VARCHAR(45),
                                DataTypes.VARCHAR(55)
                            },
                            new String[] {"id", "name", "weight", "col1", "col2"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'scooter',5.5,'c-10','c-20');",
                            inventoryDatabase.getDatabaseName())); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'football',6.6,'c-11','c-21');",
                            inventoryDatabase.getDatabaseName())); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE `%s`.`products` SET `col1`='c-12', `col2`='c-22' WHERE id=110;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-12"),
                                        BinaryStringData.fromString("c-22")
                                    })));
            statement.execute(
                    String.format(
                            "DELETE FROM `%s`.`products` WHERE `id` = 111;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
        }
        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual =
                fetchResultsExcept(
                        events, expectedSnapshot.size() + expectedBinlog.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(actual.subList(expectedSnapshot.size(), actual.size()))
                .isEqualTo(expectedBinlog);
    }

    @Test
    void testSqlInjection() throws Exception {
        inventoryDatabase.createAndInitialize();
        env.setParallelism(1);
        String sqlInjectionTable = "sqlInjection`; DROP TABLE important_data; --";
        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), sqlInjectionTable);
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s (  "
                                    + "id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                                    + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                                    + "  description VARCHAR(512),\n"
                                    + "  weight FLOAT(6)"
                                    + ");\n",
                            StatementUtils.quote(inventoryDatabase.getDatabaseName()),
                            StatementUtils.quote(sqlInjectionTable)));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.%s AUTO_INCREMENT = 101;",
                            StatementUtils.quote(inventoryDatabase.getDatabaseName()),
                            StatementUtils.quote(sqlInjectionTable)));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.%s\n"
                                    + "VALUES (default,\"scooter\",\"Small 2-wheel scooter\",3.14),\n"
                                    + "       (default,\"car battery\",\"12V car battery\",8.1),\n"
                                    + "       (default,\"12-pack drill bits\",\"12-pack of drill bits with sizes ranging from #40 to #3\",0.8),\n"
                                    + "       (default,\"hammer\",\"12oz carpenter's hammer\",0.75),\n"
                                    + "       (default,\"hammer\",\"14oz carpenter's hammer\",0.875),\n"
                                    + "       (default,\"hammer\",\"16oz carpenter's hammer\",1.0),\n"
                                    + "       (default,\"rocks\",\"box of assorted rocks\",5.3),\n"
                                    + "       (default,\"jacket\",\"water resistent black wind breaker\",0.1),\n"
                                    + "       (default,\"spare tire\",\"24 inch spare tire\",22.2);",
                            StatementUtils.quote(inventoryDatabase.getDatabaseName()),
                            StatementUtils.quote(sqlInjectionTable)));
        }
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\.sql.*")
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
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn(
                                        "name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                                .physicalColumn("description", DataTypes.VARCHAR(512))
                                .physicalColumn("weight", DataTypes.FLOAT())
                                .primaryKey(Collections.singletonList("id"))
                                .build());

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.%s ADD COLUMN `desc1` VARCHAR(45) NULL AFTER `weight`;",
                            StatementUtils.quote(inventoryDatabase.getDatabaseName()),
                            StatementUtils.quote(sqlInjectionTable)));
            expectedBinlog.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("desc1", DataTypes.VARCHAR(45)),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            "weight"))));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.%s\n"
                                    + "VALUES (default,\"scooter\",\"Small 2-wheel scooter\",3.14, 1.1),\n"
                                    + "       (default,\"car battery\",\"12V car battery\",8.1, 1.1);",
                            StatementUtils.quote(inventoryDatabase.getDatabaseName()),
                            StatementUtils.quote(sqlInjectionTable)));
            RowType rowType =
                    RowType.of(
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.FLOAT(),
                            DataTypes.VARCHAR(45));
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("Small 2-wheel scooter"),
                                        3.14f,
                                        BinaryStringData.fromString("1.1")
                                    })));
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("car battery"),
                                        BinaryStringData.fromString("12V car battery"),
                                        8.1f,
                                        BinaryStringData.fromString("1.1")
                                    })));
        }

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual =
                fetchResultsExcept(
                        events, expectedSnapshot.size() + expectedBinlog.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(actual.subList(expectedSnapshot.size(), actual.size()))
                .isEqualTo(expectedBinlog);
    }

    @Test
    void testLatestOffsetStartupMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\.products")
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
        Thread.sleep(10_000);
        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "products");

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.FLOAT(),
                                DataTypes.VARCHAR(45),
                                DataTypes.VARCHAR(55)
                            },
                            new String[] {"id", "name", "weight", "col1", "col2"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'scooter',5.5,'c-10','c-20');",
                            inventoryDatabase.getDatabaseName())); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'football',6.6,'c-11','c-21');",
                            inventoryDatabase.getDatabaseName())); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE `%s`.`products` SET `col1`='c-12', `col2`='c-22' WHERE id=110;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-12"),
                                        BinaryStringData.fromString("c-22")
                                    })));
            statement.execute(
                    String.format(
                            "DELETE FROM `%s`.`products` WHERE `id` = 111;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
        }
        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.

        Event createTableEvent = getProductsCreateTableEvent(tableId);
        List<Event> actual = fetchResultsExcept(events, expectedBinlog.size(), createTableEvent);
        assertThat(actual).isEqualTo(expectedBinlog);
    }

    @ParameterizedTest(name = "batchEmit: {0}")
    @ValueSource(booleans = {true, false})
    void testExcludeTables(boolean inBatch) throws Exception {
        inventoryDatabase.createAndInitialize();
        String databaseName = inventoryDatabase.getDatabaseName();
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(databaseName)
                        .tableList(databaseName + ".*")
                        .excludeTableList(
                                String.format(
                                        "%s.customers, %s.orders, %s.multi_max_table",
                                        databaseName, databaseName, databaseName))
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

        TableId tableId = TableId.tableId(databaseName, "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.FLOAT(),
                                DataTypes.VARCHAR(45),
                                DataTypes.VARCHAR(55)
                            },
                            new String[] {"id", "name", "weight", "col1", "col2"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'scooter',5.5,'c-10','c-20');",
                            databaseName)); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'football',6.6,'c-11','c-21');",
                            databaseName)); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE `%s`.`products` SET `col1`='c-12', `col2`='c-22' WHERE id=110;",
                            databaseName));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-12"),
                                        BinaryStringData.fromString("c-22")
                                    })));
            statement.execute(
                    String.format("DELETE FROM `%s`.`products` WHERE `id` = 111;", databaseName));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
            // Make some change of excluded tables.
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` VALUES(1,\"Anne\",\"Kretchmar\",\"mark@noanswer.org\");",
                            databaseName));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` VALUES(2,\"Anne\",\"Kretchmar\",\"mark2@noanswer.org\");",
                            databaseName));
        }
        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual =
                fetchResultsExcept(
                        events, expectedSnapshot.size() + expectedBinlog.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(actual.subList(expectedSnapshot.size(), actual.size()))
                .isEqualTo(expectedBinlog);
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (!event.equals(sideEvent)) {
                result.add(event);
                size--;
            } else {
                sideResults.add(sideEvent);
            }
        }
        // Also ensure we've received at least one or many side events.
        assertThat(sideResults).isNotEmpty();
        return result;
    }

    @Test
    public void testParseAlterStatementTinyintIsBit() throws Exception {
        testParseAlterStatement(true);
    }

    @Test
    public void testParseAlterStatementTinyint1IsNotBit() throws Exception {
        testParseAlterStatement(false);
    }

    @Test
    void testInitialStartupModeWithOpTs() throws Exception {
        inventoryDatabase.createAndInitialize();
        Configuration sourceConfiguration = new Configuration();
        sourceConfiguration.set(MySqlDataSourceOptions.HOSTNAME, MYSQL8_CONTAINER.getHost());
        sourceConfiguration.set(MySqlDataSourceOptions.PORT, MYSQL8_CONTAINER.getDatabasePort());
        sourceConfiguration.set(MySqlDataSourceOptions.USERNAME, TEST_USER);
        sourceConfiguration.set(MySqlDataSourceOptions.PASSWORD, TEST_PASSWORD);
        sourceConfiguration.set(
                MySqlDataSourceOptions.TABLES, inventoryDatabase.getDatabaseName() + ".products");
        sourceConfiguration.set(
                MySqlDataSourceOptions.SERVER_ID, getServerId(env.getParallelism()));
        sourceConfiguration.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfiguration.set(MySqlDataSourceOptions.METADATA_LIST, "op_ts");
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration, new Configuration(), this.getClass().getClassLoader());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new MySqlDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(10_000);
        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);
        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("op_ts", "0");
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(),
                                            dataChangeEvent.after(),
                                            meta);
                                })
                        .collect(Collectors.toList());
        String startTime = String.valueOf(System.currentTimeMillis());
        Thread.sleep(1000);
        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.FLOAT(),
                                DataTypes.VARCHAR(45),
                                DataTypes.VARCHAR(55)
                            },
                            new String[] {"id", "name", "weight", "col1", "col2"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'scooter',5.5,'c-10','c-20');",
                            inventoryDatabase.getDatabaseName())); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'football',6.6,'c-11','c-21');",
                            inventoryDatabase.getDatabaseName())); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE `%s`.`products` SET `col1`='c-12', `col2`='c-22' WHERE id=110;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-10"),
                                        BinaryStringData.fromString("c-20")
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        5.5f,
                                        BinaryStringData.fromString("c-12"),
                                        BinaryStringData.fromString("c-22")
                                    })));
            statement.execute(
                    String.format(
                            "DELETE FROM `%s`.`products` WHERE `id` = 111;",
                            inventoryDatabase.getDatabaseName()));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        6.6f,
                                        BinaryStringData.fromString("c-11"),
                                        BinaryStringData.fromString("c-21")
                                    })));
        }

        int snapshotRecordsCount = expectedSnapshot.size();
        int binlogRecordsCount = expectedBinlog.size();

        // Ditto, CreateTableEvent might be emitted in multiple partitions.
        List<Event> actual =
                fetchResultsExcept(
                        events, snapshotRecordsCount + binlogRecordsCount, createTableEvent);

        List<Event> actualSnapshotEvents = actual.subList(0, snapshotRecordsCount);
        List<Event> actualBinlogEvents = actual.subList(snapshotRecordsCount, actual.size());

        assertThat(actualSnapshotEvents).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        assertThat(actualBinlogEvents).hasSize(binlogRecordsCount);

        for (int i = 0; i < binlogRecordsCount; i++) {
            if (expectedBinlog.get(i) instanceof SchemaChangeEvent) {
                assertThat(actualBinlogEvents.get(i)).isEqualTo(expectedBinlog.get(i));
            } else {
                DataChangeEvent expectedEvent = (DataChangeEvent) expectedBinlog.get(i);
                DataChangeEvent actualEvent = (DataChangeEvent) actualBinlogEvents.get(i);
                assertThat(actualEvent.op()).isEqualTo(expectedEvent.op());
                assertThat(actualEvent.before()).isEqualTo(expectedEvent.before());
                assertThat(actualEvent.after()).isEqualTo(expectedEvent.after());
                assertThat(actualEvent.meta().get("op_ts")).isGreaterThanOrEqualTo(startTime);
            }
        }
    }

    private void testParseAlterStatement(boolean tinyInt1isBit) throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + "\\.products")
                        .startupOptions(StartupOptions.latest())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .treatTinyInt1AsBoolean(tinyInt1isBit)
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

        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "products");
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId));
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expected.addAll(executeAlterAndProvideExpected(tableId, statement));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN (`cols1` VARCHAR(45), `cols2` VARCHAR(55));",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("cols1", DataTypes.VARCHAR(45))),
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols2", DataTypes.VARCHAR(55))))));
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN (`cols3` VARCHAR(45), `cols4` VARCHAR(55));",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("cols3", DataTypes.VARCHAR(45))),
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols4", DataTypes.VARCHAR(55))))));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols5` BIT NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("cols5", DataTypes.BOOLEAN())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols6` BINARY(0) NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols6", BinaryType.ofEmptyLiteral())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols7` BINARY NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("cols7", DataTypes.BINARY(1))))));
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols8` CHAR(0) NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols8", CharType.ofEmptyLiteral())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols9` CHAR NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("cols9", DataTypes.CHAR(1))))));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols10` TINYINT(1) NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols10",
                                                    tinyInt1isBit
                                                            ? DataTypes.BOOLEAN()
                                                            : DataTypes.TINYINT())))));

            // Drop orders table first to remove foreign key restraints
            statement.execute(
                    String.format(
                            "DROP TABLE `%s`.`orders`;", inventoryDatabase.getDatabaseName()));

            statement.execute(
                    String.format(
                            "TRUNCATE TABLE `%s`.`products`;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(new TruncateTableEvent(tableId));

            statement.execute(
                    String.format(
                            "DROP TABLE `%s`.`products`;", inventoryDatabase.getDatabaseName()));
            expected.add(new DropTableEvent(tableId));
        }
        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testSchemaChangeEventstinyInt1isBit() throws Exception {
        testSchemaChangeEvents(true);
    }

    @Test
    public void testSchemaChangeEventsTinyint1IsNotBit() throws Exception {
        testSchemaChangeEvents(false);
    }

    public void testSchemaChangeEvents(boolean tinyInt1isBit) throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".*")
                        .startupOptions(StartupOptions.latest())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .treatTinyInt1AsBoolean(tinyInt1isBit)
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

        List<Event> expected = new ArrayList<>();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` ADD COLUMN `newcol1` INT NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT().notNull())
                                    .physicalColumn("first_name", DataTypes.VARCHAR(255).notNull())
                                    .physicalColumn("last_name", DataTypes.VARCHAR(255).notNull())
                                    .physicalColumn("email", DataTypes.VARCHAR(255).notNull())
                                    .primaryKey(Collections.singletonList("id"))
                                    .build()));
            expected.add(
                    new AddColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("newcol1", DataTypes.INT())))));

            // Add a TINYINT(1) column
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` ADD COLUMN `new_tinyint1_col1` TINYINT(1) NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "new_tinyint1_col1",
                                                    tinyInt1isBit
                                                            ? DataTypes.BOOLEAN()
                                                            : DataTypes.TINYINT())))));

            // Add a new BOOLEAN column
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` ADD COLUMN `new_bool_col1` bool NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "new_bool_col1", DataTypes.BOOLEAN())))));

            // Test MODIFY COLUMN DDL
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` MODIFY COLUMN `newcol1` DOUBLE;",
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new AlterColumnTypeEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("newcol1", DataTypes.DOUBLE())));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` MODIFY COLUMN `new_tinyint1_col1` INT;",
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new AlterColumnTypeEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("new_tinyint1_col1", DataTypes.INT())));

            // Test CHANGE COLUMN DDL
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` CHANGE COLUMN `newcol1` `newcol2` INT;",
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new AlterColumnTypeEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("newcol1", DataTypes.INT())));

            expected.add(
                    new RenameColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("newcol1", "newcol2")));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` CHANGE COLUMN `newcol2` `newcol1` DOUBLE;",
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new AlterColumnTypeEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("newcol2", DataTypes.DOUBLE())));

            expected.add(
                    new RenameColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonMap("newcol2", "newcol1")));

            // Test truncate table DDL
            statement.execute(
                    String.format(
                            "TRUNCATE TABLE `%s`.`orders`;", inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "orders"),
                            Schema.newBuilder()
                                    .physicalColumn("order_number", DataTypes.INT().notNull())
                                    .physicalColumn("order_date", DataTypes.DATE().notNull())
                                    .physicalColumn("purchaser", DataTypes.INT().notNull())
                                    .physicalColumn("quantity", DataTypes.INT().notNull())
                                    .physicalColumn("product_id", DataTypes.INT().notNull())
                                    .primaryKey(Collections.singletonList("order_number"))
                                    .build()));
            expected.add(
                    new TruncateTableEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "orders")));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` ADD COLUMN `varchar0` VARCHAR(0) NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers"),
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "varchar0", DataTypes.STRING())))));

            // Test drop table DDL
            statement.execute(
                    String.format(
                            "DROP TABLE `%s`.`orders`, `%s`.`customers`;",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new DropTableEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "orders")));
            expected.add(
                    new DropTableEvent(
                            TableId.tableId(inventoryDatabase.getDatabaseName(), "customers")));

            // Test create table DDL
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable1`("
                                    + "    id                   SERIAL,\n"
                                    + "    tiny_c               TINYINT,\n"
                                    + "    tiny_un_c            TINYINT UNSIGNED,\n"
                                    + "    tiny_un_z_c          TINYINT UNSIGNED ZEROFILL,\n"
                                    + "    small_c              SMALLINT,\n"
                                    + "    small_un_c           SMALLINT UNSIGNED,\n"
                                    + "    small_un_z_c         SMALLINT UNSIGNED ZEROFILL,\n"
                                    + "    medium_c             MEDIUMINT,\n"
                                    + "    medium_un_c          MEDIUMINT UNSIGNED,\n"
                                    + "    medium_un_z_c        MEDIUMINT UNSIGNED ZEROFILL,\n"
                                    + "    int_c                INTEGER,\n"
                                    + "    int_un_c             INTEGER UNSIGNED,\n"
                                    + "    int_un_z_c           INTEGER UNSIGNED ZEROFILL,\n"
                                    + "    int11_c              INT(11),\n"
                                    + "    big_c                BIGINT,\n"
                                    + "    big_un_c             BIGINT UNSIGNED,\n"
                                    + "    big_un_z_c           BIGINT UNSIGNED ZEROFILL,\n"
                                    + "    varchar_c            VARCHAR(255),\n"
                                    + "    char_c               CHAR(3),\n"
                                    + "    real_c               REAL,\n"
                                    + "    float_c              FLOAT,\n"
                                    + "    float_un_c           FLOAT UNSIGNED,\n"
                                    + "    float_un_z_c         FLOAT UNSIGNED ZEROFILL,\n"
                                    + "    double_c             DOUBLE,\n"
                                    + "    double_un_c          DOUBLE UNSIGNED,\n"
                                    + "    double_un_z_c        DOUBLE UNSIGNED ZEROFILL,\n"
                                    + "    decimal_c            DECIMAL(8, 4),\n"
                                    + "    decimal_un_c         DECIMAL(8, 4) UNSIGNED,\n"
                                    + "    decimal_un_z_c       DECIMAL(8, 4) UNSIGNED ZEROFILL,\n"
                                    + "    numeric_c            NUMERIC(6, 0),\n"
                                    + "    big_decimal_c        DECIMAL(65, 1),\n"
                                    + "    bit1_c               BIT,\n"
                                    + "    bit3_c               BIT(3),\n"
                                    + "    tiny1_c              TINYINT(1),\n"
                                    + "    boolean_c            BOOLEAN,\n"
                                    + "    file_uuid            BINARY(16),\n"
                                    + "    bit_c                BIT(64),\n"
                                    + "    text_c               TEXT,\n"
                                    + "    tiny_blob_c          TINYBLOB,\n"
                                    + "    blob_c               BLOB,\n"
                                    + "    medium_blob_c        MEDIUMBLOB,\n"
                                    + "    long_blob_c          LONGBLOB,\n"
                                    + "    enum_c               enum('red', 'white'),\n"
                                    + "    json_c               JSON,\n"
                                    + "    point_c              POINT,\n"
                                    + "    geometry_c           GEOMETRY,\n"
                                    + "    linestring_c         LINESTRING,\n"
                                    + "    polygon_c            POLYGON,\n"
                                    + "    multipoint_c         MULTIPOINT,\n"
                                    + "    multiline_c          MULTILINESTRING,\n"
                                    + "    multipolygon_c       MULTIPOLYGON,\n"
                                    + "    geometrycollection_c GEOMETRYCOLLECTION,"
                                    + "    year_c               YEAR,\n"
                                    + "    date_c               DATE,\n"
                                    + "    time_c               TIME(0),\n"
                                    + "    time_3_c             TIME(3),\n"
                                    + "    time_6_c             TIME(6),\n"
                                    + "    datetime_c           DATETIME(0),\n"
                                    + "    datetime3_c          DATETIME(3),\n"
                                    + "    datetime6_c          DATETIME(6),\n"
                                    + "    decimal_c0           DECIMAL(6, 2),\n"
                                    + "    decimal_c1           DECIMAL(9, 4),\n"
                                    + "    decimal_c2           DECIMAL(20, 4),\n"
                                    + "    timestamp_c          TIMESTAMP(0) NULL,\n"
                                    + "    timestamp3_c         TIMESTAMP(3) NULL,\n"
                                    + "    timestamp6_c         TIMESTAMP(6) NULL,"
                                    + "primary key(id));",
                            inventoryDatabase.getDatabaseName()));

            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable1"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                                    .physicalColumn("tiny_c", DataTypes.TINYINT())
                                    .physicalColumn("tiny_un_c", DataTypes.SMALLINT())
                                    .physicalColumn("tiny_un_z_c", DataTypes.SMALLINT())
                                    .physicalColumn("small_c", DataTypes.SMALLINT())
                                    .physicalColumn("small_un_c", DataTypes.INT())
                                    .physicalColumn("small_un_z_c", DataTypes.INT())
                                    .physicalColumn("medium_c", DataTypes.INT())
                                    .physicalColumn("medium_un_c", DataTypes.INT())
                                    .physicalColumn("medium_un_z_c", DataTypes.INT())
                                    .physicalColumn("int_c", DataTypes.INT())
                                    .physicalColumn("int_un_c", DataTypes.BIGINT())
                                    .physicalColumn("int_un_z_c", DataTypes.BIGINT())
                                    .physicalColumn("int11_c", DataTypes.INT())
                                    .physicalColumn("big_c", DataTypes.BIGINT())
                                    .physicalColumn("big_un_c", DataTypes.DECIMAL(20, 0))
                                    .physicalColumn("big_un_z_c", DataTypes.DECIMAL(20, 0))
                                    .physicalColumn("varchar_c", DataTypes.VARCHAR(255))
                                    .physicalColumn("char_c", DataTypes.CHAR(3))
                                    .physicalColumn("real_c", DataTypes.DOUBLE())
                                    .physicalColumn("float_c", DataTypes.FLOAT())
                                    .physicalColumn("float_un_c", DataTypes.FLOAT())
                                    .physicalColumn("float_un_z_c", DataTypes.FLOAT())
                                    .physicalColumn("double_c", DataTypes.DOUBLE())
                                    .physicalColumn("double_un_c", DataTypes.DOUBLE())
                                    .physicalColumn("double_un_z_c", DataTypes.DOUBLE())
                                    .physicalColumn("decimal_c", DataTypes.DECIMAL(8, 4))
                                    .physicalColumn("decimal_un_c", DataTypes.DECIMAL(8, 4))
                                    .physicalColumn("decimal_un_z_c", DataTypes.DECIMAL(8, 4))
                                    .physicalColumn("numeric_c", DataTypes.DECIMAL(6, 0))
                                    .physicalColumn("big_decimal_c", DataTypes.STRING())
                                    .physicalColumn("bit1_c", DataTypes.BOOLEAN())
                                    .physicalColumn("bit3_c", DataTypes.BINARY(1))
                                    .physicalColumn(
                                            "tiny1_c",
                                            tinyInt1isBit
                                                    ? DataTypes.BOOLEAN()
                                                    : DataTypes.TINYINT())
                                    .physicalColumn("boolean_c", DataTypes.BOOLEAN())
                                    .physicalColumn("file_uuid", DataTypes.BINARY(16))
                                    .physicalColumn("bit_c", DataTypes.BINARY(8))
                                    .physicalColumn("text_c", DataTypes.STRING())
                                    .physicalColumn("tiny_blob_c", DataTypes.BYTES())
                                    .physicalColumn("blob_c", DataTypes.BYTES())
                                    .physicalColumn("medium_blob_c", DataTypes.BYTES())
                                    .physicalColumn("long_blob_c", DataTypes.BYTES())
                                    .physicalColumn("enum_c", DataTypes.STRING())
                                    .physicalColumn("json_c", DataTypes.STRING())
                                    .physicalColumn("point_c", DataTypes.STRING())
                                    .physicalColumn("geometry_c", DataTypes.STRING())
                                    .physicalColumn("linestring_c", DataTypes.STRING())
                                    .physicalColumn("polygon_c", DataTypes.STRING())
                                    .physicalColumn("multipoint_c", DataTypes.STRING())
                                    .physicalColumn("multiline_c", DataTypes.STRING())
                                    .physicalColumn("multipolygon_c", DataTypes.STRING())
                                    .physicalColumn("geometrycollection_c", DataTypes.STRING())
                                    .physicalColumn("year_c", DataTypes.INT())
                                    .physicalColumn("date_c", DataTypes.DATE())
                                    .physicalColumn("time_c", DataTypes.TIME(0))
                                    .physicalColumn("time_3_c", DataTypes.TIME(3))
                                    .physicalColumn("time_6_c", DataTypes.TIME(6))
                                    .physicalColumn("datetime_c", DataTypes.TIMESTAMP(0))
                                    .physicalColumn("datetime3_c", DataTypes.TIMESTAMP(3))
                                    .physicalColumn("datetime6_c", DataTypes.TIMESTAMP(6))
                                    .physicalColumn("decimal_c0", DataTypes.DECIMAL(6, 2))
                                    .physicalColumn("decimal_c1", DataTypes.DECIMAL(9, 4))
                                    .physicalColumn("decimal_c2", DataTypes.DECIMAL(20, 4))
                                    .physicalColumn("timestamp_c", DataTypes.TIMESTAMP_LTZ(0))
                                    .physicalColumn("timestamp3_c", DataTypes.TIMESTAMP_LTZ(3))
                                    .physicalColumn("timestamp6_c", DataTypes.TIMESTAMP_LTZ(6))
                                    .primaryKey("id")
                                    .build()));

            // Test create table DDL with inline primary key
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable2`(id SERIAL PRIMARY KEY);",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable2"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                                    .primaryKey("id")
                                    .build()));

            // Test create table DDL with multiple primary keys
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable3`("
                                    + "id SERIAL,"
                                    + "name VARCHAR(17),"
                                    + "notes TEXT,"
                                    + "PRIMARY KEY (id, name));",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable3"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                                    .physicalColumn("name", DataTypes.VARCHAR(17).notNull())
                                    .physicalColumn("notes", DataTypes.STRING())
                                    .primaryKey("id", "name")
                                    .build()));

            // Test create table DDL with like syntax
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable4` LIKE `%s`.`newlyAddedTable3`",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable4"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                                    .physicalColumn("name", DataTypes.VARCHAR(17).notNull())
                                    .physicalColumn("notes", DataTypes.STRING())
                                    .primaryKey("id", "name")
                                    .build()));

            // Test create table DDL with as syntax, Primary key information will not be retained.
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable5` AS SELECT * FROM `%s`.`newlyAddedTable3`",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable5"),
                            Schema.newBuilder()
                                    .physicalColumn(
                                            "id", DataTypes.DECIMAL(20, 0).notNull(), null, "0")
                                    .physicalColumn("name", DataTypes.VARCHAR(17).notNull())
                                    .physicalColumn("notes", DataTypes.STRING())
                                    .build()));

            // Database and table that does not match the filter of regular expression.
            statement.execute(
                    String.format(
                            "CREATE DATABASE `%s_copy`", inventoryDatabase.getDatabaseName()));
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s_copy`.`newlyAddedTable`("
                                    + "id SERIAL,"
                                    + "name VARCHAR(17),"
                                    + "notes TEXT,"
                                    + "PRIMARY KEY (id));",
                            inventoryDatabase.getDatabaseName()));

            // This should be ignored as another_database is not included in the captured regular
            // expression.
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable6` LIKE `%s_copy`.`newlyAddedTable`",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName()));

            // This should not be ignored as MySQL will build and emit a new sql like:
            // CREATE TABLE `newlyAddedTable7` (
            //  `id` bigint unsigned NOT NULL DEFAULT '0',
            //  `name` varchar(17) DEFAULT NULL,
            //  `notes` text
            // ) START TRANSACTION.
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`newlyAddedTable7` AS SELECT * FROM `%s_copy`.`newlyAddedTable`",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName()));
            // Primary key information will not be retained.
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable7"),
                            Schema.newBuilder()
                                    .physicalColumn(
                                            "id", DataTypes.DECIMAL(20, 0).notNull(), null, "0")
                                    .physicalColumn("name", DataTypes.VARCHAR(17))
                                    .physicalColumn("notes", DataTypes.STRING())
                                    .build()));

            // The CreateTableEvent is not correctly emitted.
            statement.execute(
                    String.format(
                            "INSERT `%s`.`newlyAddedTable6` VALUES(1, 'Mark', 'eu')",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new CreateTableEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable6"),
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull(), null)
                                    .physicalColumn("name", DataTypes.VARCHAR(17))
                                    .physicalColumn("notes", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build()));
            expected.add(
                    DataChangeEvent.insertEvent(
                            TableId.tableId(
                                    inventoryDatabase.getDatabaseName(), "newlyAddedTable6"),
                            new BinaryRecordDataGenerator(
                                            new DataType[] {
                                                DataTypes.DECIMAL(20, 0),
                                                DataTypes.VARCHAR(17),
                                                DataTypes.STRING()
                                            })
                                    .generate(
                                            new Object[] {
                                                DecimalData.fromBigDecimal(BigDecimal.ONE, 20, 0),
                                                new BinaryStringData("Mark"),
                                                new BinaryStringData("eu")
                                            })));
        }
        List<Event> actual = fetchResults(events, expected.size());
        assertEqualsInAnyOrder(
                expected.stream().map(Object::toString).collect(Collectors.toList()),
                actual.stream().map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    void testDanglingDropTableEventInBinlog() throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();

        // Create a new table for later deletion
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE live_fast(ID INT PRIMARY KEY);");
        }

        String logFileName = null;
        Long logPosition = null;

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW BINARY LOGS;");
            while (rs.next()) {
                logFileName = rs.getString("Log_name");
                logPosition = rs.getLong("File_size");
            }
        }

        // We start reading binlog from the tail of current position and file to avoid reading
        // previous events. The next DDL event (DROP TABLE) will push binlog position forward.
        Preconditions.checkNotNull(logFileName, "Log file name must not be null");
        Preconditions.checkNotNull(logPosition, "Log position name must not be null");
        LOG.info("Trying to restore from {} @ {}...", logFileName, logPosition);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE live_fast;");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".*")
                        .startupOptions(StartupOptions.specificOffset(logFileName, logPosition))
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

        List<Event> expectedEvents = new ArrayList<>();

        expectedEvents.add(
                new DropTableEvent(
                        TableId.tableId(inventoryDatabase.getDatabaseName(), "live_fast")));

        List<Event> actual = fetchResults(events, expectedEvents.size());
        assertEqualsInAnyOrder(
                expectedEvents.stream().map(Object::toString).collect(Collectors.toList()),
                actual.stream().map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testIncludeComments() throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        TableId tableId =
                TableId.tableId(inventoryDatabase.getDatabaseName(), "products_with_comments");

        String createTableSql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n"
                                + "  id INTEGER NOT NULL AUTO_INCREMENT COMMENT 'column comment of id' PRIMARY KEY,\n"
                                + "  name VARCHAR(255) NOT NULL DEFAULT 'flink' COMMENT 'column comment of name',\n"
                                + "  weight FLOAT(6) COMMENT 'column comment of weight'\n"
                                + ")\n"
                                + "COMMENT 'table comment of products';",
                        inventoryDatabase.getDatabaseName(), "products_with_comments");
        executeSql(inventoryDatabase, createTableSql);

        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL8_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL8_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(INCLUDE_COMMENTS_ENABLED.key(), "true");
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".products_with_comments");
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        Configuration.fromMap(options), null, this.getClass().getClassLoader());

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        // add some column
        String addColumnSql =
                String.format(
                        "ALTER TABLE `%s`.`products_with_comments` ADD COLUMN `description` VARCHAR(512) comment 'column comment of description';",
                        inventoryDatabase.getDatabaseName());
        executeSql(inventoryDatabase, addColumnSql);

        List<Event> expectedEvents = getEventsWithComments(tableId);
        List<Event> actual = fetchResults(events, expectedEvents.size());
        assertEqualsInAnyOrder(
                expectedEvents.stream().map(Object::toString).collect(Collectors.toList()),
                actual.stream().map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testIncludeCommentsForScanBinlogNewlyAddedTableEnabled() throws Exception {
        env.setParallelism(1);
        inventoryDatabase.createAndInitialize();
        TableId tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "products");
        TableId newTableId =
                TableId.tableId(inventoryDatabase.getDatabaseName(), "products_with_comments2");

        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL8_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL8_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(INCLUDE_COMMENTS_ENABLED.key(), "true");
        options.put(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".products\\.*");
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        Configuration.fromMap(options), null, this.getClass().getClassLoader());

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        String createTableSql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n"
                                + "  id INTEGER NOT NULL AUTO_INCREMENT COMMENT 'column comment of id' PRIMARY KEY,\n"
                                + "  name VARCHAR(255) NOT NULL DEFAULT 'flink' COMMENT 'column comment of name',\n"
                                + "  weight FLOAT COMMENT 'column comment of weight'\n"
                                + ")\n"
                                + "COMMENT 'table comment of products';",
                        inventoryDatabase.getDatabaseName(), "products_with_comments2");
        executeSql(inventoryDatabase, createTableSql);

        // add some column
        String addColumnSql =
                String.format(
                        "ALTER TABLE `%s`.`products_with_comments2` ADD COLUMN `description` VARCHAR(512) comment 'column comment of description';",
                        inventoryDatabase.getDatabaseName());
        executeSql(inventoryDatabase, addColumnSql);

        List<Event> expectedEvents = new ArrayList<>();
        CreateTableEvent productCreateTableEvent = getProductsCreateTableEvent(tableId);
        expectedEvents.add(productCreateTableEvent);
        // generate snapshot data
        List<Event> productExpectedSnapshot = getSnapshotExpected(tableId);
        expectedEvents.addAll(productExpectedSnapshot);

        List<Event> newTableExpectedEvents = getEventsWithComments(newTableId);
        expectedEvents.addAll(newTableExpectedEvents);

        List<Event> actual = fetchResults(events, expectedEvents.size());
        assertEqualsInAnyOrder(
                expectedEvents.stream().map(Object::toString).collect(Collectors.toList()),
                actual.stream().map(Object::toString).collect(Collectors.toList()));
    }

    private void executeSql(UniqueDatabase database, String sql) throws SQLException {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<Event> getEventsWithComments(TableId tableId) {
        return Arrays.asList(
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT().notNull(), "column comment of id")
                                .physicalColumn(
                                        "name",
                                        DataTypes.VARCHAR(255).notNull(),
                                        "column comment of name",
                                        "flink")
                                .physicalColumn(
                                        "weight", DataTypes.FLOAT(), "column comment of weight")
                                .primaryKey(Collections.singletonList("id"))
                                .comment("table comment of products")
                                .build()),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "description",
                                                DataTypes.VARCHAR(512),
                                                "column comment of description")))));
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.FLOAT())
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.FLOAT()
                        },
                        new String[] {"id", "name", "description", "weight"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    3.14f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    8.1f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    0.8f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenter's hammer"),
                                    0.75f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenter's hammer"),
                                    0.875f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenter's hammer"),
                                    1.0f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    5.3f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    0.1f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    22.2f
                                })));
        return snapshotExpected;
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
    private List<Event> executeAlterAndProvideExpected(TableId tableId, Statement statement)
            throws SQLException {
        List<Event> expected = new ArrayList<>();
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` CHANGE COLUMN `description` `desc` VARCHAR(255) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("description", DataTypes.VARCHAR(255))));
        expected.add(
                new RenameColumnEvent(tableId, Collections.singletonMap("description", "desc")));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` CHANGE COLUMN `desc` `desc2` VARCHAR(400) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("desc", DataTypes.VARCHAR(400))));
        expected.add(new RenameColumnEvent(tableId, Collections.singletonMap("desc", "desc2")));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` ADD COLUMN `desc1` VARCHAR(45) NULL AFTER `weight`;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("desc1", DataTypes.VARCHAR(45)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "weight"))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` ADD COLUMN `col1` VARCHAR(45) NULL AFTER `weight`, ADD COLUMN `col2` VARCHAR(55) NULL AFTER `desc1`;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col1", DataTypes.VARCHAR(45)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "weight"))));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col2", DataTypes.VARCHAR(55)),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "desc1"))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` DROP COLUMN `desc2`, CHANGE COLUMN `desc1` `desc1` VARCHAR(65) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("desc2")));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("desc1", DataTypes.VARCHAR(65))));

        // Only available in mysql 8.0
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` RENAME COLUMN `desc1` TO `desc3`;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(new RenameColumnEvent(tableId, Collections.singletonMap("desc1", "desc3")));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` MODIFY COLUMN `DESC3` VARCHAR(255) NULL DEFAULT NULL;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESC3", DataTypes.VARCHAR(255))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` DROP COLUMN `DESC3`;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("DESC3")));

        // Should not catch SchemaChangeEvent of tables other than `products`
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`orders` ADD COLUMN `desc1` VARCHAR(45) NULL;",
                        inventoryDatabase.getDatabaseName()));
        return expected;
    }
}
