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
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
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
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.getServerId;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link MySqlDataSource}. */
public class MySqlPipelineITCase extends MySqlSourceTestBase {

    protected static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testInitialStartupMode() throws Exception {
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
        List<Event> actual =
                fetchResults(events, 1 + expectedSnapshot.size() + expectedBinlog.size());
        assertThat(actual.get(0)).isEqualTo(createTableEvent);
        assertThat(actual.subList(1, 10))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(actual.subList(10, actual.size())).isEqualTo(expectedBinlog);
    }

    @Test
    public void testParseAlterStatement() throws Exception {
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
        }
        List<Event> actual = fetchResults(events, expected.size());
        assertThat(actual).isEqualTo(expected);
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
