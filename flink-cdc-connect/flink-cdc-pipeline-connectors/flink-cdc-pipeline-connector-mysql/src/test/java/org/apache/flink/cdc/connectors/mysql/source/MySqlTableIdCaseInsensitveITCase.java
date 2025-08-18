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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    public void testParseAlterStatementWhenTableNameAndColumnIsUpper() throws Exception {
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
                            "ALTER TABLE `%s`.`PRODUCTS` ADD `cols1` VARCHAR(45);",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "cols1", DataTypes.VARCHAR(45))))));
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
                        "ALTER TABLE `%s`.`products` CHANGE COLUMN `DESCRIPTION` `DESC` VARCHAR(255) NULL DEFAULT NULL;",
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
                        "ALTER TABLE `%s`.`products` ADD COLUMN `DESC1` VARCHAR(45) NULL AFTER `weight`;",
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
                        "ALTER TABLE `%s`.`products` ADD COLUMN `col1` VARCHAR(45) NULL AFTER `weight`, ADD COLUMN `COL2` VARCHAR(55) NULL AFTER `desc1`;",
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
                        tableId, Collections.singletonMap("desc3", DataTypes.VARCHAR(255))));

        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`products` DROP COLUMN `desc3`;",
                        inventoryDatabase.getDatabaseName()));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("desc3")));

        // Should not catch SchemaChangeEvent of tables other than `products`
        statement.execute(
                String.format(
                        "ALTER TABLE `%s`.`orders` ADD COLUMN `desc1` VARCHAR(45) NULL;",
                        inventoryDatabase.getDatabaseName()));
        return expected;
    }
}
