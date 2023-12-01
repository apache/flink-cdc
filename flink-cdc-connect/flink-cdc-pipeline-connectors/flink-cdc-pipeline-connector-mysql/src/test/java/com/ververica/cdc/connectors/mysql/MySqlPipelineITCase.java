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

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import com.ververica.cdc.connectors.mysql.source.MySqlDataSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.MySqlVersion;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests. */
public class MySqlPipelineITCase extends MySqlSourceTestBase {

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

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
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testParseAlterStatement() throws Exception {
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
                        .serverId(getServerId())
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(true);

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
        expected.add(
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.VARCHAR(255).notNull())
                                .physicalColumn("description", DataTypes.VARCHAR(512))
                                .physicalColumn("weight", DataTypes.FLOAT())
                                .primaryKey(Arrays.asList("id"))
                                .build()));
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` CHANGE COLUMN `description` `desc` VARCHAR(255) NULL DEFAULT NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AlterColumnTypeEvent(
                            tableId,
                            new HashMap<String, DataType>() {
                                {
                                    put("description", DataTypes.VARCHAR(255));
                                }
                            }));
            expected.add(
                    new RenameColumnEvent(
                            tableId,
                            new HashMap<String, String>() {
                                {
                                    put("description", "desc");
                                }
                            }));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` CHANGE COLUMN `desc` `desc2` VARCHAR(400) NULL DEFAULT NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AlterColumnTypeEvent(
                            tableId,
                            new HashMap<String, DataType>() {
                                {
                                    put("desc", DataTypes.VARCHAR(400));
                                }
                            }));
            expected.add(
                    new RenameColumnEvent(
                            tableId,
                            new HashMap<String, String>() {
                                {
                                    put("desc", "desc2");
                                }
                            }));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `desc1` VARCHAR(45) NULL AFTER `weight`;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("desc1", DataTypes.VARCHAR(45)),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            Column.physicalColumn("weight", DataTypes.BIGINT())))));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `col1` VARCHAR(45) NULL AFTER `weight`, ADD COLUMN `col2` VARCHAR(55) NULL AFTER `desc1`;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("col1", DataTypes.VARCHAR(45)),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            Column.physicalColumn("weight", DataTypes.BIGINT())))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("col2", DataTypes.VARCHAR(55)),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            Column.physicalColumn("desc1", DataTypes.BIGINT())))));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` DROP COLUMN `desc2`, CHANGE COLUMN `desc1` `desc1` VARCHAR(65) NULL DEFAULT NULL;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new DropColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    Column.physicalColumn("desc2", DataTypes.BIGINT()))));
            expected.add(
                    new AlterColumnTypeEvent(
                            tableId,
                            new HashMap<String, DataType>() {
                                {
                                    put("desc1", DataTypes.VARCHAR(65));
                                }
                            }));

            // Only available in mysql 8.0
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` RENAME COLUMN `desc1` TO `desc3`;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new RenameColumnEvent(
                            tableId,
                            new HashMap<String, String>() {
                                {
                                    put("desc1", "desc3");
                                }
                            }));

            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` DROP COLUMN `DESC3`;",
                            inventoryDatabase.getDatabaseName()));
            expected.add(
                    new DropColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    Column.physicalColumn("DESC3", DataTypes.BIGINT()))));
        }
        List<Event> actual = fetchRowData(events, expected.size());
        assertThat(actual).isEqualTo(expected);
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }

    private static List<Event> fetchRowData(Iterator<Event> iter, int size) {
        List<Event> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Event event = iter.next();
            rows.add(event);
            size--;
        }
        return rows;
    }
}
