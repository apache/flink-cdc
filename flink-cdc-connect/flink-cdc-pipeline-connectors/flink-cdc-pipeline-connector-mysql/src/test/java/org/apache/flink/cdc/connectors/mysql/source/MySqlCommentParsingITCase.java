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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT tests for MySQL DDL parsing with different quote types in COMMENT clauses.
 *
 * <p>This test verifies that the MySQL CDC connector can correctly parse DDL statements containing
 * both single quotes and double quotes in COMMENT clauses, which is important for handling binlog
 * events that may use either quote type.
 */
class MySqlCommentParsingITCase extends MySqlSourceTestBase {

    protected static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase testDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "comment_test", TEST_USER, TEST_PASSWORD);

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
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    void testColumnCommentWithDoubleQuotes() throws Exception {
        testDatabase.createAndInitialize();

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create table with column comments using double quotes (simulating binlog DDL)
            statement.execute(
                    "CREATE TABLE test_double_quotes ("
                            + "  id INT NOT NULL PRIMARY KEY,"
                            + "  ac_code INT DEFAULT \"0\" COMMENT \"This is a comment with double quotes\","
                            + "  name VARCHAR(255) COMMENT \"User name field\""
                            + ") ENGINE=InnoDB");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + "\\.test_double_quotes")
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
                                "MySQL Source",
                                new EventTypeInfo())
                        .executeAndCollect();

        List<Event> eventList = new ArrayList<>();
        while (events.hasNext() && eventList.size() < 1) {
            Event event = events.next();
            if (event instanceof SchemaChangeEvent) {
                eventList.add(event);
            }
        }
        events.close();

        assertThat(eventList).hasSize(1);
        CreateTableEvent createTableEvent = (CreateTableEvent) eventList.get(0);
        Schema schema = createTableEvent.getSchema();

        // Verify that comments are correctly parsed (MySQL normalizes double quotes to single
        // quotes)
        Column acCodeColumn = schema.getColumn("ac_code").orElse(null);
        assertThat(acCodeColumn).isNotNull();
        assertThat(acCodeColumn.getComment()).isEqualTo("This is a comment with double quotes");

        Column nameColumn = schema.getColumn("name").orElse(null);
        assertThat(nameColumn).isNotNull();
        assertThat(nameColumn.getComment()).isEqualTo("User name field");
    }

    @Test
    void testColumnCommentWithSingleQuotes() throws Exception {
        testDatabase.createAndInitialize();

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create table with column comments using single quotes (standard)
            statement.execute(
                    "CREATE TABLE test_single_quotes ("
                            + "  id INT NOT NULL PRIMARY KEY,"
                            + "  code INT DEFAULT '0' COMMENT 'This is a comment with single quotes',"
                            + "  description VARCHAR(255) COMMENT 'Description field'"
                            + ") ENGINE=InnoDB");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + "\\.test_single_quotes")
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
                                "MySQL Source",
                                new EventTypeInfo())
                        .executeAndCollect();

        List<Event> eventList = new ArrayList<>();
        while (events.hasNext() && eventList.size() < 1) {
            Event event = events.next();
            if (event instanceof SchemaChangeEvent) {
                eventList.add(event);
            }
        }
        events.close();

        assertThat(eventList).hasSize(1);
        CreateTableEvent createTableEvent = (CreateTableEvent) eventList.get(0);
        Schema schema = createTableEvent.getSchema();

        // Verify that comments are correctly parsed
        Column codeColumn = schema.getColumn("code").orElse(null);
        assertThat(codeColumn).isNotNull();
        assertThat(codeColumn.getComment()).isEqualTo("This is a comment with single quotes");

        Column descColumn = schema.getColumn("description").orElse(null);
        assertThat(descColumn).isNotNull();
        assertThat(descColumn.getComment()).isEqualTo("Description field");
    }

    @Test
    void testTableCommentWithDoubleQuotes() throws Exception {
        testDatabase.createAndInitialize();

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create table with table comment using double quotes
            statement.execute(
                    "CREATE TABLE test_table_comment ("
                            + "  id INT NOT NULL PRIMARY KEY,"
                            + "  data VARCHAR(255)"
                            + ") ENGINE=InnoDB COMMENT \"Table level comment with double quotes\"");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + "\\.test_table_comment")
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
                                "MySQL Source",
                                new EventTypeInfo())
                        .executeAndCollect();

        List<Event> eventList = new ArrayList<>();
        while (events.hasNext() && eventList.size() < 1) {
            Event event = events.next();
            if (event instanceof SchemaChangeEvent) {
                eventList.add(event);
            }
        }
        events.close();

        assertThat(eventList).hasSize(1);
        CreateTableEvent createTableEvent = (CreateTableEvent) eventList.get(0);
        Schema schema = createTableEvent.getSchema();

        // Verify that table comment is correctly parsed
        assertThat(schema.comment()).isEqualTo("Table level comment with double quotes");
    }

    @Test
    void testAlterTableAddColumnWithDoubleQuoteComment() throws Exception {
        testDatabase.createAndInitialize();

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create initial table
            statement.execute(
                    "CREATE TABLE test_alter ("
                            + "  id INT NOT NULL PRIMARY KEY"
                            + ") ENGINE=InnoDB");
        }

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + "\\.test_alter")
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
                                "MySQL Source",
                                new EventTypeInfo())
                        .executeAndCollect();

        // Wait for CreateTableEvent
        List<Event> eventList = new ArrayList<>();
        while (events.hasNext() && eventList.size() < 1) {
            Event event = events.next();
            if (event instanceof SchemaChangeEvent) {
                eventList.add(event);
            }
        }

        // Now alter the table to add a column with double quote comment
        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "ALTER TABLE test_alter ADD COLUMN new_col VARCHAR(100) COMMENT \"New column comment\"");
        }

        // Wait for AddColumnEvent
        while (events.hasNext() && eventList.size() < 2) {
            Event event = events.next();
            if (event instanceof SchemaChangeEvent) {
                eventList.add(event);
            }
        }
        events.close();

        assertThat(eventList).hasSizeGreaterThanOrEqualTo(2);
        AddColumnEvent addColumnEvent = (AddColumnEvent) eventList.get(1);

        // Verify that the new column's comment is correctly parsed
        Column newColumn = addColumnEvent.getAddedColumns().get(0).getAddColumn();
        assertThat(newColumn.getName()).isEqualTo("new_col");
        assertThat(newColumn.getComment()).isEqualTo("New column comment");
    }
}
