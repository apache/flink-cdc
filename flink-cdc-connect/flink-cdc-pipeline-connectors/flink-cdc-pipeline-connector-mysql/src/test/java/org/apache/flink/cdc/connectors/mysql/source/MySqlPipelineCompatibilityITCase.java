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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests to check MySQL pipeline connector works well with different MySQL server
 * versions.
 */
@ParameterizedClass
@EnumSource(
        value = MySqlVersion.class,
        names = {"V5_7", "V8_0", "V8_4"})
class MySqlPipelineCompatibilityITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlPipelineCompatibilityITCase.class);

    private static Path tempFolder;
    private static File resourceFolder;

    private final MySqlVersion version;
    private final MySqlContainer mySqlContainer;
    private final UniqueDatabase testDatabase;

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    MySqlPipelineCompatibilityITCase(MySqlVersion version) {
        this.version = version;
        this.mySqlContainer =
                (MySqlContainer)
                        new MySqlContainer(version)
                                .withConfigurationOverride(buildCustomMySqlConfig(version))
                                .withSetupSQL("docker/setup.sql")
                                .withDatabaseName("flink-test")
                                .withUsername("flinkuser")
                                .withPassword("flinkpw")
                                .withLogConsumer(new Slf4jLogConsumer(LOG));
        this.testDatabase =
                new UniqueDatabase(mySqlContainer, "inventory", TEST_USER, TEST_PASSWORD);
    }

    @BeforeEach
    void setup() throws Exception {
        // Initialize static resources if needed
        if (resourceFolder == null) {
            resourceFolder =
                    Paths.get(
                                    Objects.requireNonNull(
                                                    MySqlPipelineCompatibilityITCase.class
                                                            .getClassLoader()
                                                            .getResource("."))
                                            .toURI())
                            .toFile();
            tempFolder = Files.createTempDirectory(resourceFolder.toPath(), "mysql-config");
        }

        env.setParallelism(4);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());

        LOG.info("Starting container for MySQL {}...", version.getVersion());
        Startables.deepStart(Stream.of(mySqlContainer)).join();
        LOG.info("Container is started.");

        testDatabase.createAndInitialize();
    }

    @AfterEach
    void tearDown() {
        testDatabase.dropDatabase();
        if (mySqlContainer != null) {
            LOG.info("Stopping container for MySQL {}...", version.getVersion());
            mySqlContainer.stop();
            LOG.info("Container is stopped.");
        }
    }

    @Test
    void testSnapshotRead() throws Exception {
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(mySqlContainer.getHost())
                        .port(mySqlContainer.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + ".products")
                        .startupOptions(StartupOptions.initial())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        List<Event> snapshotEvents = fetchEvents(events, 9);

        assertThat(snapshotEvents).hasSize(9);
        assertThat(snapshotEvents.stream().filter(e -> e instanceof DataChangeEvent)).hasSize(9);

        events.close();
    }

    @Test
    void testBinlogRead() throws Exception {
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(mySqlContainer.getHost())
                        .port(mySqlContainer.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + ".products")
                        .startupOptions(StartupOptions.initial())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        fetchEvents(events, 9);

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`products` VALUES (default,'test_product','desc',1.0);",
                            testDatabase.getDatabaseName()));
        }

        List<Event> binlogEvents = fetchEvents(events, 1);
        assertThat(binlogEvents).hasSize(1);
        assertThat(binlogEvents.get(0)).isInstanceOf(DataChangeEvent.class);

        events.close();
    }

    private String getServerId(int parallelism) {
        int serverId = (int) (Math.random() * 100) + 5400;
        return serverId + "-" + (serverId + parallelism);
    }

    private List<Event> fetchEvents(CloseableIterator<Event> iterator, int count) {
        List<Event> events = new ArrayList<>();
        while (count > 0 && iterator.hasNext()) {
            Event event = iterator.next();
            if (event instanceof DataChangeEvent) {
                events.add(event);
                count--;
            }
        }
        return events;
    }

    private String buildCustomMySqlConfig(MySqlVersion version) {
        try {
            if (resourceFolder == null) {
                resourceFolder =
                        Paths.get(
                                        Objects.requireNonNull(
                                                        MySqlPipelineCompatibilityITCase.class
                                                                .getClassLoader()
                                                                .getResource("."))
                                                .toURI())
                                .toFile();
                tempFolder = Files.createTempDirectory(resourceFolder.toPath(), "mysql-config");
            }
            // Create version-specific directory to avoid conflicts
            Path versionDir =
                    Files.createDirectories(
                            Paths.get(
                                    tempFolder.toString(), version.getVersion().replace(".", "_")));
            Path cnf = Paths.get(versionDir.toString(), "my.cnf");
            // Check if file already exists to avoid FileAlreadyExistsException
            if (!Files.exists(cnf)) {
                Files.createFile(cnf);
            }
            StringBuilder mysqlConfBuilder = new StringBuilder();
            mysqlConfBuilder.append(
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n"
                            + "gtid-mode = OFF\n");

            if (version == MySqlVersion.V8_0 || version == MySqlVersion.V8_4) {
                mysqlConfBuilder.append("secure_file_priv=/var/lib/mysql\n");
            }

            Files.write(
                    cnf,
                    Collections.singleton(mysqlConfBuilder.toString()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.TRUNCATE_EXISTING);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
