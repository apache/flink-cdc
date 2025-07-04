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

package org.apache.flink.cdc.connectors.mysql;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.table.api.ValidationException;

import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.MySqlTestUtils.basicSourceBuilder;
import static org.apache.flink.cdc.connectors.mysql.MySqlTestUtils.setupSource;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion.V5_5;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion.V5_7;

/** Test for the {@link MySqlValidator}. */
public class MySqlValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlValidatorTest.class);

    private Path tempFolder;
    private static File resourceFolder;

    @BeforeAll
    public static void setup() throws Exception {
        resourceFolder =
                Paths.get(
                                Objects.requireNonNull(
                                                MySqlValidatorTest.class
                                                        .getClassLoader()
                                                        .getResource("."))
                                        .toURI())
                        .toFile();
    }

    @BeforeEach
    public void prepareTempFolder() throws IOException {
        tempFolder = Files.createTempDirectory(resourceFolder.toPath(), "mysql-config");
    }

    @Disabled("The jdbc driver used in this module cannot connect to MySQL 5.5")
    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testValidateVersion(boolean incrementalSnapshot) {
        MySqlVersion version = V5_5;
        String message =
                String.format(
                        "Currently Flink MySql CDC connector only supports MySql whose version is larger or equal to 5.6, but actual is %s.",
                        version);
        doValidate(version, "docker/server/my.cnf", message, incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testValidateBinlogFormat(boolean incrementalSnapshot) {
        String mode = "STATEMENT";
        String message =
                String.format(
                        "The MySQL server is configured with binlog_format %s rather than ROW, which is required for this "
                                + "connector to work properly. Change the MySQL configuration to use a binlog_format=ROW "
                                + "and restart the connector.",
                        mode);
        doValidate(
                V5_7,
                buildMySqlConfigFile("[mysqld]\nbinlog_format = " + mode),
                message,
                incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testValidateBinlogRowImage(boolean incrementalSnapshot) {
        String mode = "MINIMAL";
        String message =
                String.format(
                        "The MySQL server is configured with binlog_row_image %s rather than FULL, which is "
                                + "required for this connector to work properly. Change the MySQL configuration to use a "
                                + "binlog_row_image=FULL and restart the connector.",
                        mode);
        doValidate(
                V5_7,
                buildMySqlConfigFile("[mysqld]\nbinlog_format = ROW\nbinlog_row_image = " + mode),
                message,
                incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testValidateTimezone(boolean incrementalSnapshot) {
        String message =
                String.format(
                        "The MySQL server has a timezone offset (%d seconds ahead of UTC) which does not match "
                                + "the configured timezone %s. Specify the right %s to avoid inconsistencies "
                                + "for time-related fields.",
                        45240, // +12:34 is 45240 seconds ahead of UTC
                        "UTC",
                        SERVER_TIME_ZONE.key());
        doValidate(
                V5_7,
                buildMySqlConfigFile("[mysqld]\ndefault-time-zone=+12:34"),
                message,
                incrementalSnapshot);
    }

    private void doValidate(
            MySqlVersion version,
            String configPath,
            String exceptionMessage,
            boolean incrementalSnapshot) {
        try (MySqlContainer container =
                new MySqlContainer(version).withConfigurationOverride(configPath)) {

            LOG.info("Starting containers...");
            Startables.deepStart(Stream.of(container)).join();
            LOG.info("Containers are started.");

            UniqueDatabase database =
                    new UniqueDatabase(
                            container,
                            "inventory",
                            container.getUsername(),
                            container.getPassword());

            Assertions.assertThatThrownBy(() -> startSource(database, incrementalSnapshot))
                    .isExactlyInstanceOf(ValidationException.class)
                    .hasMessage(exceptionMessage);
        }
    }

    private void startSource(UniqueDatabase database, boolean runIncrementalSnapshot)
            throws Exception {
        if (runIncrementalSnapshot) {
            MySqlSource<SourceRecord> mySqlSource =
                    MySqlSource.<SourceRecord>builder()
                            .hostname(database.getHost())
                            .username(database.getUsername())
                            .password(database.getPassword())
                            .port(database.getDatabasePort())
                            .databaseList(database.getDatabaseName())
                            .tableList(database.getDatabaseName() + ".products")
                            .deserializer(new MySqlTestUtils.ForwardDeserializeSchema())
                            .serverTimeZone("UTC")
                            .build();
            try (SplitEnumerator<MySqlSplit, PendingSplitsState> enumerator =
                    mySqlSource.createEnumerator(new MockSplitEnumeratorContext<>(1))) {
                enumerator.start();
            }

        } else {
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder(database, "UTC", false).build();
            setupSource(source);
        }
    }

    private String buildMySqlConfigFile(String content) {
        try {
            Path cnf = Files.createFile(Paths.get(tempFolder.toString(), "my.cnf"));
            Files.write(
                    cnf,
                    Collections.singleton(content),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
