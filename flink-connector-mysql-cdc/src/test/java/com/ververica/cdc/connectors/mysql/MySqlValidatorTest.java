/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.table.api.ValidationException;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.MySqlVersion;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.basicSourceBuilder;
import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.setupSource;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.mysql.testutils.MySqlVersion.V5_5;
import static com.ververica.cdc.connectors.mysql.testutils.MySqlVersion.V5_7;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for the {@link MySqlValidator}. */
@RunWith(Parameterized.class)
public class MySqlValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlValidatorTest.class);

    private static TemporaryFolder tempFolder;
    private static File resourceFolder;

    @Parameterized.Parameter public boolean runIncrementalSnapshot;

    @Parameterized.Parameters(name = "runIncrementalSnapshot = {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @BeforeClass
    public static void setup() throws Exception {
        resourceFolder =
                Paths.get(
                                Objects.requireNonNull(
                                                MySqlValidatorTest.class
                                                        .getClassLoader()
                                                        .getResource("."))
                                        .toURI())
                        .toFile();
        tempFolder = new TemporaryFolder(resourceFolder);
        tempFolder.create();
    }

    @AfterClass
    public static void tearDown() {
        tempFolder.delete();
    }

    @Ignore("The jdbc driver used in this module cannot connect to MySQL 5.5")
    @Test
    public void testValidateVersion() {
        MySqlVersion version = V5_5;
        String message =
                String.format(
                        "Currently Flink MySql CDC connector only supports MySql whose version is larger or equal to 5.6, but actual is %s.",
                        version);
        doValidate(version, "docker/server/my.cnf", message);
    }

    @Test
    public void testValidateBinlogFormat() {
        String mode = "STATEMENT";
        String message =
                String.format(
                        "The MySQL server is configured with binlog_format %s rather than ROW, which is required for this "
                                + "connector to work properly. Change the MySQL configuration to use a binlog_format=ROW "
                                + "and restart the connector.",
                        mode);
        doValidate(V5_7, buildMySqlConfigFile("[mysqld]\nbinlog_format = " + mode), message);
    }

    @Test
    public void testValidateBinlogRowImage() {
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
                message);
    }

    @Test
    public void testValidateTimezone() {
        String message =
                String.format(
                        "The MySQL server has a timezone offset (%d seconds ahead of UTC) which does not match "
                                + "the configured timezone %s. Specify the right %s to avoid inconsistencies "
                                + "for time-related fields.",
                        45240, // +12:34 is 45240 seconds ahead of UTC
                        "UTC",
                        SERVER_TIME_ZONE.key());
        doValidate(V5_7, buildMySqlConfigFile("[mysqld]\ndefault-time-zone=+12:34"), message);
    }

    private void doValidate(MySqlVersion version, String configPath, String exceptionMessage) {
        MySqlContainer container =
                new MySqlContainer(version).withConfigurationOverride(configPath);

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(container)).join();
        LOG.info("Containers are started.");

        UniqueDatabase database =
                new UniqueDatabase(
                        container, "inventory", container.getUsername(), container.getPassword());

        try {
            startSource(database);
            fail("Should fail.");
        } catch (Exception e) {
            assertTrue(e instanceof ValidationException);
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    private void startSource(UniqueDatabase database) throws Exception {
        if (runIncrementalSnapshot) {
            MySqlSource<?> mySqlSource =
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

            mySqlSource.createEnumerator(new MockSplitEnumeratorContext<>(1)).start();
        } else {
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder(database, "UTC", false).build();
            setupSource(source);
        }
    }

    private String buildMySqlConfigFile(String content) {
        try {
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
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
