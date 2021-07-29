/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import com.ververica.cdc.connectors.mysql.MySqlTestUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSource;
import com.ververica.cdc.connectors.mysql.source.utils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.basicSourceBuilder;
import static com.ververica.cdc.connectors.mysql.MySqlTestUtils.setupSource;
import static java.nio.file.Paths.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for the {@link MySqlValidator}. */
@RunWith(Parameterized.class)
public class MySqlValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlValidatorTest.class);

    private static TemporaryFolder tempFolder;
    private static File resourceFolder;
    private static final RowType SPLIT_KEYS =
            LogicalTypeUtils.toRowType(
                    DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType());
    final boolean runInParallel;

    @Parameterized.Parameters(name = "runInParallel = {0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    public MySqlValidatorTest(boolean runInParallel) {
        this.runInParallel = runInParallel;
    }

    @BeforeClass
    public static void setup() throws Exception {
        resourceFolder =
                get(Objects.requireNonNull(
                                        MySqlValidatorTest.class.getClassLoader().getResource("."))
                                .toURI())
                        .toFile();
        tempFolder = new TemporaryFolder(resourceFolder);
        tempFolder.create();
    }

    @AfterClass
    public static void tearDown() {
        tempFolder.delete();
    }

    @Test
    public void testValidateVersion() {
        String message =
                "Currently Flink MySql CDC connector only supports MySql whose version is larger or equal to 5.7.";
        runValidate("5.6", "docker/my.cnf", message);
    }

    @Test
    public void testValidateRowFormat() {
        String message =
                "The MySQL server is not configured to use a ROW binlog_format, "
                        + "which is required for this connector to work properly. "
                        + "Change the MySQL configuration to use a binlog_format=ROW and restart the connector.";
        runValidate("5.7", buildConfigFile("[mysqld]\nbinlog_format = STATEMENT"), message);
    }

    private void runValidate(String tag, String configPath, String exceptionMessage) {
        MySqlContainer container = new MySqlContainer(tag).withConfigurationOverride(configPath);
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
            assertTrue(e instanceof TableException);
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    private void startSource(UniqueDatabase database) throws Exception {
        if (runInParallel) {
            MySqlParallelSource<?> mySqlParallelSource =
                    new MySqlParallelSource<>(
                            new MySqlTestUtils.ForwardDeserializeSchema(),
                            Configuration.fromMap(database.getConfigMap()));
            mySqlParallelSource.createEnumerator(new MockSplitEnumeratorContext<>(1)).start();
        } else {
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder(database, false).build();
            setupSource(source);
        }
    }

    private String buildConfigFile(String content) {
        try {
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(get(folder.getPath(), "my.cnf"));
            Files.write(
                    cnf,
                    Collections.singleton(content),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
