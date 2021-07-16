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

import org.apache.flink.table.api.TableException;

import com.ververica.cdc.connectors.mysql.source.utils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
public class MySqlValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlValidatorTest.class);

    private static TemporaryFolder tempFolder;
    private static File resourceFolder;

    private final List<TestSpec> specs =
            Arrays.asList(
                    TestSpec.newTestSpec("5.6")
                            .withException(
                                    "Currently Flink MySql CDC connector only supports MySql whose "
                                            + "version is larger or equal to 5.7."),
                    TestSpec.newTestSpec("5.7")
                            .withConfig("[mysqld]\nbinlog_format = STATEMENT")
                            .withException(
                                    "The MySQL server is not configured to use a ROW binlog_format, "
                                            + "which is required for this connector to work properly. "
                                            + "Change the MySQL configuration to use a binlog_format=ROW and restart the connector."));

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
    public void testValidator() {
        for (TestSpec spec : specs) {
            MySqlContainer container =
                    new MySqlContainer(spec.tag).withConfigurationOverride(spec.configPath);
            LOG.info("Starting containers...");
            Startables.deepStart(Stream.of(container)).join();
            LOG.info("Containers are started.");

            UniqueDatabase database =
                    new UniqueDatabase(container, "inventory", "mysqluser", "mysqlpw");
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder(container, database, false).build();

            try {
                setupSource(source);
                fail("Should fail.");
            } catch (Exception e) {
                assertTrue(e instanceof TableException);
                assertEquals(spec.exceptionMessage, e.getMessage());
            }
        }
    }

    private static class TestSpec {
        String tag;
        String configPath = "docker/my.cnf";
        String exceptionMessage;

        TestSpec(String tag) {
            this.tag = tag;
        }

        static TestSpec newTestSpec(String tag) {
            return new TestSpec(tag);
        }

        TestSpec withConfig(String config) {
            try {
                File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
                Path cnf = Files.createFile(get(folder.getPath(), "my.cnf"));
                Files.write(
                        cnf,
                        Collections.singleton(config),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.APPEND);
                this.configPath = get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create my.cnf file.", e);
            }
            return this;
        }

        TestSpec withException(String exceptionMessage) {
            this.exceptionMessage = exceptionMessage;
            return this;
        }
    }
}
