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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.utils.TestCaseUtils;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;

/** A more complicated IT case for Evolving MySQL schema with gh-ost/pt-osc utility. */
class MySqlOscITCase extends MySqlSourceTestBase {
    private static final MySqlContainer MYSQL8_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private static final String PERCONA_TOOLKIT = "perconalab/percona-toolkit:3.5.7";

    protected static final GenericContainer<?> PERCONA_TOOLKIT_CONTAINER =
            createPerconaToolkitContainer();

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "customer", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final String GH_OST_RESOURCE_NAME =
            DockerClientFactory.instance().client().versionCmd().exec().getArch().equals("amd64")
                    ? "ghost-cli/gh-ost-binary-linux-amd64-20231207144046.tar.gz"
                    : "ghost-cli/gh-ost-binary-linux-arm64-20231207144046.tar.gz";

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void takeoverOutput() {
        System.setOut(new PrintStream(outCaptor));
    }

    @AfterEach
    protected void handInStdOut() {
        System.setOut(standardOut);
        outCaptor.reset();
    }

    @BeforeAll
    static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        Startables.deepStart(Stream.of(PERCONA_TOOLKIT_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterAll
    static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        PERCONA_TOOLKIT_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @BeforeEach
    void before() {
        customerDatabase.createAndInitialize();
        TestValuesTableFactory.clearAllData();
        ValuesDatabase.clear();
        env.setParallelism(4);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @AfterEach
    void after() {
        customerDatabase.dropDatabase();
    }

    private static void installGhOstCli(Container<?> container) {
        try {
            container.copyFileToContainer(
                    MountableFile.forClasspathResource(GH_OST_RESOURCE_NAME), "/tmp/gh-ost.tar.gz");
            execInContainer(
                    container, "unzip binary", "tar", "-xzvf", "/tmp/gh-ost.tar.gz", "-C", "/bin");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericContainer<?> createPerconaToolkitContainer() {
        GenericContainer<?> perconaToolkit =
                new GenericContainer<>(PERCONA_TOOLKIT)
                        // keep container alive
                        .withCommand("tail", "-f", "/dev/null")
                        .withNetwork(NETWORK)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        return perconaToolkit;
    }

    private void insertRecordsPhase1(UniqueDatabase database, int startIndex, int count)
            throws Exception {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (int i = startIndex; i < startIndex + count; i++) {
                statement.execute(
                        String.format(
                                "insert into customers (id, name, address, phone_number) values (%s, '%s', '%s', '%s');",
                                i, "flink_" + i, "Address Line #" + i, 1000000000L + i));
            }
        }
    }

    private void insertRecordsPhase2(UniqueDatabase database, int startIndex, int count)
            throws Exception {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (int i = startIndex; i < startIndex + count; i++) {
                statement.execute(
                        String.format(
                                "insert into customers (id, name, address, phone_number, ext) values (%s, '%s', '%s', '%s', %s);",
                                i, "flink_" + i, "Address Line #" + i, 1000000000L + i, i));
            }
        }
    }

    @Test
    void testGhOstSchemaMigration() throws Exception {
        String databaseName = customerDatabase.getDatabaseName();

        LOG.info("Step 1: Install gh-ost command line utility");
        installGhOstCli(MYSQL8_CONTAINER);

        Thread yamlJob = runJob(databaseName, "customers");
        yamlJob.start();

        LOG.info("Step 2: Start pipeline job");
        insertRecordsPhase1(customerDatabase, 5000, 1000);

        LOG.info("Step 3: Evolve schema with gh-ost - ADD COLUMN");

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                execInContainer(
                                        MYSQL8_CONTAINER,
                                        "evolve schema",
                                        "gh-ost",
                                        "--user=" + TEST_USER,
                                        "--password=" + TEST_PASSWORD,
                                        "--database=" + databaseName,
                                        "--table=customers",
                                        "--alter=add column ext int first",
                                        "--allow-on-master", // because we don't have a replica
                                        "--initially-drop-old-table", // drop previously generated
                                        // temporary tables
                                        "--execute");
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });

        thread.start();
        insertRecordsPhase1(customerDatabase, 7000, 3000);

        thread.join();
        insertRecordsPhase2(customerDatabase, 12000, 1000);

        try {
            TestCaseUtils.repeatedCheck(
                    () -> outCaptor.toString().split(System.lineSeparator()).length == 5023);
        } catch (Exception e) {
            LOG.error("Failed to verify results. Captured stdout: {}", outCaptor.toString(), e);
        } finally {
            yamlJob.interrupt();
        }
    }

    @Test
    void testPtOscSchemaMigration() throws Exception {
        String databaseName = customerDatabase.getDatabaseName();

        LOG.info("Step 1: Install gh-ost command line utility");
        installGhOstCli(MYSQL8_CONTAINER);

        Thread yamlJob = runJob(databaseName, "customers");
        yamlJob.start();

        LOG.info("Step 2: Start pipeline job");
        insertRecordsPhase1(customerDatabase, 5000, 1000);

        LOG.info("Step 3: Evolve schema with gh-ost - ADD COLUMN");

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                execInContainer(
                                        PERCONA_TOOLKIT_CONTAINER,
                                        "evolve schema",
                                        "pt-online-schema-change",
                                        "--user=" + TEST_USER,
                                        "--host=" + INTER_CONTAINER_MYSQL_ALIAS,
                                        "--password=" + TEST_PASSWORD,
                                        "P=3306,t=customers,D=" + databaseName,
                                        "--alter",
                                        "add column ext int first",
                                        "--charset=utf8",
                                        "--recursion-method=NONE", // Do not look for slave nodes
                                        "--print",
                                        "--execute");
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });

        LOG.info("Insertion Phase 1 finishes");
        thread.start();
        insertRecordsPhase1(customerDatabase, 7000, 3000);
        LOG.info("Insertion Phase 2 finishes");

        thread.join();
        insertRecordsPhase2(customerDatabase, 12000, 1000);
        LOG.info("Insertion Phase 3 finishes");

        try {
            TestCaseUtils.repeatedCheck(
                    () -> outCaptor.toString().split(System.lineSeparator()).length == 5023);
        } catch (Exception e) {
            LOG.error("Failed to verify results. Captured stdout: {}", outCaptor.toString(), e);
        } finally {
            yamlJob.interrupt();
        }
    }

    private static void execInContainer(Container<?> container, String prompt, String... commands)
            throws IOException, InterruptedException {
        {
            LOG.info(
                    "Starting to {} with the following command: `{}`",
                    prompt,
                    String.join(" ", commands));
            Container.ExecResult execResult = container.execInContainer(commands);
            if (execResult.getExitCode() == 0) {
                LOG.info("Successfully {}. Stdout: {}", prompt, execResult.getStdout());
            } else {
                LOG.error(
                        "Failed to {}. Exit code: {}, Stdout: {}, Stderr: {}",
                        prompt,
                        execResult.getExitCode(),
                        execResult.getStdout(),
                        execResult.getStderr());
                throw new IOException("Failed to execute commands: " + String.join(" ", commands));
            }
        }
    }

    private Thread runJob(String databaseName, String tableName) {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup MySQL source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(MySqlDataSourceOptions.HOSTNAME, MYSQL8_CONTAINER.getHost());
        sourceConfig.set(MySqlDataSourceOptions.PORT, MYSQL8_CONTAINER.getDatabasePort());
        sourceConfig.set(MySqlDataSourceOptions.USERNAME, TEST_USER);
        sourceConfig.set(MySqlDataSourceOptions.PASSWORD, TEST_PASSWORD);
        sourceConfig.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfig.set(MySqlDataSourceOptions.TABLES, databaseName + "." + tableName);
        sourceConfig.set(MySqlDataSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES, true);

        SourceDef sourceDef =
                new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "MySQL Source", sourceConfig);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.singletonList(
                                new RouteDef(
                                        databaseName + "." + tableName,
                                        "sink_db.sink_tbl",
                                        null,
                                        null)),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = composer.compose(pipelineDef);
        return new Thread(
                () -> {
                    try {
                        execution.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
