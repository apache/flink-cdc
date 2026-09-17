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

package org.apache.flink.cdc.pipeline.tests.stage2;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end tests for existing target table schema expansion. Verifies that the {@code
 * existing-table.schema-expansion.enabled} flag reaches the expander through all three execution
 * paths: regular streaming, distributed streaming, and batch.
 */
@Testcontainers
@ParameterizedClass
@ValueSource(ints = {1})
class ExistingTableSchemaExpansionE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG =
            LoggerFactory.getLogger(ExistingTableSchemaExpansionE2eITCase.class);

    private static final Duration TIMEOUT = Duration.ofMinutes(3);

    // Paimon warehouse and connector
    private static final String PAIMON_SQL_CONNECTOR_FORMAT = "paimon-sql-connector-%s.jar";

    ExistingTableSchemaExpansionE2eITCase(int parallelism) {
        super(parallelism);
    }

    // ------------------------------------------------------------------------------------------
    // Fluss containers (for distributed streaming test)
    // ------------------------------------------------------------------------------------------
    private static final String flussImageTag = "apache/fluss:0.9.0-incubating";
    private static final String zooKeeperImageTag = "zookeeper:3.9.2";

    private static final List<String> flussCoordinatorProperties =
            Arrays.asList(
                    "zookeeper.address: zookeeper:2181",
                    "bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123",
                    "internal.listener.name: INTERNAL",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                    "security.sasl.enabled.mechanisms: PLAIN",
                    "security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
                    "super.users: User:admin");

    private static final List<String> flussTabletServerProperties =
            Arrays.asList(
                    "zookeeper.address: zookeeper:2181",
                    "bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123",
                    "internal.listener.name: INTERNAL",
                    "tablet-server.id: 0",
                    "kv.snapshot.interval: 0s",
                    "data.dir: /tmp/fluss/data",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                    "security.sasl.enabled.mechanisms: PLAIN",
                    "security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
                    "super.users: User:admin");

    @Container private static final GenericContainer<?> ZOOKEEPER = containerZookeeper();

    @Container
    private static final GenericContainer<?> FLUSS_COORDINATOR = containerFlussCoordinator();

    @Container
    private static final GenericContainer<?> FLUSS_TABLET_SERVER = containerFlussTablet();

    private static GenericContainer<?> containerZookeeper() {
        return new GenericContainer<>(zooKeeperImageTag)
                .withNetworkAliases("zookeeper")
                .withExposedPorts(2181)
                .withNetwork(NETWORK)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    private static GenericContainer<?> containerFlussCoordinator() {
        return new GenericContainer<>(flussImageTag)
                .withEnv(
                        ImmutableMap.of(
                                "FLUSS_PROPERTIES", String.join("\n", flussCoordinatorProperties)))
                .withCommand("coordinatorServer")
                .withNetworkAliases("coordinator-server")
                .withExposedPorts(9123)
                .withNetwork(NETWORK)
                .dependsOn(ZOOKEEPER)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    private static GenericContainer<?> containerFlussTablet() {
        return new GenericContainer<>(flussImageTag)
                .withEnv(
                        ImmutableMap.of(
                                "FLUSS_PROPERTIES", String.join("\n", flussTabletServerProperties)))
                .withCommand("tabletServer")
                .withNetworkAliases("tablet-server")
                .withExposedPorts(9123)
                .withNetwork(NETWORK)
                .dependsOn(ZOOKEEPER, FLUSS_COORDINATOR)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "paimon_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeAll
    static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL, ZOOKEEPER, FLUSS_COORDINATOR, FLUSS_TABLET_SERVER))
                .join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        inventoryDatabase.createAndInitialize();
        copyPaimonJars();
    }

    @Override
    protected List<String> copyJarToFlinkLib() {
        return Collections.singletonList(String.format("fluss-flink-%s.jar", flinkVersion));
    }

    @AfterEach
    public void after() {
        super.after();
        inventoryDatabase.dropDatabase();
    }

    private void copyPaimonJars() throws Exception {
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource(getPaimonSQLConnectorName())),
                sharedVolume.toString() + "/" + getPaimonSQLConnectorName());
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-shade-hadoop.jar")),
                sharedVolume.toString() + "/flink-shade-hadoop.jar");
    }

    // ==========================================================================================
    // Test 1: Regular streaming path (MySQL -> Paimon)
    // Uses SchemaOperator + regular SchemaCoordinator
    // ==========================================================================================
    @Test
    void testRegularStreaming() throws Exception {
        String warehouse = sharedVolume.toString() + "/paimon_regular_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();

        preCreatePaimonTable(warehouse, database, "products", "id INT, name STRING");

        String pipelineJob = buildMysqlToPaimonPipeline(database, warehouse, "", "");
        Path paimonConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonConnector, hadoopJar);
        waitUntilJobRunning(TIMEOUT);
        LOG.info("Regular streaming pipeline is running");

        validatePaimonResult(warehouse, database, "products", expectedProductsData());
    }

    // ==========================================================================================
    // Test 2: Batch path (MySQL -> Paimon)
    // Uses BatchSchemaOperator
    // ==========================================================================================
    @Test
    void testBatch() throws Exception {
        String warehouse = sharedVolume.toString() + "/paimon_batch_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();

        preCreatePaimonTable(warehouse, database, "products", "id INT, name STRING");

        String pipelineJob =
                buildMysqlToPaimonPipeline(
                        database, warehouse, "  execution.runtime-mode: BATCH\n", "");
        Path paimonConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonConnector, hadoopJar);
        waitUntilJobFinished(TIMEOUT);
        LOG.info("Batch pipeline has finished");

        validatePaimonResult(warehouse, database, "products", expectedProductsData());
    }

    // ==========================================================================================
    // Test 3: Distributed streaming path (Fluss -> Fluss)
    // Fluss source's isParallelMetadataSource() returns true, triggering the distributed
    // SchemaCoordinator path instead of the regular one.
    // ==========================================================================================
    @Test
    void testDistributedStreaming() throws Exception {
        String sourceDb = "expansion_source";
        String targetDb = "expansion_target";

        // Pre-create Fluss source table with full schema and insert data.
        // Also pre-create target table with only id and name (partial schema).
        String setupSql =
                String.format(
                        "SET 'execution.runtime-mode' = 'batch';\n"
                                + "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                                + "CREATE CATALOG fluss_setup WITH (\n"
                                + "  'type' = 'fluss',\n"
                                + "  'bootstrap.servers' = 'coordinator-server:9123',\n"
                                + "  'client.security.protocol' = 'sasl',\n"
                                + "  'client.security.sasl.mechanism' = 'PLAIN',\n"
                                + "  'client.security.sasl.username' = 'developer',\n"
                                + "  'client.security.sasl.password' = 'developer-pass'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS fluss_setup.%s;\n"
                                + "CREATE DATABASE IF NOT EXISTS fluss_setup.%s;\n"
                                + "CREATE TABLE fluss_setup.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  description STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket-num' = '4', 'bucket-key' = 'id');\n"
                                + "CREATE TABLE fluss_setup.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket-num' = '4', 'bucket-key' = 'id');\n"
                                + "INSERT INTO fluss_setup.%s.products VALUES\n"
                                + "  (101, 'One', 'Alice'),\n"
                                + "  (102, 'Two', 'Bob'),\n"
                                + "  (103, 'Three', 'Cecily');",
                        sourceDb, targetDb, sourceDb, targetDb, sourceDb);

        executeSqlInFlinkSqlClient(setupSql, "setup_fluss");

        // Fluss -> Fluss with existing-table.schema-expansion.enabled: true.
        // Fluss source's isParallelMetadataSource() returns true, so the Composer
        // uses translateDistributed() which creates a distributed SchemaCoordinator.
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "  table.discoverer.pattern: %s\\.products\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "  existing-table.schema-expansion.enabled: true\n"
                                + "\n"
                                + "route:\n"
                                + "  - source-table: %s.products\n"
                                + "    sink-table: %s.products\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: lenient\n"
                                + "  parallelism: %s",
                        sourceDb, sourceDb, targetDb, parallelism);

        Path flussConnector = TestUtils.getResource("fluss-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, flussConnector);
        waitUntilJobRunning(TIMEOUT);
        LOG.info("Distributed streaming pipeline is running");

        validateFlussResult(
                targetDb,
                "products",
                Arrays.asList("101, One, Alice", "102, Two, Bob", "103, Three, Cecily"));
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private String buildMysqlToPaimonPipeline(
            String database, String warehouse, String extraPipelineOpts, String extraSinkOpts) {
        return String.format(
                "source:\n"
                        + "  type: mysql\n"
                        + "  hostname: mysql\n"
                        + "  port: 3306\n"
                        + "  username: %s\n"
                        + "  password: %s\n"
                        + "  tables: %s.\\.*\n"
                        + "  server-id: 5400-5404\n"
                        + "  server-time-zone: UTC\n"
                        + "  scan.startup.mode: snapshot\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: paimon\n"
                        + "  catalog.properties.warehouse: %s\n"
                        + "  catalog.properties.metastore: filesystem\n"
                        + "  catalog.properties.cache-enabled: false\n"
                        + "  existing-table.schema-expansion.enabled: true\n"
                        + extraSinkOpts
                        + "\n"
                        + "pipeline:\n"
                        + "  schema.change.behavior: lenient\n"
                        + "  parallelism: %s\n"
                        + extraPipelineOpts,
                MYSQL_TEST_USER,
                MYSQL_TEST_PASSWORD,
                database,
                warehouse,
                parallelism);
    }

    private void preCreatePaimonTable(
            String warehouse, String database, String table, String columns) throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG paimon_pre WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS paimon_pre.%s;\n"
                                + "CREATE TABLE IF NOT EXISTS paimon_pre.%s.%s (\n"
                                + "  %s\n"
                                + ") WITH ('bucket' = '4', 'bucket-key' = 'id');",
                        warehouse, database, database, table, columns);
        executeSqlInFlinkSqlClient(sql, "pre_create_paimon");
    }

    private void executeSqlInFlinkSqlClient(String sql, String scriptName) throws Exception {
        String containerPath = sharedVolume.toString() + "/" + scriptName + ".sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerPath);
        Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerPath);
        Assertions.assertThat(result.getExitCode())
                .as(
                        "SQL script %s should succeed. Stdout: %s Stderr: %s",
                        scriptName, result.getStdout(), result.getStderr())
                .isEqualTo(0);
    }

    private void validatePaimonResult(
            String warehouse, String database, String table, List<String> expected)
            throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchPaimonTableRows(warehouse, database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info("Successfully verified {} Paimon records.", expected.size());
                return;
            } catch (AssertionError e) {
                LOG.warn(
                        "Paimon results mismatch, expected {} got {}. Retrying...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private List<String> fetchPaimonTableRows(String warehouse, String database, String table)
            throws Exception {
        String peekTemplate =
                readLines("docker/peek-paimon.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(peekTemplate, warehouse, database, table);
        String containerPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerPath);
        Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to query Paimon. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(ExistingTableSchemaExpansionE2eITCase::extractRow)
                .map(row -> String.join(", ", row))
                .collect(Collectors.toList());
    }

    private void validateFlussResult(String database, String table, List<String> expected)
            throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        int rowCount = expected.size();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchFlussTableRows(database, table, rowCount);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info("Successfully verified {} Fluss records.", expected.size());
                return;
            } catch (AssertionError e) {
                LOG.warn(
                        "Fluss results mismatch, expected {} got {}. Retrying...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private List<String> fetchFlussTableRows(String database, String table, int rowCount)
            throws Exception {
        String peekTemplate =
                readLines("docker/peek-fluss.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(peekTemplate, database, table, rowCount);
        String containerPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerPath);
        Container.ExecResult result =
                jobManager.execInContainer("/opt/flink/bin/sql-client.sh", "-f", containerPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to query Fluss. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(ExistingTableSchemaExpansionE2eITCase::extractRow)
                .map(row -> String.join(", ", row))
                .collect(Collectors.toList());
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private String getPaimonSQLConnectorName() {
        return String.format(PAIMON_SQL_CONNECTOR_FORMAT, flinkVersion);
    }

    private List<String> expectedProductsData() {
        return Arrays.asList(
                "101, One, Alice, 3.202, red, {\"key1\": \"value1\"}, null",
                "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}, null",
                "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}, null",
                "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}, null",
                "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null",
                "106, Six, Ferris, 9.813, null, null, null",
                "107, Seven, Grace, 2.117, null, null, null",
                "108, Eight, Hesse, 6.819, null, null, null",
                "109, Nine, IINA, 5.223, null, null, null");
    }
}
