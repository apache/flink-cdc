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

import org.apache.flink.api.common.JobStatus;
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
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end tests verifying that the {@code existing-table.schema-expansion.mode} sink option
 * reaches the schema expander through the whole wiring path (YAML option, Composer, schema operator
 * factory, schema registry or batch schema operator, metadata applier) in all three execution
 * topologies: regular streaming, batch, and distributed streaming.
 *
 * <p>Every case pre-creates the target table with a strict subset of the source columns, so a flag
 * lost on any wiring hop leaves the missing columns unwritten and fails the assertions.
 */
@Testcontainers
@ParameterizedClass
@ValueSource(ints = {1, 4})
class ExistingTableSchemaExpansionE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG =
            LoggerFactory.getLogger(ExistingTableSchemaExpansionE2eITCase.class);

    private static final Duration EXPANSION_TESTCASE_TIMEOUT = Duration.ofMinutes(3);
    private static final String flussImageTag = "apache/fluss:0.9.0-incubating";
    private static final String zooKeeperImageTag = "zookeeper:3.9.2";

    ExistingTableSchemaExpansionE2eITCase(int parallelism) {
        super(parallelism);
    }

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

    @Container
    private static final GenericContainer<?> ZOOKEEPER =
            new GenericContainer<>(zooKeeperImageTag)
                    .withNetworkAliases("zookeeper")
                    .withExposedPorts(2181)
                    .withNetwork(NETWORK)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    private static final GenericContainer<?> FLUSS_COORDINATOR =
            new GenericContainer<>(flussImageTag)
                    .withEnv(
                            ImmutableMap.of(
                                    "FLUSS_PROPERTIES",
                                    String.join("\n", flussCoordinatorProperties)))
                    .withCommand("coordinatorServer")
                    .withNetworkAliases("coordinator-server")
                    .withExposedPorts(9123)
                    .withNetwork(NETWORK)
                    .dependsOn(ZOOKEEPER)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    private static final GenericContainer<?> FLUSS_TABLET_SERVER =
            new GenericContainer<>(flussImageTag)
                    .withEnv(
                            ImmutableMap.of(
                                    "FLUSS_PROPERTIES",
                                    String.join("\n", flussTabletServerProperties)))
                    .withCommand("tabletServer")
                    .withNetworkAliases("tablet-server")
                    .withExposedPorts(9123)
                    .withNetwork(NETWORK)
                    .dependsOn(ZOOKEEPER, FLUSS_COORDINATOR)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "paimon_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Override
    protected List<String> copyJarToFlinkLib() {
        // Due to a bug described in https://github.com/apache/fluss/pull/1267, it's not viable to
        // pass Fluss dependency with `--jar` CLI option.
        return Collections.singletonList(String.format("fluss-flink-%s.jar", flinkVersion));
    }

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
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(
                        TestUtils.getResource(getPaimonSQLConnectorResourceName())),
                sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName());
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-shade-hadoop.jar")),
                sharedVolume.toString() + "/flink-shade-hadoop.jar");
    }

    @AfterEach
    public void after() {
        super.after();
        inventoryDatabase.dropDatabase();
    }

    /**
     * MySQL reports no parallel metadata, so the Composer wires the regular schema operator backed
     * by the regular schema coordinator.
     */
    @Test
    void testRegularStreamingPath() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        preCreatePaimonProductsTable(warehouse, database);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.metastore: filesystem\n"
                                + "  catalog.properties.cache-enabled: false\n"
                                + "  existing-table.schema-expansion.mode: EXPAND\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonCdcConnector, hadoopJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        // `products` existed with (id, name) only, the remaining columns come from the expander.
        validatePaimonSinkResult(warehouse, database, "products", expectedProductsRows());
        // `customers` did not exist upfront, so the sink still creates it on its own.
        validatePaimonSinkResult(warehouse, database, "customers", expectedCustomersRows());
    }

    /**
     * Batch runtime mode makes the Composer wire a batch schema operator instead of the
     * coordinator-based one. MySQL accepts the {@code snapshot} startup mode only in batch
     * pipelines, which also bounds the source so that the job reaches a terminal state.
     */
    @Test
    void testBatchPath() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        preCreatePaimonProductsTable(warehouse, database);

        String pipelineJob =
                String.format(
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
                                + "  existing-table.schema-expansion.mode: EXPAND\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s\n"
                                + "  execution.runtime-mode: BATCH",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonCdcConnector, hadoopJar);
        waitUntilJobFinished(EXPANSION_TESTCASE_TIMEOUT);
        LOG.info("Batch pipeline job has finished");

        validatePaimonSinkResult(warehouse, database, "products", expectedProductsRows());
        validatePaimonSinkResult(warehouse, database, "customers", expectedCustomersRows());
    }

    /**
     * Fluss reports parallel metadata, so the Composer wires the distributed schema operator backed
     * by the distributed schema coordinator. That topology only accepts {@code LENIENT}, {@code
     * IGNORE} and {@code EXCEPTION}, and the expander deliberately stays a no-op for the latter
     * two.
     */
    @Test
    void testDistributedStreamingPath() throws Exception {
        String sourceDatabase = "expansion_source_" + parallelism;
        String sinkDatabase = "expansion_sink_" + parallelism;
        prepareFlussTables(sourceDatabase, sinkDatabase);

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
                                + "  scan.startup.mode: earliest\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "  existing-table.schema-expansion.mode: EXPAND\n"
                                + "\n"
                                + "route:\n"
                                + "  - source-table: %s.products\n"
                                + "    sink-table: %s.products\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: lenient\n"
                                + "  parallelism: %s",
                        sourceDatabase, sourceDatabase, sinkDatabase, parallelism);
        Path flussCdcConnector = TestUtils.getResource("fluss-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, flussCdcConnector);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Distributed pipeline job is running");

        // The sink table existed with (id, name) only, `description` comes from the expander.
        validateFlussSinkResult(
                sinkDatabase,
                "products",
                Arrays.asList("101, One, Alice", "102, Two, Bob", "103, Three, Cecily"));
    }

    /**
     * CHECK mode never issues DDL: a target table missing upstream columns must fail the job, and
     * the target schema must stay untouched.
     */
    @Test
    void testCheckFailsOnMissingColumn() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        preCreatePaimonProductsTable(warehouse, database);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.products\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.metastore: filesystem\n"
                                + "  catalog.properties.cache-enabled: false\n"
                                + "  existing-table.schema-expansion.mode: CHECK\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonCdcConnector, hadoopJar);

        // The job must fail because the pre-created table lacks upstream columns, and CHECK never
        // repairs it: the target schema must stay unchanged.
        waitUntilJobState(EXPANSION_TESTCASE_TIMEOUT, JobStatus.FAILED);
        Assertions.assertThat(fetchPaimonTableColumns(warehouse, database, "products"))
                .containsExactly("id", "name");
    }

    /**
     * CHECK mode with a fully compatible target table passes without issuing DDL, and the pipeline
     * keeps syncing data.
     */
    @Test
    void testCheckPassesWithCompatibleTable() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        preCreateCompatiblePaimonProductsTable(warehouse, database);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.products\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.metastore: filesystem\n"
                                + "  catalog.properties.cache-enabled: false\n"
                                + "  existing-table.schema-expansion.mode: CHECK\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonCdcConnector, hadoopJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("CHECK-mode pipeline job is running");

        validatePaimonSinkResult(
                warehouse, database, "products", expectedProductsRowsWithoutDefaultValue());
    }

    /**
     * EXPAND mode is strict: a target column that cannot safely contain the pipeline type fails the
     * job instead of being silently dropped.
     */
    @Test
    void testExpandFailsOnIncompatibleTarget() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        preCreateIncompatiblePaimonProductsTable(warehouse, database);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.products\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.metastore: filesystem\n"
                                + "  catalog.properties.cache-enabled: false\n"
                                + "  existing-table.schema-expansion.mode: EXPAND\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        submitPipelineJob(pipelineJob, paimonCdcConnector, hadoopJar);

        // id STRING cannot safely contain the INT pipeline column, so EXPAND must fail the job.
        waitUntilJobState(EXPANSION_TESTCASE_TIMEOUT, JobStatus.FAILED);
    }

    /**
     * The distributed schema coordinator honors CHECK as well: missing columns in the routed Fluss
     * target table fail the job without touching the target schema.
     */
    @Test
    void testCheckFailsOnMissingColumnInDistributedTopology() throws Exception {
        String sourceDatabase = "check_source_" + parallelism;
        String sinkDatabase = "check_sink_" + parallelism;
        prepareFlussTables(sourceDatabase, sinkDatabase);

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
                                + "  scan.startup.mode: earliest\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "  existing-table.schema-expansion.mode: CHECK\n"
                                + "\n"
                                + "route:\n"
                                + "  - source-table: %s.products\n"
                                + "    sink-table: %s.products\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: lenient\n"
                                + "  parallelism: %s",
                        sourceDatabase, sourceDatabase, sinkDatabase, parallelism);
        Path flussCdcConnector = TestUtils.getResource("fluss-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, flussCdcConnector);

        // The distributed schema coordinator must fail the job because the routed target table
        // lacks the description column, leaving the target schema unchanged.
        waitUntilJobState(EXPANSION_TESTCASE_TIMEOUT, JobStatus.FAILED);
        Assertions.assertThat(fetchFlussTableColumns(sinkDatabase, "products"))
                .containsExactly("id", "name");
    }

    /** Creates the Paimon target table holding a strict subset of the source columns. */
    private void preCreatePaimonProductsTable(String warehouse, String database) throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG paimon_catalog WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS paimon_catalog.%s;\n"
                                + "CREATE TABLE paimon_catalog.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                // Use Paimon's default dynamic bucket so the pre-created table
                                // matches what the CDC sink's PaimonHashFunction assumes; a fixed
                                // bucket here would mismatch the sink's pre-partitioning and drop
                                // records.
                                + ") WITH ('bucket' = '-1');",
                        warehouse, database, database);
        executePaimonSql(sql, "pre_create_paimon");
    }

    /** Creates the Paimon target table with every upstream column and matching types. */
    private void preCreateCompatiblePaimonProductsTable(String warehouse, String database)
            throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG paimon_catalog WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS paimon_catalog.%s;\n"
                                + "CREATE TABLE paimon_catalog.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  description STRING,\n"
                                + "  weight FLOAT,\n"
                                + "  enum_c STRING,\n"
                                + "  json_c STRING,\n"
                                + "  point_c STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket' = '-1');",
                        warehouse, database, database);
        executePaimonSql(sql, "pre_create_compatible_paimon");
    }

    /**
     * Creates the Paimon target table whose key column type cannot safely contain the pipeline
     * type.
     */
    private void preCreateIncompatiblePaimonProductsTable(String warehouse, String database)
            throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG paimon_catalog WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS paimon_catalog.%s;\n"
                                + "CREATE TABLE paimon_catalog.%s.products (\n"
                                + "  id STRING NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket' = '-1');",
                        warehouse, database, database);
        executePaimonSql(sql, "pre_create_incompatible_paimon");
    }

    private List<String> fetchPaimonTableColumns(String warehouse, String database, String table)
            throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG paimon_catalog WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "DESCRIBE paimon_catalog.%s.%s;",
                        warehouse, database, table);
        String containerSqlPath = sharedVolume.toString() + "/describe_paimon.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);
        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to describe Paimon table. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return extractDescribeColumns(result.getStdout());
    }

    private List<String> fetchFlussTableColumns(String database, String table) throws Exception {
        String sql =
                String.format(
                        "CREATE CATALOG fluss_catalog WITH (\n"
                                + "  'type' = 'fluss',\n"
                                + "  'bootstrap.servers' = 'coordinator-server:9123',\n"
                                + "  'client.security.protocol' = 'sasl',\n"
                                + "  'client.security.sasl.mechanism' = 'PLAIN',\n"
                                + "  'client.security.sasl.username' = 'developer',\n"
                                + "  'client.security.sasl.password' = 'developer-pass'\n"
                                + ");\n"
                                + "DESCRIBE fluss_catalog.%s.%s;",
                        database, table);
        String containerSqlPath = sharedVolume.toString() + "/describe_fluss.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);
        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer("/opt/flink/bin/sql-client.sh", "-f", containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to describe Fluss table. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return extractDescribeColumns(result.getStdout());
    }

    /** Extracts column names from the tableau output of a DESCRIBE statement. */
    private static List<String> extractDescribeColumns(String stdout) {
        return Arrays.stream(stdout.split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(ExistingTableSchemaExpansionE2eITCase::extractRow)
                .filter(row -> row.length > 0)
                .map(row -> row[0])
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Creates and populates the Fluss source table with the full schema, plus the routed target
     * table holding a strict subset of the source columns.
     */
    private void prepareFlussTables(String sourceDatabase, String sinkDatabase) throws Exception {
        String sql =
                String.format(
                        "SET 'execution.runtime-mode' = 'batch';\n"
                                + "CREATE CATALOG fluss_catalog WITH (\n"
                                + "  'type' = 'fluss',\n"
                                + "  'bootstrap.servers' = 'coordinator-server:9123',\n"
                                + "  'client.security.protocol' = 'sasl',\n"
                                + "  'client.security.sasl.mechanism' = 'PLAIN',\n"
                                + "  'client.security.sasl.username' = 'developer',\n"
                                + "  'client.security.sasl.password' = 'developer-pass'\n"
                                + ");\n"
                                + "CREATE DATABASE IF NOT EXISTS fluss_catalog.%s;\n"
                                + "CREATE DATABASE IF NOT EXISTS fluss_catalog.%s;\n"
                                + "CREATE TABLE fluss_catalog.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  description STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket-num' = '4', 'bucket-key' = 'id');\n"
                                + "CREATE TABLE fluss_catalog.%s.products (\n"
                                + "  id INT NOT NULL,\n"
                                + "  name STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH ('bucket-num' = '4', 'bucket-key' = 'id');\n"
                                + "INSERT INTO fluss_catalog.%s.products VALUES\n"
                                + "  (101, 'One', 'Alice'),\n"
                                + "  (102, 'Two', 'Bob'),\n"
                                + "  (103, 'Three', 'Cecily');",
                        sourceDatabase, sinkDatabase, sourceDatabase, sinkDatabase, sourceDatabase);
        executeFlussSql(sql, "prepare_fluss");
    }

    /** Runs a script against the Paimon catalog, whose connector must be passed explicitly. */
    private void executePaimonSql(String sql, String scriptName) throws Exception {
        String containerSqlPath = sharedVolume.toString() + "/" + scriptName + ".sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);
        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerSqlPath);
        checkSqlResult(result, scriptName);
    }

    /** Runs a script against the Fluss catalog, whose connector already sits in Flink's lib. */
    private void executeFlussSql(String sql, String scriptName) throws Exception {
        String containerSqlPath = sharedVolume.toString() + "/" + scriptName + ".sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);
        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer("/opt/flink/bin/sql-client.sh", "-f", containerSqlPath);
        checkSqlResult(result, scriptName);
    }

    private static void checkSqlResult(
            org.testcontainers.containers.Container.ExecResult result, String scriptName) {
        Assertions.assertThat(result.getExitCode())
                .as(
                        "SQL script %s should succeed. Stdout: %s; Stderr: %s",
                        scriptName, result.getStdout(), result.getStderr())
                .isZero();
    }

    private List<String> fetchPaimonTableRows(String warehouse, String database, String table)
            throws Exception {
        String template =
                readLines("docker/peek-paimon.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, database, table);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/" + getPaimonSQLConnectorResourceName(),
                        "--jar",
                        sharedVolume.toString() + "/flink-shade-hadoop.jar",
                        "-f",
                        containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to execute peek script. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(ExistingTableSchemaExpansionE2eITCase::extractRow)
                .map(row -> String.format("%s", String.join(", ", row)))
                .collect(Collectors.toList());
    }

    private List<String> fetchFlussTableRows(String database, String table, int rowCount)
            throws Exception {
        String template =
                readLines("docker/peek-fluss.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, database, table, rowCount);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer("/opt/flink/bin/sql-client.sh", "-f", containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to execute peek script. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(ExistingTableSchemaExpansionE2eITCase::extractRow)
                .map(row -> String.format("%s", String.join(", ", row)))
                .collect(Collectors.toList());
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private void validatePaimonSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + EXPANSION_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchPaimonTableRows(warehouse, database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info("Successfully verified {} records.", expected.size());
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                // AssertionError contains way too much records and might flood the log output.
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private void validateFlussSinkResult(String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Fluss {}::{} results...", database, table);
        long deadline = System.currentTimeMillis() + EXPANSION_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        int rowCount = expected.size();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchFlussTableRows(database, table, rowCount);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info("Successfully verified {} records.", expected.size());
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                // AssertionError contains way too much records and might flood the log output.
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static List<String> expectedProductsRows() {
        // EXPAND adds enum_c via derived DDL, which carries the MySQL DEFAULT 'red' into the
        // Paimon column definition; the sink then applies it to NULL rows.
        return Arrays.asList(
                "101, One, Alice, 3.202, red, {\"key1\": \"value1\"}, null",
                "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}, null",
                "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}, null",
                "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}, null",
                "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null",
                "106, Six, Ferris, 9.813, red, null, null",
                "107, Seven, Grace, 2.117, red, null, null",
                "108, Eight, Hesse, 6.819, red, null, null",
                "109, Nine, IINA, 5.223, red, null, null");
    }

    private static List<String> expectedProductsRowsWithoutDefaultValue() {
        // CHECK pre-creates enum_c without a Paimon column default, so NULL rows stay NULL.
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

    private static List<String> expectedCustomersRows() {
        return Arrays.asList(
                "101, user_1, Shanghai, 123567891234",
                "102, user_2, Shanghai, 123567891234",
                "103, user_3, Shanghai, 123567891234",
                "104, user_4, Shanghai, 123567891234");
    }

    protected String getPaimonSQLConnectorResourceName() {
        return String.format("paimon-sql-connector-%s.jar", flinkVersion);
    }
}
