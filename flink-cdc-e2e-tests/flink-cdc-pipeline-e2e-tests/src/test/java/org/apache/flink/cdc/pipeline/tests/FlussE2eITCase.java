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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** An End-to-end test case for Fluss pipeline connector. */
@Testcontainers
public class FlussE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(FlussE2eITCase.class);
    private static final Duration FLUSS_TESTCASE_TIMEOUT = Duration.ofMinutes(3);
    private static final String flussImageTag = "fluss/fluss:0.7.0";
    private static final String zooKeeperImageTag = "zookeeper:3.9.2";

    private static final List<String> flussCoordinatorProperties =
            Arrays.asList(
                    "zookeeper.address: zookeeper:2181",
                    "bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123",
                    "internal.listener.name: INTERNAL",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                    "security.sasl.enabled.mechanisms: PLAIN",
                    "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
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
                    "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
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

    protected final UniqueDatabase inventoryDatabaseWithPrimaryKey =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    protected final UniqueDatabase inventoryDatabaseWithoutPrimaryKey =
            new UniqueDatabase(
                    MYSQL, "mysql_inventory_wo_pk", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Override
    protected List<String> copyJarToFlinkLib() {
        // Due to a bug described in https://github.com/apache/fluss/pull/1267, it's not viable to
        // pass Fluss dependency with `--jar` CLI option. We may remove this workaround and use
        // `submitPipelineJob` to carry extra jar later.
        return Collections.singletonList("fluss-sql-connector.jar");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        inventoryDatabaseWithPrimaryKey.createAndInitialize();
        inventoryDatabaseWithoutPrimaryKey.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        inventoryDatabaseWithPrimaryKey.dropDatabase();
        inventoryDatabaseWithoutPrimaryKey.dropDatabase();
    }

    @ParameterizedTest(name = "PkTable: {0}")
    @ValueSource(booleans = {true, false})
    void testMySqlToFluss(boolean hasPrimaryKey) throws Exception {
        UniqueDatabase inventoryDatabase =
                hasPrimaryKey
                        ? inventoryDatabaseWithPrimaryKey
                        : inventoryDatabaseWithoutPrimaryKey;
        String database = inventoryDatabase.getDatabaseName();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  scan.incremental.snapshot.chunk.key-column: %s.\\.*:id\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        database,
                        database,
                        parallelism);
        Path flussConnector = TestUtils.getResource("fluss-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, flussConnector);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkResult(
                database,
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106, hammer, 16oz carpenter's hammer, 1.0, null, null, null",
                        "107, rocks, box of assorted rocks, 5.3, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null"));

        validateSinkResult(
                database,
                "customers",
                Arrays.asList(
                        "101, user_1, Shanghai, 123567891234",
                        "102, user_2, Shanghai, 123567891234",
                        "103, user_3, Shanghai, 123567891234",
                        "104, user_4, Shanghai, 123567891234"));

        if (!hasPrimaryKey) {
            // Non-primary key does not support deleting rows for now.
            return;
        }

        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        inventoryDatabase.getDatabaseName());

        // Fluss does not support applying DDL events for now.
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            stat.execute("DELETE FROM products WHERE id=111;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistant white wind breaker', 0.2, null, null, null);");
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter', 5.18, null, null, null);");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateSinkResult(
                database,
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106, hammer, 18oz carpenter hammer, 1.0, null, null, null",
                        "107, rocks, box of assorted rocks, 5.1, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null",
                        "110, jacket, water resistant white wind breaker, 0.2, null, null, null",
                        "111, scooter, Big 2-wheel scooter, 5.18, null, null, null"));
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
                .map(FlussE2eITCase::extractRow)
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

    private void validateSinkResult(String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Fluss {}::{} results...", database, table);
        long deadline = System.currentTimeMillis() + FLUSS_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        int rowCount = expected.size();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchFlussTableRows(database, table, rowCount);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + FLUSS_TESTCASE_TIMEOUT.toMillis())
                                / 1000);
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
}
