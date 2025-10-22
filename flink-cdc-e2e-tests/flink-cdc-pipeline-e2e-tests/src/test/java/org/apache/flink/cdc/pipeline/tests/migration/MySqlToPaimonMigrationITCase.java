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

package org.apache.flink.cdc.pipeline.tests.migration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.cdc.pipeline.tests.utils.TarballFetcher;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * E2e cases for stopping & restarting jobs of `MySQL source to Paimon sink` from previous state.
 */
class MySqlToPaimonMigrationITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToPaimonMigrationITCase.class);

    private static final Duration PAIMON_TESTCASE_TIMEOUT = Duration.ofMinutes(3);

    protected UniqueDatabase mysqlInventoryDatabase;
    private final Function<String, String> dbNameFormatter =
            (s) -> String.format(s, mysqlInventoryDatabase.getDatabaseName());

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase =
                new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
        mysqlInventoryDatabase.createAndInitialize();
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
        if (mysqlInventoryDatabase != null) {
            mysqlInventoryDatabase.dropDatabase();
        }
    }

    @Test
    void testBasicJobSubmitting() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String content =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
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
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MySqlContainer.MYSQL_PORT,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        warehouse,
                        4);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        JobID jobID = submitPipelineJob(content, paimonCdcConnector, hadoopJar);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job ID is {} ", jobID);
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
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
        LOG.info("Snapshot phase successfully finished.");

        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Job gracefully stopped.");
    }

    @ParameterizedTest(name = "{0} -> SNAPSHOT")
    @EnumSource(names = {"V3_2_1", "V3_3_0", "V3_4_0", "V3_5_0", "SNAPSHOT"})
    void testStartingJobFromSavepoint(TarballFetcher.CdcVersion migrateFromVersion)
            throws Exception {
        TarballFetcher.fetch(jobManager, migrateFromVersion);
        LOG.info("Successfully fetched CDC {}.", migrateFromVersion);

        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String content =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
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
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MySqlContainer.MYSQL_PORT,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        warehouse,
                        4);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        JobID jobID = submitPipelineJob(content, paimonCdcConnector, hadoopJar);
        Assertions.assertThat(jobID).isNotNull();

        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
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
        LOG.info("Snapshot stage finished successfully.");

        generateIncrementalEventsPhaseOne();
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
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
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null"));
        LOG.info("Incremental stage 1 finished successfully.");

        generateIncrementalEventsPhaseTwo();
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, null",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}, null",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}, null",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}, null",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}, null",
                        "106, hammer, 18oz carpenter hammer, 1.0, null, null, null, null",
                        "107, rocks, box of assorted rocks, 5.1, null, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null, null",
                        "110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1",
                        "111, scooter, Big 2-wheel scooter, 5.18, null, null, null, 1"));
        LOG.info("Incremental stage 2 finished successfully.");

        String savepointPath = stopJobWithSavepoint(jobID);
        LOG.info("Stopped Job {} and created a savepoint at {}.", jobID, savepointPath);

        JobID newJobID =
                submitPipelineJob(content, savepointPath, true, paimonCdcConnector, hadoopJar);
        LOG.info("Reincarnated Job {} has been submitted successfully.", newJobID);

        generateIncrementalEventsPhaseThree();
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, null",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}, null",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}, null",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}, null",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}, null",
                        "106, hammer, 18oz carpenter hammer, 1.0, null, null, null, null",
                        "107, rocks, box of assorted rocks, 5.1, null, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null, null",
                        "110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1"));
        cancelJob(newJobID);
    }

    @ParameterizedTest(name = "{0} -> SNAPSHOT")
    @EnumSource(names = {"SNAPSHOT"})
    void testStartingJobFromSavepointWithSchemaChange(TarballFetcher.CdcVersion migrateFromVersion)
            throws Exception {
        TarballFetcher.fetch(jobManager, migrateFromVersion);
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", "/tmp/cdc/");

        LOG.info("Successfully fetched CDC {}.", migrateFromVersion);

        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String content =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
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
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MySqlContainer.MYSQL_PORT,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        warehouse,
                        4);
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        JobID jobID = submitPipelineJob(migrateFromVersion, content, paimonCdcConnector, hadoopJar);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job ID is {} ", jobID);

        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
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
        LOG.info("Snapshot stage finished successfully.");

        generateIncrementalEventsPhaseOne();
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
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
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null"));
        LOG.info("Incremental stage 1 finished successfully.");

        String savepointPath = stopJobWithSavepoint(jobID);
        LOG.info("Stopped Job {} and created a savepoint at {}.", jobID, savepointPath);
        // Modify schema and make some data changes.
        generateIncrementalEventsPhaseTwo();
        JobID newJobID =
                submitPipelineJob(content, savepointPath, true, paimonCdcConnector, hadoopJar);
        LOG.info("Reincarnated Job {} has been submitted successfully.", newJobID);
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, null",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}, null",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}, null",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}, null",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}, null",
                        "106, hammer, 18oz carpenter hammer, 1.0, null, null, null, null",
                        "107, rocks, box of assorted rocks, 5.1, null, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null, null",
                        "110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1",
                        "111, scooter, Big 2-wheel scooter, 5.18, null, null, null, 1"));
        LOG.info("Incremental stage 2 finished successfully.");

        generateIncrementalEventsPhaseThree();
        validateSinkResult(
                warehouse,
                mysqlInventoryDatabase.getDatabaseName(),
                "products",
                Arrays.asList(
                        "101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, null",
                        "102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}, null",
                        "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}, null",
                        "104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}, null",
                        "105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}, null",
                        "106, hammer, 18oz carpenter hammer, 1.0, null, null, null, null",
                        "107, rocks, box of assorted rocks, 5.1, null, null, null, null",
                        "108, jacket, water resistent black wind breaker, 0.1, null, null, null, null",
                        "109, spare tire, 24 inch spare tire, 22.2, null, null, null, null",
                        "110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1"));
        cancelJob(newJobID);
    }

    private void generateIncrementalEventsPhaseOne() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;",
                "UPDATE products SET weight='5.1' WHERE id=107;");
    }

    private void generateIncrementalEventsPhaseTwo() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;",
                "UPDATE products SET weight='5.1' WHERE id=107;",
                "ALTER TABLE products ADD COLUMN new_col INT;",
                "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);",
                "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);");
    }

    private void generateIncrementalEventsPhaseThree() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;",
                "UPDATE products SET weight='5.17' WHERE id=111;",
                "DELETE FROM products WHERE id=111;");
    }

    private void executeMySqlStatements(UniqueDatabase database, String... statements) {
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            for (String sql : statements) {
                try {
                    stat.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to execute SQL statement " + sql, e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute MySQL statements.", e);
        }
    }

    private void validateSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + PAIMON_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchPaimonTableRows(warehouse, database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + PAIMON_TESTCASE_TIMEOUT.toMillis())
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

    private List<String> fetchPaimonTableRows(String warehouse, String database, String table)
            throws Exception {
        String template =
                readLines("docker/peek-paimon.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, database, table);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        Container.ExecResult result =
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
                .map(MySqlToPaimonMigrationITCase::extractRow)
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

    protected String getPaimonSQLConnectorResourceName() {
        return String.format("paimon-sql-connector-%s.jar", flinkVersion);
    }
}
