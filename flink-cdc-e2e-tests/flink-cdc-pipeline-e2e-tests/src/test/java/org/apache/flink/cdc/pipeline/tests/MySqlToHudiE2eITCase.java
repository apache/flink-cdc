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
import org.apache.flink.cdc.pipeline.tests.utils.TarballFetcher;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for mysql cdc to Iceberg pipeline job. */
public class MySqlToHudiE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToHudiE2eITCase.class);

    private static final Duration HUDI_TESTCASE_TIMEOUT = Duration.ofMinutes(20);

    private static final String FLINK_LIB_DIR = "/opt/flink/lib";

    private static final String PEEK_SQL_FILE = "peek-hudi.sql";

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "hudi_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private String warehouse;

    @BeforeAll
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("Containers are started.");
    }

    /*
     * The Flink SQL Client requires certain core dependencies, like Hadoop's FileSystem,
     * on its main classpath (`/lib`) to be discovered correctly by the ServiceLoader.
     * Adding them as temporary session JARs via the `--jar` flag is unreliable for these
     * low-level services.
     *
     * By copying these dependencies directly into the container's `/opt/flink/lib`
     * directory, we ensure they are loaded by Flink's main classloader, which
     * permanently resolves the `No FileSystem for scheme: file` error during validation.
     */
    @BeforeEach
    @Override
    public void before() throws Exception {
        LOG.info("Starting containers...");
        jobManagerConsumer = new ToStringConsumer();
        // Using FixedHost instead of GenericContainer to ensure that ports are fixed for easier
        // debugging during dev
        jobManager =
                new FixedHostPortGenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withFixedExposedPort(8081, 8081)
                        .withFixedExposedPort(9005, 9005)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withEnv(
                                "FLINK_ENV_JAVA_OPTS",
                                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:9005")
                        .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(sharedVolume))
                        .withLogConsumer(jobManagerConsumer);
        Startables.deepStart(Stream.of(jobManager)).join();
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", sharedVolume.toString());

        taskManagerConsumer = new ToStringConsumer();
        taskManager =
                new FixedHostPortGenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withFixedExposedPort(9006, 9006)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withEnv(
                                "FLINK_ENV_JAVA_OPTS",
                                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:9006")
                        .dependsOn(jobManager)
                        .withVolumesFrom(jobManager, BindMode.READ_WRITE)
                        .withLogConsumer(taskManagerConsumer);
        Startables.deepStart(Stream.of(taskManager)).join();
        runInContainerAsRoot(taskManager, "chmod", "0777", "-R", sharedVolume.toString());

        TarballFetcher.fetchLatest(jobManager);
        LOG.info("CDC executables deployed.");

        inventoryDatabase.createAndInitialize();

        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource(getHudiSQLConnectorResourceName())),
                FLINK_LIB_DIR + "/" + getHudiSQLConnectorResourceName());
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-shade-hadoop.jar")),
                FLINK_LIB_DIR + "/flink-shade-hadoop.jar");
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("hudi-hadoop-common.jar")),
                FLINK_LIB_DIR + "/hudi-hadoop-common.jar");
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-hadoop-compatibility.jar")),
                FLINK_LIB_DIR + "/flink-hadoop-compatibility.jar");
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-parquet.jar")),
                FLINK_LIB_DIR + "/flink-parquet.jar");
    }

    @AfterEach
    public void after() {
        try {
            super.after();
            inventoryDatabase.dropDatabase();
        } catch (Exception e) {
            LOG.error("Failed to clean up resources", e);
        }
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        warehouse = sharedVolume.toString() + "/hudi_warehouse_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();

        LOG.info("Preparing Hudi warehouse directory: {}", warehouse);
        runInContainerAsRoot(jobManager, "mkdir", "-p", warehouse);
        runInContainerAsRoot(jobManager, "chmod", "-R", "0777", warehouse);

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
                                + "  type: hudi\n"
                                + "  path: %s\n"
                                + "  hoodie.datasource.write.recordkey.field: id\n"
                                + "  hoodie.table.type: MERGE_ON_READ\n"
                                + "  hoodie.schema.on.read.enable: true\n"
                                + "  write.bucket_assign.tasks: 2\n"
                                + "  write.tasks: 2\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        Path hudiCdcConnector = TestUtils.getResource("hudi-cdc-pipeline-connector.jar");
        Path hudiHadoopCommonJar = TestUtils.getResource("hudi-hadoop-common.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        Path hadoopCompatibilityJar = TestUtils.getResource("flink-hadoop-compatibility.jar");
        Path dropMetricsJar = TestUtils.getResource("flink-metrics-dropwizard.jar");
        Path flinkParquet = TestUtils.getResource("flink-parquet.jar");
        submitPipelineJob(
                pipelineJob,
                hudiCdcConnector,
                hudiHadoopCommonJar,
                hadoopJar,
                hadoopCompatibilityJar,
                dropMetricsJar,
                flinkParquet);
        waitUntilJobRunning(Duration.ofSeconds(60));
        LOG.info("Pipeline job is running");

        // Validate that source records from RDB have been initialized properly and landed in sink
        validateSinkResult(warehouse, database, "products", getProductsExpectedSinkResults());
        validateSinkResult(warehouse, database, "customers", getCustomersExpectedSinkResults());

        // Generate binlogs
        LOG.info("Begin incremental reading stage.");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database);
        List<String> recordsInIncrementalPhase;
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'Ten','Jukebox',0.2, null, null, null);"); // 110
            stat.execute("UPDATE products SET description='Fay' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.125' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            stat.execute("DELETE FROM products WHERE id=101;");

            stat.execute(
                    "INSERT INTO products VALUES (default,'Eleven','Kryo',5.18, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'Twelve', 'Lily', 2.14, null, null);"); // 112

            validateSinkResult(
                    warehouse, database, "products", getProductsExpectedAfterDropSinkResults());

            recordsInIncrementalPhase = createChangesAndValidate(stat);
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // Build expected results
        List<String> recordsInSnapshotPhase = getProductsExpectedAfterAddModSinkResults();
        recordsInSnapshotPhase.addAll(recordsInIncrementalPhase);

        validateSinkResult(warehouse, database, "products", recordsInSnapshotPhase);
    }

    /**
     * Executes a series of DDL (Data Definition Language) and DML (Data Manipulation Language)
     * operations on the {@code products} table to simulate schema evolution and data loading.
     *
     * <p>The method performs two primary phases:
     *
     * <ol>
     *   <li><b>Column Addition:</b> It sequentially adds 10 new columns, named {@code point_c_0}
     *       through {@code point_c_9}, each with a {@code VARCHAR(10)} type. After each column is
     *       added, it executes a batch of 1000 {@code INSERT} statements, populating the columns
     *       that exist at that point.
     *   <li><b>Column Modification:</b> After all columns are added, it enters a second phase. In
     *       each of the 10 iterations, it first inserts another 1000 rows and then modifies the
     *       data type of the first new column ({@code point_c_0}), progressively increasing its
     *       size from {@code VARCHAR(10)} to {@code VARCHAR(19)}.
     * </ol>
     *
     * Throughout this process, the method constructs and returns a list of strings. Each string
     * represents the expected data for each inserted row in a comma-separated format, which can be
     * used for validation.
     *
     * @param stat The JDBC {@link java.sql.Statement} object used to execute the SQL commands.
     * @return A {@link java.util.List} of strings, where each string is a CSV representation of an
     *     inserted row, reflecting the expected state in the database.
     * @throws java.sql.SQLException if a database access error occurs or the executed SQL is
     *     invalid.
     */
    private List<String> createChangesAndValidate(Statement stat) throws SQLException {
        List<String> result = new ArrayList<>();
        StringBuilder sqlFields = new StringBuilder();

        // Auto-increment id will start from this
        int currentId = 113;
        final int statementBatchCount = 1000;

        // Step 1 - Add Column: Add 10 columns with VARCHAR(10) sequentially
        for (int addColumnRepeat = 0; addColumnRepeat < 10; addColumnRepeat++) {
            String addColAlterTableCmd =
                    String.format(
                            "ALTER TABLE products ADD COLUMN point_c_%s VARCHAR(10);",
                            addColumnRepeat);
            stat.execute(addColAlterTableCmd);
            LOG.info("Executed: {}", addColAlterTableCmd);
            sqlFields.append(", '1'");
            StringBuilder resultFields = new StringBuilder();
            for (int addedFieldCount = 0; addedFieldCount < 10; addedFieldCount++) {
                if (addedFieldCount <= addColumnRepeat) {
                    resultFields.append(", 1");
                } else {
                    resultFields.append(", null");
                }
            }

            for (int statementCount = 0; statementCount < statementBatchCount; statementCount++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));
                result.add(
                        String.format(
                                "%s, finally, null, 2.14, null, null%s", currentId, resultFields));
                currentId++;
            }
            stat.executeBatch();
        }

        // Step 2 - Modify type for the columns added in Step 1, increasing the VARCHAR length
        for (int modifyColumnRepeat = 0; modifyColumnRepeat < 10; modifyColumnRepeat++) {
            // Perform 1000 inserts as a batch, continuing the ID sequence from Step 1
            for (int statementCount = 0; statementCount < statementBatchCount; statementCount++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));

                result.add(
                        String.format(
                                "%s, finally, null, 2.14, null, null%s",
                                currentId, ", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1"));
                // Continue incrementing the counter for each insert
                currentId++;
            }
            stat.executeBatch();

            String modifyColTypeAlterCmd =
                    String.format(
                            "ALTER TABLE products MODIFY point_c_0 VARCHAR(%s);",
                            10 + modifyColumnRepeat);
            stat.execute(modifyColTypeAlterCmd);
            LOG.info("Executed: {}", modifyColTypeAlterCmd);
        }

        return result;
    }

    private List<String> fetchHudiTableRows(String warehouse, String databaseName, String tableName)
            throws Exception {
        String template =
                readLines("docker/" + PEEK_SQL_FILE).stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, databaseName, tableName);
        String containerSqlPath = sharedVolume.toString() + "/" + PEEK_SQL_FILE;
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);
        LOG.info("Executing SQL client in container with Hudi connector and Hadoop JARs");

        // Pass in empty FLINK_ENV_JAVA_OPTS so that we do not launch a new JVM (for SQL
        // submission/parsing) inheriting environment variables which will cause it to bind to the
        // same debug port, causing the port already in use error
        String[] commandToExecute = {
            "bash",
            "-c",
            "FLINK_ENV_JAVA_OPTS='' /opt/flink/bin/sql-client.sh"
                    + " --jar "
                    + FLINK_LIB_DIR
                    + "/"
                    + getHudiSQLConnectorResourceName()
                    + " --jar "
                    + FLINK_LIB_DIR
                    + "/flink-shade-hadoop.jar"
                    + " -f "
                    + containerSqlPath
        };
        LOG.debug("Executing command: {}", String.join(" ", commandToExecute));
        Container.ExecResult result = jobManager.execInContainer(commandToExecute);

        LOG.debug("SQL client execution completed with exit code: {}", result.getExitCode());
        LOG.debug("SQL client stdout: {}", result.getStdout());
        LOG.debug("SQL client stderr: {}", result.getStderr());

        if (result.getExitCode() != 0) {
            LOG.error("SQL client execution failed!");
            LOG.error("Exit code: {}", result.getExitCode());
            LOG.error("Stdout: {}", result.getStdout());
            LOG.error("Stderr: {}", result.getStderr());
            throw new RuntimeException(
                    "Failed to execute Hudi peek script. Exit code: "
                            + result.getExitCode()
                            + ". Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(MySqlToHudiE2eITCase::extractRow)
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

    protected String getHudiSQLConnectorResourceName() {
        return "hudi-sql-connector.jar";
    }

    private void validateSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Hudi {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + HUDI_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchHudiTableRows(warehouse, database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds for {}::{}.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + HUDI_TESTCASE_TIMEOUT.toMillis())
                                / 1000,
                        database,
                        table);
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                // AssertionError contains way too much records and might flood the log output.
                if (expected.size() == results.size()) {
                    // Size of rows match up, print the contents
                    final int rowsToPrint = 100;
                    LOG.warn(
                            "Result expected: {}, but got {}",
                            expected.stream()
                                    .sorted()
                                    .limit(rowsToPrint)
                                    .collect(Collectors.toList()),
                            results.stream()
                                    .sorted()
                                    .limit(rowsToPrint)
                                    .collect(Collectors.toList()));
                } else {
                    LOG.warn(
                            "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                            expected.size(),
                            results.size());
                }
            }

            Thread.sleep(10000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static List<String> getProductsExpectedSinkResults() {
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

    private static List<String> getProductsExpectedAfterDropSinkResults() {
        return Arrays.asList(
                "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}",
                "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}",
                "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}",
                "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}",
                "106, Six, Fay, 9.813, null, null",
                "107, Seven, Grace, 5.125, null, null",
                "108, Eight, Hesse, 6.819, null, null",
                "109, Nine, IINA, 5.223, null, null",
                "110, Ten, Jukebox, 0.2, null, null",
                "111, Eleven, Kryo, 5.18, null, null",
                "112, Twelve, Lily, 2.14, null, null");
    }

    private static List<String> getProductsExpectedAfterAddModSinkResults() {
        // We need this list to be mutable, i.e. not fixed sized
        // Arrays.asList returns a fixed size list which is not mutable
        return new ArrayList<>(
                Arrays.asList(
                        "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}, null, null, null, null, null, null, null, null, null, null",
                        "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}, null, null, null, null, null, null, null, null, null, null",
                        "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}, null, null, null, null, null, null, null, null, null, null",
                        "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null, null, null, null, null, null, null, null, null, null",
                        "106, Six, Fay, 9.813, null, null, null, null, null, null, null, null, null, null, null, null",
                        "107, Seven, Grace, 5.125, null, null, null, null, null, null, null, null, null, null, null, null",
                        "108, Eight, Hesse, 6.819, null, null, null, null, null, null, null, null, null, null, null, null",
                        "109, Nine, IINA, 5.223, null, null, null, null, null, null, null, null, null, null, null, null",
                        "110, Ten, Jukebox, 0.2, null, null, null, null, null, null, null, null, null, null, null, null",
                        "111, Eleven, Kryo, 5.18, null, null, null, null, null, null, null, null, null, null, null, null",
                        "112, Twelve, Lily, 2.14, null, null, null, null, null, null, null, null, null, null, null, null"));
    }

    private static List<String> getCustomersExpectedSinkResults() {
        return Arrays.asList(
                "101, user_1, Shanghai, 123567891234",
                "102, user_2, Shanghai, 123567891234",
                "103, user_3, Shanghai, 123567891234",
                "104, user_4, Shanghai, 123567891234");
    }
}
