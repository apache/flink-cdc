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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.cdc.pipeline.tests.utils.TarballFetcher;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;

import org.apache.hudi.common.model.HoodieTableType;
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
import org.testcontainers.containers.GenericContainer;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for mysql cdc to Iceberg pipeline job. */
public class MySqlToHudiE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToHudiE2eITCase.class);

    private static final Duration HUDI_TESTCASE_TIMEOUT = Duration.ofMinutes(20);

    private static final String FLINK_LIB_DIR = "/opt/flink/lib";

    private static final String PEEK_SQL_FILE = "peek-hudi.sql";

    private static final String TABLE_TYPE = HoodieTableType.MERGE_ON_READ.name();

    // Custom Flink properties for Hudi tests with increased metaspace and heap for heavy
    // dependencies
    private static final String HUDI_FLINK_PROPERTIES =
            FLINK_PROPERTIES
                    + "\n"
                    + "taskmanager.memory.jvm-metaspace.size: 512M"
                    + "\n"
                    + "taskmanager.memory.task.heap.size: 1024M"
                    + "\n"
                    + "taskmanager.memory.process.size: 4GB";

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "hudi_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private String warehouse;

    private final boolean debug = false;

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
     * <p>
     * By copying these dependencies directly into the container's `/opt/flink/lib`
     * directory, we ensure they are loaded by Flink's main classloader, which
     * permanently resolves the `No FileSystem for scheme: file` error during validation.
     */
    @BeforeEach
    @Override
    public void before() throws Exception {
        LOG.info("Starting containers...");

        // 2. Instantiate the correct class and apply class-specific methods
        if (debug) {
            // Use FixedHost instead of GenericContainer to ensure that ports are fixed for easier
            // debugging during dev
            jobManager =
                    new FixedHostPortGenericContainer<>(getFlinkDockerImageTag())
                            .withFixedExposedPort(8081, JOB_MANAGER_REST_PORT)
                            .withFixedExposedPort(9005, 9005)
                            .withEnv(
                                    "FLINK_ENV_JAVA_OPTS",
                                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:9005");
            taskManager =
                    new FixedHostPortGenericContainer<>(getFlinkDockerImageTag())
                            .withFixedExposedPort(9006, 9006)
                            .withEnv(
                                    "FLINK_ENV_JAVA_OPTS",
                                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:9006");
        } else {
            jobManager =
                    new GenericContainer<>(getFlinkDockerImageTag())
                            // Expose ports for random mapping by Docker
                            .withExposedPorts(JOB_MANAGER_REST_PORT);
            taskManager = new FixedHostPortGenericContainer<>(getFlinkDockerImageTag());
        }

        jobManagerConsumer = new ToStringConsumer();
        jobManager
                .withCommand("jobmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(sharedVolume))
                .withLogConsumer(jobManagerConsumer);
        Startables.deepStart(Stream.of(jobManager)).join();
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", sharedVolume.toString());

        taskManagerConsumer = new ToStringConsumer();
        taskManager
                .withCommand("taskmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                .withEnv("FLINK_PROPERTIES", HUDI_FLINK_PROPERTIES)
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
                                + "  hoodie.table.type: "
                                + TABLE_TYPE
                                + " \n"
                                + "  write.bucket_assign.tasks: 2\n"
                                + "  write.tasks: 2\n"
                                + "  compaction.delta_commits: 2\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        database,
                        warehouse,
                        parallelism);
        Path hudiCdcConnector = TestUtils.getResource("hudi-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        Path hadoopCompatibilityJar = TestUtils.getResource("flink-hadoop-compatibility.jar");
        Path dropMetricsJar = TestUtils.getResource("flink-metrics-dropwizard.jar");
        Path flinkParquet = TestUtils.getResource("flink-parquet.jar");
        JobID pipelineJobID =
                submitPipelineJob(
                        pipelineJob,
                        hudiCdcConnector,
                        hadoopJar,
                        hadoopCompatibilityJar,
                        dropMetricsJar,
                        flinkParquet);
        waitUntilJobRunning(pipelineJobID, Duration.ofSeconds(60));
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

        // Verify that compaction was scheduled for at least one table (only for MOR tables)
        LOG.info("Verifying compaction scheduling for MOR tables...");
        if (TABLE_TYPE.equals(HoodieTableType.MERGE_ON_READ.name())) {
            assertCompactionScheduled(warehouse, database, Arrays.asList("products", "customers"));
        }
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
     * <p>Throughout this process, the method constructs and returns a list of strings. Each string
     * represents the expected data for each inserted row in a comma-separated format, which can be
     * used for validation.
     *
     * @param stat The JDBC {@link Statement} object used to execute the SQL commands.
     * @return A {@link List} of strings, where each string is a CSV representation of an inserted
     *     row, reflecting the expected state in the database.
     * @throws SQLException if a database access error occurs or the executed SQL is invalid.
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

    @Override
    public String stopJobWithSavepoint(JobID jobID) {
        String savepointPath = "/opt/flink/";
        try {
            // Use REST API to stop with savepoint to avoid CLI classpath conflicts
            // (Hadoop/Hudi JARs in FLINK_LIB_DIR conflict with Flink's commons-cli)
            LOG.info("Stopping job {} with savepoint to {}", jobID, savepointPath);

            String savepointLocation =
                    getRestClusterClient()
                            .stopWithSavepoint(
                                    jobID, false, savepointPath, SavepointFormatType.CANONICAL)
                            .get(60, java.util.concurrent.TimeUnit.SECONDS);

            LOG.info("Savepoint completed at: {}", savepointLocation);
            return savepointLocation;
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop job with savepoint", e);
        }
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

    @Test
    public void testStopAndRestartFromSavepoint() throws Exception {
        warehouse = sharedVolume.toString() + "/hudi_warehouse_savepoint_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();

        LOG.info("Preparing Hudi warehouse directory: {}", warehouse);
        runInContainerAsRoot(jobManager, "mkdir", "-p", warehouse);
        runInContainerAsRoot(jobManager, "chmod", "-R", "0777", warehouse);

        // Configure pipeline with checkpointing
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5600-5604\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: hudi\n"
                                + "  path: %s\n"
                                + "  hoodie.table.type: MERGE_ON_READ\n"
                                + "  write.bucket_assign.tasks: 2\n"
                                + "  write.tasks: 2\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s\n"
                                + "\n",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);

        Path hudiCdcConnector = TestUtils.getResource("hudi-cdc-pipeline-connector.jar");
        Path hudiHadoopCommonJar = TestUtils.getResource("hudi-hadoop-common.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        Path hadoopCompatibilityJar = TestUtils.getResource("flink-hadoop-compatibility.jar");
        Path dropMetricsJar = TestUtils.getResource("flink-metrics-dropwizard.jar");
        Path flinkParquet = TestUtils.getResource("flink-parquet.jar");

        // Start the pipeline job
        LOG.info("Phase 1: Starting initial pipeline job");
        JobID pipelineJobID1 =
                submitPipelineJob(
                        pipelineJob,
                        hudiCdcConnector,
                        hudiHadoopCommonJar,
                        hadoopJar,
                        hadoopCompatibilityJar,
                        dropMetricsJar,
                        flinkParquet);
        waitUntilJobRunning(pipelineJobID1, Duration.ofSeconds(60));

        // Store the jobID of the submitted job, we will need it for stopping the job later
        Collection<JobStatusMessage> jobs =
                getRestClusterClient().listJobs().get(10, TimeUnit.SECONDS);
        Assertions.assertThat(jobs).hasSize(1);
        JobStatusMessage pipelineJobMessage = jobs.iterator().next();
        LOG.info(
                "Pipeline job: ID={}, Name={}, Status={}",
                pipelineJobMessage.getJobId(),
                pipelineJobMessage.getJobName(),
                pipelineJobMessage.getJobState());

        // Validate initial snapshot data for both tables
        validateSinkResult(warehouse, database, "products", getProductsExpectedSinkResults());
        validateSinkResult(warehouse, database, "customers", getCustomersExpectedSinkResults());
        LOG.info("Phase 1: Initial snapshot validated successfully");

        // Phase 2: Insert incremental data before stopping
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'Pre-Stop Product','Description',1.23, null, null, null);");
            stat.execute(
                    "INSERT INTO customers VALUES (105, 'user_pre_stop', 'Beijing', '987654321');");
            LOG.info("Phase 2: Incremental data inserted before stop");

            // Wait for data to be checkpointed
            Thread.sleep(5000);
        }

        // Validate data before stopping
        List<String> expectedProductsBeforeStop = new ArrayList<>(getProductsExpectedSinkResults());
        expectedProductsBeforeStop.add(
                "110, Pre-Stop Product, Description, 1.23, null, null, null");

        List<String> expectedCustomersBeforeStop =
                new ArrayList<>(getCustomersExpectedSinkResults());
        expectedCustomersBeforeStop.add("105, user_pre_stop, Beijing, 987654321");

        validateSinkResult(warehouse, database, "products", expectedProductsBeforeStop);
        validateSinkResult(warehouse, database, "customers", expectedCustomersBeforeStop);
        LOG.info("Phase 2: Data validated before stop");

        // Phase 3: Stop job with savepoint
        LOG.info("Phase 3: Stopping job with savepoint");
        Collection<JobStatusMessage> runningJobs =
                getRestClusterClient().listJobs().get(10, TimeUnit.SECONDS).stream()
                        .filter(j -> j.getJobState().equals(JobStatus.RUNNING))
                        .collect(Collectors.toList());

        if (runningJobs.isEmpty()) {
            throw new RuntimeException("No running jobs found!");
        }

        String savepointPath = stopJobWithSavepoint(pipelineJobMessage.getJobId());
        LOG.info("Job stopped with savepoint at: {}", savepointPath);

        // Phase 4: Restart from savepoint
        LOG.info("Phase 4: Restarting job from savepoint");
        JobID pipelineJobID2 =
                submitPipelineJob(
                        pipelineJob,
                        savepointPath,
                        false,
                        hudiCdcConnector,
                        hudiHadoopCommonJar,
                        hadoopJar,
                        hadoopCompatibilityJar,
                        dropMetricsJar,
                        flinkParquet);
        waitUntilJobRunning(pipelineJobID2, Duration.ofSeconds(60));
        LOG.info("Job restarted from savepoint");

        // Wait for Hudi to stabilize after restart
        Thread.sleep(5000);

        // Validate data after restart - should be the same as before stop
        validateSinkResult(warehouse, database, "products", expectedProductsBeforeStop);
        validateSinkResult(warehouse, database, "customers", expectedCustomersBeforeStop);
        LOG.info("Phase 4: Data consistency validated after restart from savepoint");

        // Phase 5: Continue with post-restart data to ensure pipeline still works
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'Post-Restart Product','New Description',4.56, null, null, null);");
            stat.execute("UPDATE products SET description='Updated Description' WHERE id=110;");
            stat.execute(
                    "INSERT INTO customers VALUES (106, 'user_post_restart', 'Guangzhou', '111222333');");
            stat.execute("DELETE FROM customers WHERE id=101;");
            LOG.info("Phase 5: Post-restart data changes applied");
        }

        // Phase 6: Final validation
        List<String> expectedProductsFinal = new ArrayList<>(expectedProductsBeforeStop);
        // Update the pre-stop product description
        expectedProductsFinal.removeIf(row -> row.startsWith("110,"));
        expectedProductsFinal.add(
                "110, Pre-Stop Product, Updated Description, 1.23, null, null, null");
        expectedProductsFinal.add(
                "111, Post-Restart Product, New Description, 4.56, null, null, null");

        List<String> expectedCustomersFinal = new ArrayList<>(expectedCustomersBeforeStop);
        // Remove deleted customer
        expectedCustomersFinal.removeIf(row -> row.startsWith("101,"));
        expectedCustomersFinal.add("106, user_post_restart, Guangzhou, 111222333");

        validateSinkResult(warehouse, database, "products", expectedProductsFinal);
        validateSinkResult(warehouse, database, "customers", expectedCustomersFinal);
        LOG.info(
                "Phase 6: Final validation successful - stop/restart with savepoint working correctly for multiple tables");
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

    public void waitUntilJobRunning(JobID jobId, Duration timeout) {
        waitUntilJobState(jobId, timeout, JobStatus.RUNNING);
    }

    public void waitUntilJobFinished(JobID jobId, Duration timeout) {
        waitUntilJobState(jobId, timeout, JobStatus.FINISHED);
    }

    public void waitUntilJobState(JobID jobId, Duration timeout, JobStatus expectedStatus) {
        RestClusterClient<?> clusterClient = getRestClusterClient();
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            Collection<JobStatusMessage> jobStatusMessages;
            try {
                jobStatusMessages = clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn("Error when fetching job status.", e);
                continue;
            }

            if (jobStatusMessages == null || jobStatusMessages.isEmpty()) {
                continue;
            }

            Optional<JobStatusMessage> optMessage =
                    jobStatusMessages.stream().filter(j -> j.getJobId().equals(jobId)).findFirst();

            if (!optMessage.isPresent()) {
                LOG.warn("Job: {} not found, waiting for the next loop...", jobId);
                continue;
            }

            JobStatusMessage message = optMessage.get();
            JobStatus jobStatus = message.getJobState();
            if (!expectedStatus.isTerminalState() && jobStatus.isTerminalState()) {
                throw new ValidationException(
                        String.format(
                                "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                message.getJobName(), message.getJobId(), message.getJobState()));
            } else if (jobStatus == expectedStatus) {
                return;
            }
        }
    }

    /**
     * Asserts that compaction was scheduled for the given tables by checking for
     * .compaction.requested files in the Hudi timeline directory inside the container.
     *
     * <p>Should only be invoked for MERGE_ON_READ tables.
     *
     * @param warehouse The warehouse directory path
     * @param database The database name
     * @param tables List of table names to check
     */
    private void assertCompactionScheduled(String warehouse, String database, List<String> tables)
            throws Exception {
        boolean compactionFound = false;
        StringBuilder debugInfo = new StringBuilder();

        for (String table : tables) {
            // This will exclude metadata table timeline results
            String timelinePath =
                    String.format("%s/%s/%s/.hoodie/timeline", warehouse, database, table);
            debugInfo.append(
                    String.format(
                            "\nChecking timeline for %s.%s at: %s", database, table, timelinePath));

            // Check if timeline directory exists in container
            Container.ExecResult lsResult = jobManager.execInContainer("ls", "-la", timelinePath);
            if (lsResult.getExitCode() != 0) {
                debugInfo.append(
                        String.format(
                                " - Timeline directory does not exist or cannot be accessed: %s",
                                lsResult.getStderr()));
                continue;
            }

            // Find .compaction.requested files
            Container.ExecResult findResult =
                    jobManager.execInContainer(
                            "find", timelinePath, "-name", "*.compaction.requested");

            if (findResult.getExitCode() == 0 && !findResult.getStdout().trim().isEmpty()) {
                compactionFound = true;
                String[] compactionFiles = findResult.getStdout().trim().split("\n");
                debugInfo.append(
                        String.format(
                                " - Found %d compaction file(s): %s",
                                compactionFiles.length, Arrays.toString(compactionFiles)));
                LOG.info(
                        "Compaction scheduled for table {}.{}: {}",
                        database,
                        table,
                        Arrays.toString(compactionFiles));
            } else {
                debugInfo.append(" - No compaction.requested files found");

                // List all timeline files for debugging
                Container.ExecResult allFilesResult =
                        jobManager.execInContainer("ls", "-1", timelinePath);
                if (allFilesResult.getExitCode() == 0) {
                    debugInfo.append(
                            String.format(
                                    "\n    All timeline files: %s",
                                    allFilesResult.getStdout().replace("\n", ", ")));
                }
            }
        }

        if (!compactionFound) {
            LOG.error("Compaction verification failed. Debug info:{}", debugInfo);
            Assertions.fail(
                    "No compaction.requested files found in any table timeline. "
                            + "Expected at least one compaction to be scheduled."
                            + debugInfo);
        } else {
            LOG.info("Compaction verification successful!");
        }
    }
}
