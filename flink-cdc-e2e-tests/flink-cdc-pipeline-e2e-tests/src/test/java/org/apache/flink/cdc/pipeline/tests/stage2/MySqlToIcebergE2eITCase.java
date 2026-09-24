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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.cdc.pipeline.tests.utils.TarballFetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** End-to-end tests for mysql cdc to Iceberg pipeline job. */
@ParameterizedClass
@ValueSource(ints = {1, 4})
public class MySqlToIcebergE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToIcebergE2eITCase.class);

    @TempDir public Path temporaryFolder;

    public MySqlToIcebergE2eITCase(int parallelism) {
        super(parallelism);
    }

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "iceberg_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private String warehouse;

    @BeforeAll
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        LOG.info("Starting containers...");
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        warehouse =
                Files.createDirectory(temporaryFolder.resolve(UUID.randomUUID().toString()), attr)
                        .toString();
        jobManagerConsumer = new ToStringConsumer();
        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(sharedVolume))
                        .withFileSystemBind(warehouse, warehouse, BindMode.READ_WRITE)
                        .withLogConsumer(jobManagerConsumer);
        Startables.deepStart(Stream.of(jobManager)).join();
        LOG.info("JobManager is started.");
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", sharedVolume.toString());
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", warehouse);

        taskManagerConsumer = new ToStringConsumer();
        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .dependsOn(jobManager)
                        .withVolumesFrom(jobManager, BindMode.READ_WRITE)
                        .withFileSystemBind(warehouse, warehouse, BindMode.READ_WRITE)
                        .withLogConsumer(taskManagerConsumer);
        Startables.deepStart(Stream.of(taskManager)).join();
        LOG.info("TaskManager is started.");
        runInContainerAsRoot(taskManager, "chmod", "0777", "-R", sharedVolume.toString());
        runInContainerAsRoot(taskManager, "chmod", "0777", "-R", warehouse);
        inventoryDatabase.createAndInitialize();

        TarballFetcher.fetchLatest(jobManager);
        LOG.info("CDC executables deployed.");
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
        String database = inventoryDatabase.getDatabaseName();
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
                                + "  type: iceberg\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.type: hadoop\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %s",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse, parallelism);
        JobID jobId = submitIcebergPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(60));
        LOG.info("Pipeline job is running");
        validateSinkResult(
                warehouse,
                database,
                "products",
                Arrays.asList(
                        "101, One, Alice, 3.202, red, {\"key1\": \"value1\"}, null",
                        "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}, null",
                        "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}, null",
                        "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}, null",
                        "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null",
                        "106, Six, Ferris, 9.813, null, null, null",
                        "107, Seven, Grace, 2.117, null, null, null",
                        "108, Eight, Hesse, 6.819, null, null, null",
                        "109, Nine, IINA, 5.223, null, null, null"));

        validateSinkResult(
                warehouse,
                database,
                "customers",
                Arrays.asList(
                        "101, user_1, Shanghai, 123567891234",
                        "102, user_2, Shanghai, 123567891234",
                        "103, user_3, Shanghai, 123567891234",
                        "104, user_4, Shanghai, 123567891234"));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
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

            // Same row updated twice before the schema-change flush below.
            stat.execute("UPDATE products SET description='Bob v1' WHERE id=102;");
            stat.execute("UPDATE products SET description='Bob v2' WHERE id=102;");

            // modify table schema
            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            stat.execute("DELETE FROM products WHERE id=101;");

            // And once more after the flush; the latest value should win, no duplicate.
            stat.execute("UPDATE products SET weight='1.125' WHERE id=102;");
            // Update then delete the same row; it should be gone.
            stat.execute("UPDATE products SET description='Cecily v2' WHERE id=103;");
            stat.execute("DELETE FROM products WHERE id=103;");
            // Delete then re-insert the same id; the re-inserted row should survive.
            stat.execute("DELETE FROM products WHERE id=104;");
            stat.execute(
                    "INSERT INTO products (id, name, description, weight, enum_c, json_c) "
                            + "VALUES (104, 'Four', 'Reborn', 9.875, 'white', null);");

            stat.execute(
                    "INSERT INTO products VALUES (default,'Eleven','Kryo',5.18, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'Twelve', 'Lily', 2.14, null, null);"); // 112

            validateSinkResult(
                    warehouse,
                    database,
                    "products",
                    Arrays.asList(
                            "102, Two, Bob v2, 1.125, white, {\"key2\":\"value2\"}",
                            "104, Four, Reborn, 9.875, white, null",
                            "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}",
                            "106, Six, Fay, 9.813, null, null",
                            "107, Seven, Grace, 5.125, null, null",
                            "108, Eight, Hesse, 6.819, null, null",
                            "109, Nine, IINA, 5.223, null, null",
                            "110, Ten, Jukebox, 0.2, null, null",
                            "111, Eleven, Kryo, 5.18, null, null",
                            "112, Twelve, Lily, 2.14, null, null"));

            recordsInIncrementalPhase = createChangesAndValidate(stat);
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
        List<String> recordsInSnapshotPhase =
                new ArrayList<>(
                        Arrays.asList(
                                "102, Two, Bob v2, 1.125, white, {\"key2\":\"value2\"}, null, null, null, null, null, null, null, null, null, null",
                                "104, Four, Reborn, 9.875, white, null, null, null, null, null, null, null, null, null, null, null",
                                "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null, null, null, null, null, null, null, null, null, null",
                                "106, Six, Fay, 9.813, null, null, null, null, null, null, null, null, null, null, null, null",
                                "107, Seven, Grace, 5.125, null, null, null, null, null, null, null, null, null, null, null, null",
                                "108, Eight, Hesse, 6.819, null, null, null, null, null, null, null, null, null, null, null, null",
                                "109, Nine, IINA, 5.223, null, null, null, null, null, null, null, null, null, null, null, null",
                                "110, Ten, Jukebox, 0.2, null, null, null, null, null, null, null, null, null, null, null, null",
                                "111, Eleven, Kryo, 5.18, null, null, null, null, null, null, null, null, null, null, null, null",
                                "112, Twelve, Lily, 2.14, null, null, null, null, null, null, null, null, null, null, null, null"));
        recordsInSnapshotPhase.addAll(recordsInIncrementalPhase);
        recordsInSnapshotPhase =
                recordsInSnapshotPhase.stream().sorted().collect(Collectors.toList());
        validateSinkResult(warehouse, database, "products", recordsInSnapshotPhase);
    }

    @Test
    @Timeout(600)
    void testMaintainsAutomaticallyDiscoveredAndRoutedTables() throws Exception {
        String database = inventoryDatabase.getDatabaseName();
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database);
        TableIdentifier target = TableIdentifier.of("maintenance", "orders");
        try (Connection connection =
                        DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement statement = connection.createStatement();
                HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
            statement.execute(
                    "CREATE TABLE maintenance_orders (id INT PRIMARY KEY, name VARCHAR(100))");
            statement.execute("CREATE TABLE maintenance_ignored LIKE maintenance_orders");
            statement.execute("INSERT INTO maintenance_orders VALUES (1, 'row-1')");
            statement.execute("INSERT INTO maintenance_ignored VALUES (999, 'ignored')");
            assertThat(catalog.tableExists(target)).isFalse();

            String pipelineJob =
                    String.format(
                            "source:\n"
                                    + "  type: mysql\n"
                                    + "  hostname: mysql\n"
                                    + "  port: 3306\n"
                                    + "  username: %s\n"
                                    + "  password: %s\n"
                                    + "  tables: %s.maintenance_\\.*\n"
                                    + "  tables.exclude: %s.maintenance_ignored\n"
                                    + "  server-id: 5400-5404\n"
                                    + "  server-time-zone: UTC\n"
                                    + "\n"
                                    + "sink:\n"
                                    + "  type: iceberg\n"
                                    + "  catalog.properties.type: hadoop\n"
                                    + "  catalog.properties.warehouse: %s\n"
                                    + "  sink.maintenance.enabled: true\n"
                                    + "  sink.maintenance.parallelism: 1\n"
                                    + "  sink.maintenance.rate-limit: 1 s\n"
                                    + "  sink.maintenance.lock-check-delay: 1 s\n"
                                    + "  sink.maintenance.lock.jdbc.uri: 'jdbc:mysql://mysql:3306/%s?useSSL=false&allowPublicKeyRetrieval=true'\n"
                                    + "  sink.maintenance.lock.jdbc.properties.user: %s\n"
                                    + "  sink.maintenance.lock.jdbc.properties.password: %s\n"
                                    + "  sink.maintenance.lock.jdbc.init-lock-tables: true\n"
                                    + "  sink.maintenance.rewrite-data-files.enabled: true\n"
                                    + "  sink.maintenance.rewrite-data-files.interval: 2 s\n"
                                    + "  sink.maintenance.rewrite-data-files.min-input-files: 2\n"
                                    + "  sink.maintenance.rewrite-data-files.delete-file-threshold: 1\n"
                                    + "  sink.maintenance.expire-snapshots.enabled: true\n"
                                    + "  sink.maintenance.expire-snapshots.interval: 2 s\n"
                                    + "  sink.maintenance.expire-snapshots.max-age: 30 s\n"
                                    + "  sink.maintenance.expire-snapshots.retain-last: 2\n"
                                    + "  sink.maintenance.delete-orphan-files.enabled: true\n"
                                    + "  sink.maintenance.delete-orphan-files.interval: 2 s\n"
                                    + "  sink.maintenance.delete-orphan-files.min-age: 3 d\n"
                                    + "\n"
                                    + "route:\n"
                                    + "  - source-table: %s.maintenance_orders\n"
                                    + "    sink-table: maintenance.orders\n"
                                    + "\n"
                                    + "pipeline:\n"
                                    + "  parallelism: %s\n",
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            database,
                            database,
                            warehouse,
                            database,
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            database,
                            parallelism);
            JobID jobId = submitIcebergPipelineJob(pipelineJob);
            assertThat(jobId).isNotNull();
            waitUntilJobRunning(STARTUP_WAITING_TIMEOUT);
            await().atMost(EVENT_WAITING_TIMEOUT)
                    .untilAsserted(
                            () -> {
                                assertMaintenanceJobRunning(jobId);
                                assertThat(catalog.tableExists(target)).isTrue();
                            });
            Table table = catalog.loadTable(target);
            List<String> expected = new ArrayList<>();
            expected.add("1:row-1");
            awaitMaintenanceRows(jobId, table, expected);
            long firstSnapshot = table.currentSnapshot().snapshotId();

            // Wait for each commit so CDC produces multiple files for maintenance to combine.
            for (int id = 2; id <= 6; id++) {
                statement.execute(
                        String.format(
                                "INSERT INTO maintenance_orders VALUES (%s, 'row-%s')", id, id));
                expected.add(id + ":row-" + id);
                awaitMaintenanceRows(jobId, table, expected);
            }

            runInContainerAsRoot(jobManager, "chmod", "0777", "-R", warehouse);
            Path oldOrphan =
                    Path.of(warehouse, "maintenance", "orders", "data", "old-orphan.parquet");
            Path newOrphan = oldOrphan.resolveSibling("new-orphan.parquet");
            Files.write(oldOrphan, new byte[] {1});
            Files.write(newOrphan, new byte[] {2});
            Files.setLastModifiedTime(
                    oldOrphan, FileTime.from(Instant.now().minus(Duration.ofDays(10))));

            await().atMost(EVENT_WAITING_TIMEOUT)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(
                            () -> {
                                assertMaintenanceJobRunning(jobId);
                                table.refresh();
                                assertThat(table.currentSnapshot().operation())
                                        .isEqualTo("replace");
                                assertThat(table.snapshot(firstSnapshot)).isNull();
                                assertThat(table.snapshots()).hasSize(2);
                                try (CloseableIterable<FileScanTask> files =
                                        table.newScan().planFiles()) {
                                    assertThat(files).hasSize(1);
                                }
                                assertThat(oldOrphan).doesNotExist();
                                assertThat(newOrphan).exists();
                                assertMaintenanceRows(table, expected);
                            });
            long compactedSnapshot = table.currentSnapshot().snapshotId();

            statement.execute("UPDATE maintenance_orders SET name = 'updated' WHERE id = 1");
            statement.execute("DELETE FROM maintenance_orders WHERE id = 2");
            statement.execute("INSERT INTO maintenance_orders VALUES (7, 'later')");
            expected.remove("1:row-1");
            expected.remove("2:row-2");
            expected.add("1:updated");
            expected.add("7:later");
            awaitMaintenanceRows(jobId, table, expected);
            await().atMost(EVENT_WAITING_TIMEOUT)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(
                            () -> {
                                assertMaintenanceJobRunning(jobId);
                                table.refresh();
                                assertThat(table.currentSnapshot().snapshotId())
                                        .isNotEqualTo(compactedSnapshot);
                                assertThat(table.currentSnapshot().operation())
                                        .isEqualTo("replace");
                                assertMaintenanceRows(table, expected);
                                assertThat(newOrphan).exists();
                            });
            assertThat(catalog.tableExists(TableIdentifier.of(database, "maintenance_orders")))
                    .isFalse();
            assertThat(catalog.tableExists(TableIdentifier.of(database, "maintenance_ignored")))
                    .isFalse();
            triggerCheckpointWithRetry(jobId);
            assertMaintenanceJobRunning(jobId);
        }
    }

    private JobID submitIcebergPipelineJob(String pipelineJob) throws Exception {
        return submitPipelineJob(
                pipelineJob,
                TestUtils.getResource("mysql-cdc-pipeline-connector.jar"),
                TestUtils.getResource("iceberg-cdc-pipeline-connector.jar"),
                TestUtils.getResource("hadoop-client-api.jar"),
                TestUtils.getResource("hadoop-client-runtime.jar"),
                TestUtils.getResource("commons-logging.jar"));
    }

    private void awaitMaintenanceRows(JobID jobId, Table table, List<String> expected) {
        await().atMost(EVENT_WAITING_TIMEOUT)
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(
                        () -> {
                            assertMaintenanceJobRunning(jobId);
                            table.refresh();
                            assertMaintenanceRows(table, expected);
                        });
    }

    private void assertMaintenanceJobRunning(JobID jobId) throws Exception {
        JobStatus status = getRestClusterClient().getJobStatus(jobId).get(10, TimeUnit.SECONDS);
        if (status.isGloballyTerminalState()) {
            throw new IllegalStateException(
                    "Maintenance pipeline terminated: " + status,
                    getRestClusterClient()
                            .requestJobResult(jobId)
                            .get(10, TimeUnit.SECONDS)
                            .getSerializedThrowable()
                            .map(error -> error.deserializeError(getClass().getClassLoader()))
                            .orElse(null));
        }
        assertThat(status).isEqualTo(JobStatus.RUNNING);
    }

    private static void assertMaintenanceRows(Table table, List<String> expected) throws Exception {
        List<String> actual = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                actual.add(record.getField("id") + ":" + record.getField("name"));
            }
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * Basic Schema: id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, description VARCHAR(512),
     * weight FLOAT, enum_c enum('red', 'white'), json_c JSON.
     */
    private List<String> createChangesAndValidate(Statement stat) throws SQLException {
        List<String> result = new ArrayList<>();
        StringBuilder sqlFields = new StringBuilder();

        // Add Column.
        for (int addColumnRepeat = 0; addColumnRepeat < 10; addColumnRepeat++) {
            stat.execute(
                    String.format(
                            "ALTER TABLE products ADD COLUMN point_c_%s VARCHAR(10);",
                            addColumnRepeat));
            sqlFields.append(", '1'");
            StringBuilder resultFields = new StringBuilder();
            for (int addedFieldCount = 0; addedFieldCount < 10; addedFieldCount++) {
                if (addedFieldCount <= addColumnRepeat) {
                    resultFields.append(", 1");
                } else {
                    resultFields.append(", null");
                }
            }
            for (int statementCount = 0; statementCount < 1000; statementCount++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));
                int id = addColumnRepeat * 1000 + statementCount + 113;
                result.add(
                        String.format("%s, finally, null, 2.14, null, null%s", id, resultFields));
            }
            stat.executeBatch();
        }

        // Modify Column type.
        for (int modifyColumnRepeat = 0; modifyColumnRepeat < 10; modifyColumnRepeat++) {
            for (int statementCount = 0; statementCount < 1000; statementCount++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));
                int id = modifyColumnRepeat * 1000 + statementCount + 10113;
                result.add(
                        String.format(
                                "%s, finally, null, 2.14, null, null%s",
                                id, ", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1"));
            }
            stat.executeBatch();
            stat.execute(
                    String.format(
                            "ALTER TABLE products MODIFY point_c_0 VARCHAR(%s);",
                            10 + modifyColumnRepeat));
        }

        return result;
    }

    private List<String> fetchIcebergTableRows(
            String warehouse, String databaseName, String tableName) throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        List<String> results = new ArrayList<>();
        Table table = catalog.loadTable(TableIdentifier.of(databaseName, tableName));
        org.apache.iceberg.Schema schema = table.schema();
        CloseableIterable<Record> records = IcebergGenerics.read(table).project(schema).build();
        for (Record record : records) {
            List<String> fieldValues = new ArrayList<>();
            for (Types.NestedField field : schema.columns()) {
                String fieldValue = Objects.toString(record.getField(field.name()), "null");
                fieldValues.add(fieldValue);
            }
            String joinedString = String.join(", ", fieldValues);
            results.add(joinedString);
        }
        return results;
    }

    private void validateSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", warehouse);
        LOG.info("Verifying Iceberg {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        List<String> expectedResults = expected.stream().sorted().collect(Collectors.toList());
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchIcebergTableRows(warehouse, database, table);
                results = results.stream().sorted().collect(Collectors.toList());
                Assertions.assertThat(results).hasSameSizeAs(expectedResults);
                for (int recordIndex = 0; recordIndex < expectedResults.size(); recordIndex++) {
                    Assertions.assertThat(results.get(recordIndex))
                            .isEqualTo(expectedResults.get(recordIndex));
                }
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + EVENT_WAITING_TIMEOUT.toMillis())
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
            Thread.sleep(10000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
    }
}
