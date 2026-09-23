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

package org.apache.flink.cdc.connectors.fluss;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR;
import static org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior.IGNORE;
import static org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior.LENIENT;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectBatchRows;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for Fluss Pipeline. */
public class FlussPipelineITCase {
    private static final int MAX_PARALLELISM = 4;
    private static final Duration RESULT_TIMEOUT = Duration.ofMinutes(5);

    // Always use parent-first classloader for CDC classes.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    // The pipeline job keeps running (unbounded streaming) and permanently occupies
    // MAX_PARALLELISM slots, so reserve extra slots (matching the batch query's default
    // parallelism) so that checkResult's LIMIT query jobs can still be scheduled concurrently.
    private static final int QUERY_PARALLELISM = 2;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM + QUERY_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    /**
     * Use {@link FlussClusterExtension} to start a Fluss cluster with sasl authentication for every
     * test case.
     */
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setNumOfTabletServers(3)
                    .build();

    static final String CATALOG_NAME = "test_catalog";
    static final String SOURCE_DB = "source_db";
    static final String SINK_DB = "sink_db";
    static final String TABLE_1 = "table1";
    static final String TABLE_2 = "table2";

    protected TableEnvironment tBatchEnv;
    private MiniCluster miniCluster;

    @BeforeEach
    void before(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        this.miniCluster = miniCluster;
        waitForFlussClusterReady();
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tBatchEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        QUERY_PARALLELISM);
        tBatchEnv.executeSql("create database " + SOURCE_DB).await();
        tBatchEnv.executeSql("create database " + SINK_DB).await();
    }

    private void waitForFlussClusterReady() throws Exception {
        int maxRetries = 30;
        int retryIntervalMs = 1000;
        Exception lastException = null;

        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection =
                    ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
                return;
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(retryIntervalMs);
            }
        }

        throw new IllegalStateException(
                "Failed to connect to Fluss cluster after " + maxRetries + " attempts",
                lastException);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("drop database %s cascade", SOURCE_DB));
        tBatchEnv.executeSql(String.format("drop database %s cascade", SINK_DB));
    }

    @Test
    void testSinglePrimaryTable() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING, PRIMARY KEY (col1) NOT ENFORCED");
        insertSourceRows(TABLE_1, "('1', 'a'), ('2', 'b'), ('3', 'c')");
        insertSourceRows(TABLE_1, "('2', 'b2')");
        deleteSourceRows(TABLE_1, "col1 = '1'");
        Thread.sleep(3000L);

        composeAndCheckInLenientMode(TABLE_1, Arrays.asList("+I[2, b2]", "+I[3, c]"));
    }

    @Test
    void testSingleLogTable() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING");
        insertSourceRows(TABLE_1, "('1', 'a'), ('2', 'b'), ('3', 'c')");

        composeAndCheckInLenientMode(TABLE_1, Arrays.asList("+I[1, a]", "+I[2, b]", "+I[3, c]"));
    }

    @Test
    void testSingleLogTableWithAddColumn() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING");
        insertSourceRows(TABLE_1, "('1', 'a')");

        PipelineExecution.ExecutionInfo executionInfo = composeAndExecuteInLenientMode(TABLE_1);
        try {
            checkResult(TABLE_1, Collections.singletonList("+I[1, a]"));
            alterSourceTable(TABLE_1, "ADD newColumn STRING");
            insertSourceRows(TABLE_1, "('2', 'b', 'bb')");
            alterSourceTable(TABLE_1, "ADD newColumn2 STRING");
            insertSourceRows(TABLE_1, "('3', 'c', 'cc', 'ccc')");
            checkResult(
                    TABLE_1,
                    Arrays.asList(
                            "+I[1, a, null, null]", "+I[2, b, bb, null]", "+I[3, c, cc, ccc]"));
        } finally {
            cancelJob(executionInfo);
        }
    }

    @Test
    void testSingleLogTableInLenientMode() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING");
        insertSourceRows(TABLE_1, "('2', CAST(NULL AS STRING)), ('3', '3')");

        composeAndCheck(
                TABLE_1,
                Arrays.asList("+I[2, null]", "+I[3, 3]"),
                new Configuration().set(PIPELINE_SCHEMA_CHANGE_BEHAVIOR, LENIENT));
    }

    @Test
    void testSingleLogTableInIgnoreMode() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING");
        insertSourceRows(TABLE_1, "('2', CAST(NULL AS STRING)), ('3', '3')");

        composeAndCheck(
                TABLE_1,
                Arrays.asList("+I[2, null]", "+I[3, 3]"),
                new Configuration().set(PIPELINE_SCHEMA_CHANGE_BEHAVIOR, IGNORE));
    }

    @Test
    void testMultiTables() throws Exception {
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING, PRIMARY KEY (col1) NOT ENFORCED");
        createSourceTable(TABLE_2, "col1 STRING, col2 STRING, PRIMARY KEY (col1) NOT ENFORCED");
        insertSourceRows(TABLE_1, "('1', '1'), ('2', '2'), ('3', '3')");
        insertSourceRows(TABLE_2, "('1', '1'), ('2', '2'), ('3', '3')");

        PipelineExecution.ExecutionInfo executionInfo =
                composeAndExecuteInLenientMode(TABLE_1, TABLE_2);
        try {
            checkResult(TABLE_1, Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[3, 3]"));
            checkResult(TABLE_2, Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[3, 3]"));
        } finally {
            cancelJob(executionInfo);
        }
    }

    @Test
    void testInsertExistTableWithMoreColumns() throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s ("
                                        + "col2 STRING, "
                                        + "col1 STRING, "
                                        + "PRIMARY KEY (col1) NOT ENFORCED)",
                                SINK_DB, TABLE_1))
                .await();
        createSourceTable(
                TABLE_1, "col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY (col1) NOT ENFORCED");
        insertSourceRows(TABLE_1, "('1', 'a', 'aa'), ('2', 'b', 'bb')");

        composeAndCheckInLenientMode(TABLE_1, Arrays.asList("+I[a, 1]", "+I[b, 2]"));
    }

    @Test
    void testInsertExistTableWithLessColumns() throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s ("
                                        + "col1 STRING, "
                                        + "col2 STRING, "
                                        + "col3 STRING, "
                                        + "PRIMARY KEY (col1) NOT ENFORCED)",
                                SINK_DB, TABLE_1))
                .await();
        createSourceTable(TABLE_1, "col1 STRING, col2 STRING, PRIMARY KEY (col1) NOT ENFORCED");
        insertSourceRows(TABLE_1, "('1', 'a'), ('2', 'b')");

        composeAndCheckInLenientMode(TABLE_1, Arrays.asList("+I[1, a, null]", "+I[2, b, null]"));
    }

    private void composeAndCheckInLenientMode(String tableName, List<String> expectedRows)
            throws Exception {
        composeAndCheck(
                tableName,
                expectedRows,
                new Configuration().set(PIPELINE_SCHEMA_CHANGE_BEHAVIOR, LENIENT));
    }

    private void composeAndCheck(String tableName, List<String> expectedRows, Configuration config)
            throws Exception {
        PipelineExecution.ExecutionInfo executionInfo =
                composeAndExecute(Collections.singletonList(tableName), config);
        try {
            checkResult(tableName, expectedRows);
        } finally {
            cancelJob(executionInfo);
        }
    }

    private PipelineExecution.ExecutionInfo composeAndExecuteInLenientMode(String... tableNames)
            throws Exception {
        return composeAndExecute(
                Arrays.asList(tableNames),
                new Configuration().set(PIPELINE_SCHEMA_CHANGE_BEHAVIOR, LENIENT));
    }

    private PipelineExecution.ExecutionInfo composeAndExecute(
            List<String> tableNames, Configuration pipelineConfig) throws Exception {
        return composeAndExecute(tableNames, defaultFlussOptions(), pipelineConfig);
    }

    private PipelineExecution.ExecutionInfo composeAndExecute(
            List<String> tableNames, Map<String, String> sinkOption, Configuration pipelineConfig)
            throws Exception {
        FlinkPipelineComposer composer =
                FlinkPipelineComposer.ofApplicationCluster(
                        StreamExecutionEnvironment.getExecutionEnvironment());
        composer.getEnv().enableCheckpointing(1000L);

        SourceDef sourceDef =
                new SourceDef(
                        "fluss",
                        "Fluss Source",
                        Configuration.fromMap(defaultSourceOptions(tableNames)));
        SinkDef sinkDef = new SinkDef("fluss", "Fluss Sink", Configuration.fromMap(sinkOption));

        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 4);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.singletonList(
                                new RouteDef(sourceRoutePattern(), SINK_DB + ".<>", "<>", null)),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = composer.compose(pipelineDef);
        return execution.execute();
    }

    private Map<String, String> defaultSourceOptions(List<String> tableNames) {
        Map<String, String> sourceOptions = defaultFlussOptions();
        sourceOptions.put("table.discoverer.pattern", sourcePattern(tableNames));
        sourceOptions.put("scan.startup.mode", "full");
        sourceOptions.put("scan.discovery.interval", "1 s");
        return sourceOptions;
    }

    private Map<String, String> defaultFlussOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        options.put("properties.client.security.protocol", "sasl");
        options.put("properties.client.security.sasl.mechanism", "PLAIN");
        options.put("properties.client.security.sasl.username", "guest");
        options.put("properties.client.security.sasl.password", "password2");
        return options;
    }

    private String sourceRoutePattern() {
        return SOURCE_DB + ".\\.*";
    }

    private String sourcePattern(List<String> tableNames) {
        return tableNames.stream()
                .map(tableName -> SOURCE_DB + "\\." + tableName)
                .collect(Collectors.joining("|"));
    }

    private String sourceTableName(String tableName) {
        return SOURCE_DB + "." + tableName;
    }

    private String sinkTableName(String tableName) {
        return SINK_DB + "." + tableName;
    }

    private void createSourceTable(String tableName, String columns) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format("CREATE TABLE %s (%s)", sourceTableName(tableName), columns))
                .await();
    }

    private void insertSourceRows(String tableName, String rows) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES %s", sourceTableName(tableName), rows))
                .await();
    }

    private void deleteSourceRows(String tableName, String condition) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "DELETE FROM %s WHERE %s", sourceTableName(tableName), condition))
                .await();
    }

    private void alterSourceTable(String tableName, String alterStatement) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "ALTER TABLE %s %s", sourceTableName(tableName), alterStatement))
                .await();
    }

    private void cancelJob(PipelineExecution.ExecutionInfo executionInfo) throws Exception {
        miniCluster.cancelJob(JobID.fromHexString(executionInfo.getId())).get();
    }

    private void checkResult(String tableName, List<String> expectedRows) throws Exception {
        waitUntilTableReady(SINK_DB, tableName);
        long deadline = System.currentTimeMillis() + RESULT_TIMEOUT.toMillis();
        Throwable lastError = null;
        String limitSql =
                String.format(
                        "select * from %s.%s limit %d",
                        CATALOG_NAME, sinkTableName(tableName), expectedRows.size());
        while (System.currentTimeMillis() < deadline) {
            try (CloseableIterator<Row> rowIter = tBatchEnv.executeSql(limitSql).collect()) {
                List<String> result = collectBatchRows(rowIter);
                assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRows);
                return;
            } catch (AssertionError | Exception e) {
                lastError = e;
                Thread.sleep(500L);
            }
        }
        if (lastError instanceof Exception) {
            throw (Exception) lastError;
        }
        if (lastError instanceof AssertionError) {
            throw (AssertionError) lastError;
        }
        throw new AssertionError("Timed out waiting for Fluss sink result: " + expectedRows);
    }

    /**
     * Waits until the given table has been created by the sink and its bucket assignments have an
     * elected leader, so that subsequent limit-pushdown queries against it won't block on leader
     * election or fail because the table doesn't exist yet.
     */
    private void waitUntilTableReady(String databaseName, String tableName) throws Exception {
        TablePath tablePath = TablePath.of(databaseName, tableName);
        long deadline = System.currentTimeMillis() + RESULT_TIMEOUT.toMillis();
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TableInfo tableInfo = null;
            Exception lastError = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    tableInfo = admin.getTableInfo(tablePath).get();
                    break;
                } catch (Exception e) {
                    lastError = e;
                    Thread.sleep(500L);
                }
            }
            if (tableInfo == null) {
                throw new IllegalStateException(
                        "Sink table " + tablePath + " was not created in time", lastError);
            }
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());
        }
    }

    private static org.apache.fluss.config.Configuration initConfig() {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 1.0);

        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        return conf;
    }

    String getBootstrapServers() {
        return String.join(
                ",",
                FLUSS_CLUSTER_EXTENSION
                        .getClientConfig("CLIENT")
                        .get(ConfigOptions.BOOTSTRAP_SERVERS));
    }
}
