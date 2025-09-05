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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.loopCheck;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Parallelized Integration test for MySQL connector. */
class MySqlParallelizedPipelineITCase extends MySqlSourceTestBase {

    private static final int PARALLELISM = 4;
    private static final int TEST_TABLE_NUMBER = 100;

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    private final UniqueDatabase parallelismDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, "extreme_parallelism_test_database", TEST_USER, TEST_PASSWORD);

    @BeforeEach
    public void init() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    public void cleanup() {
        System.setOut(standardOut);
        parallelismDatabase.dropDatabase();
    }

    @Test
    void testExtremeParallelizedSchemaChange() throws Exception {
        final String databaseName = parallelismDatabase.getDatabaseName();
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), TEST_USER, TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(String.format("CREATE DATABASE %s;", databaseName));
            stat.execute(String.format("USE %s;", databaseName));
            for (int i = 1; i <= TEST_TABLE_NUMBER; i++) {
                stat.execute(String.format("DROP TABLE IF EXISTS TABLE%d;", i));
                stat.execute(
                        String.format(
                                "CREATE TABLE TABLE%d (ID INT NOT NULL PRIMARY KEY,VERSION VARCHAR(17));",
                                i));
                stat.execute(String.format("INSERT INTO TABLE%d VALUES (%d, 'No.%d');", i, i, i));
            }
        } catch (SQLException e) {
            LOG.error("Initialize table failed.", e);
            throw e;
        }
        LOG.info("Table initialized successfully.");

        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup MySQL source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(MySqlDataSourceOptions.HOSTNAME, MYSQL_CONTAINER.getHost());
        sourceConfig.set(MySqlDataSourceOptions.PORT, MYSQL_CONTAINER.getDatabasePort());
        sourceConfig.set(MySqlDataSourceOptions.USERNAME, TEST_USER);
        sourceConfig.set(MySqlDataSourceOptions.PASSWORD, TEST_PASSWORD);
        sourceConfig.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfig.set(MySqlDataSourceOptions.TABLES, "\\.*.\\.*");
        sourceConfig.set(MySqlDataSourceOptions.SERVER_ID, getServerId(PARALLELISM));

        SourceDef sourceDef =
                new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "MySQL Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, PARALLELISM);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        Thread executeThread =
                new Thread(
                        () -> {
                            try {
                                execution.execute();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        executeThread.start();

        try {
            loopCheck(
                    () ->
                            outCaptor.toString().trim().split("\n").length
                                    >= TEST_TABLE_NUMBER * (PARALLELISM + 1),
                    "collect enough rows",
                    Duration.ofSeconds(120),
                    Duration.ofSeconds(1));
        } finally {
            executeThread.interrupt();
        }

        // Check the order and content of all received events
        String outputEvents = outCaptor.toString();
        assertThat(outputEvents)
                .contains(
                        IntStream.rangeClosed(1, TEST_TABLE_NUMBER)
                                .boxed()
                                .flatMap(
                                        i ->
                                                Stream.concat(
                                                        IntStream.range(0, PARALLELISM)
                                                                .boxed()
                                                                .map(
                                                                        subTaskId ->
                                                                                String.format(
                                                                                        "%d> CreateTableEvent{tableId=%s.TABLE%d, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                                                                                        subTaskId,
                                                                                        parallelismDatabase
                                                                                                .getDatabaseName(),
                                                                                        i)),
                                                        Stream.of(
                                                                String.format(
                                                                        "> DataChangeEvent{tableId=%s.TABLE%d, before=[], after=[%d, No.%d], op=INSERT, meta=()}",
                                                                        parallelismDatabase
                                                                                .getDatabaseName(),
                                                                        i,
                                                                        i,
                                                                        i))))
                                .collect(Collectors.toList()));
    }
}
