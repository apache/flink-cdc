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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.testutils.SqlServerSourceTestUtils;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Parallelized Integration test for SQL Server connector. */
public class SqlServerParallelizedPipelineITCase extends SqlServerTestBase {

    private static final int PARALLELISM = 4;
    private static final int TEST_TABLE_NUMBER = 10;
    private static final String DATABASE_NAME = "parallel_test";

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    public void init() {
        System.setOut(new PrintStream(outCaptor));
        ValuesDatabase.clear();
        initializeParallelTestDatabase();
    }

    @AfterEach
    public void cleanup() {
        System.setOut(standardOut);
    }

    private void initializeParallelTestDatabase() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            // Drop database if exists
            statement.execute(
                    String.format(
                            "IF EXISTS(select 1 from sys.databases where name = '%s') "
                                    + "BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                                    + "DROP DATABASE [%s]; END",
                            DATABASE_NAME, DATABASE_NAME, DATABASE_NAME));

            // Create database
            statement.execute(String.format("CREATE DATABASE %s;", DATABASE_NAME));
            statement.execute(String.format("USE %s;", DATABASE_NAME));

            // Wait for SQL Server Agent
            statement.execute("WAITFOR DELAY '00:00:03';");
            statement.execute("EXEC sys.sp_cdc_enable_db;");

            // Create multiple test tables
            for (int i = 1; i <= TEST_TABLE_NUMBER; i++) {
                statement.execute(
                        String.format(
                                "CREATE TABLE dbo.TABLE%d (ID INT NOT NULL PRIMARY KEY, VERSION VARCHAR(17));",
                                i));
                statement.execute(
                        String.format("INSERT INTO dbo.TABLE%d VALUES (%d, 'No.%d');", i, i, i));
                statement.execute(
                        String.format(
                                "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                                        + "@source_name = 'TABLE%d', @role_name = NULL, @supports_net_changes = 0;",
                                i));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize parallel test database", e);
        }
    }

    @Test
    void testParallelizedSnapshotReading() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        // Start checkpointing so snapshot completion is check-pointed and stream splits can follow.
        composer.getEnv().enableCheckpointing(2000);

        // Setup SQL Server source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(SqlServerDataSourceOptions.HOSTNAME, MSSQL_SERVER_CONTAINER.getHost());
        sourceConfig.set(
                SqlServerDataSourceOptions.PORT,
                MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT));
        sourceConfig.set(SqlServerDataSourceOptions.USERNAME, MSSQL_SERVER_CONTAINER.getUsername());
        sourceConfig.set(SqlServerDataSourceOptions.PASSWORD, MSSQL_SERVER_CONTAINER.getPassword());
        sourceConfig.set(SqlServerDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfig.set(SqlServerDataSourceOptions.TABLES, DATABASE_NAME + ".dbo.\\.*");

        SourceDef sourceDef =
                new SourceDef(
                        SqlServerDataSourceFactory.IDENTIFIER, "SQL Server Source", sourceConfig);

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
        AtomicReference<Throwable> pipelineFailure = new AtomicReference<>();
        Thread executeThread =
                new Thread(
                        () -> {
                            try {
                                execution.execute();
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            } catch (CancellationException ignored) {
                            } catch (Throwable t) {
                                pipelineFailure.compareAndSet(null, t);
                            }
                        });

        executeThread.start();

        // Expected: each sink subtask prints its CreateTableEvent plus one DataChangeEvent per
        // table.
        int expectedMinimumEvents = TEST_TABLE_NUMBER * (PARALLELISM + 1);

        try {
            SqlServerSourceTestUtils.loopCheck(
                    () -> {
                        if (pipelineFailure.get() != null) {
                            throw new RuntimeException(pipelineFailure.get());
                        }
                        return outCaptor.toString().trim().split("\n").length
                                >= expectedMinimumEvents;
                    },
                    "collect enough rows",
                    Duration.ofSeconds(120),
                    Duration.ofSeconds(1));
        } finally {
            executeThread.interrupt();
            executeThread.join(Duration.ofSeconds(30).toMillis());
        }

        if (pipelineFailure.get() != null) {
            throw new RuntimeException("Pipeline execution failed", pipelineFailure.get());
        }

        // Verify all tables have been captured
        String outputEvents = outCaptor.toString();

        // Verify CreateTableEvents for all tables
        assertThat(outputEvents)
                .contains(
                        IntStream.range(0, PARALLELISM)
                                .boxed()
                                .flatMap(
                                        subtask ->
                                                IntStream.rangeClosed(1, TEST_TABLE_NUMBER)
                                                        .mapToObj(
                                                                i ->
                                                                        String.format(
                                                                                "%d> CreateTableEvent{tableId=%s.dbo.TABLE%d",
                                                                                subtask,
                                                                                DATABASE_NAME,
                                                                                i)))
                                .toArray(String[]::new));

        // Verify DataChangeEvents for all tables
        IntStream.rangeClosed(1, TEST_TABLE_NUMBER)
                .forEach(
                        i ->
                                assertThat(outputEvents)
                                        .contains(
                                                String.format(
                                                        "DataChangeEvent{tableId=%s.dbo.TABLE%d",
                                                        DATABASE_NAME, i)));
    }

    @Test
    void testParallelizedWithStreamingChanges() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        // Ensure checkpointing so the source can finalize snapshot splits and start streaming.
        composer.getEnv().enableCheckpointing(2000);

        // Setup SQL Server source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(SqlServerDataSourceOptions.HOSTNAME, MSSQL_SERVER_CONTAINER.getHost());
        sourceConfig.set(
                SqlServerDataSourceOptions.PORT,
                MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT));
        sourceConfig.set(SqlServerDataSourceOptions.USERNAME, MSSQL_SERVER_CONTAINER.getUsername());
        sourceConfig.set(SqlServerDataSourceOptions.PASSWORD, MSSQL_SERVER_CONTAINER.getPassword());
        sourceConfig.set(SqlServerDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        // Only capture first 3 tables
        final int capturedTableCount = 3;
        sourceConfig.set(
                SqlServerDataSourceOptions.TABLES,
                IntStream.rangeClosed(1, capturedTableCount)
                        .mapToObj(i -> DATABASE_NAME + ".dbo.TABLE" + i)
                        .collect(Collectors.joining(",")));

        SourceDef sourceDef =
                new SourceDef(
                        SqlServerDataSourceFactory.IDENTIFIER, "SQL Server Source", sourceConfig);

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
        AtomicReference<Throwable> pipelineFailure = new AtomicReference<>();
        Thread executeThread =
                new Thread(
                        () -> {
                            try {
                                execution.execute();
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            } catch (CancellationException ignored) {
                            } catch (Throwable t) {
                                pipelineFailure.compareAndSet(null, t);
                            }
                        });

        executeThread.start();

        // Wait for snapshot completion
        int expectedSnapshotEvents = capturedTableCount * (PARALLELISM + 1);
        try {
            SqlServerSourceTestUtils.loopCheck(
                    () -> {
                        if (pipelineFailure.get() != null) {
                            throw new RuntimeException(pipelineFailure.get());
                        }
                        return outCaptor.toString().trim().split("\n").length
                                >= expectedSnapshotEvents;
                    },
                    "collect snapshot events",
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1));
        } catch (Exception e) {
            executeThread.interrupt();
            throw e;
        }

        if (pipelineFailure.get() != null) {
            throw new RuntimeException("Pipeline execution failed", pipelineFailure.get());
        }

        // Perform streaming updates
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute("UPDATE dbo.TABLE1 SET VERSION = 'Updated' WHERE ID = 1");
            statement.execute("INSERT INTO dbo.TABLE2 VALUES (100, 'New Record')");
            statement.execute("DELETE FROM dbo.TABLE3 WHERE ID = 3");
        }

        // Wait for streaming events
        int expectedTotalEvents = expectedSnapshotEvents + 3; // 3 additional streaming events
        try {
            SqlServerSourceTestUtils.loopCheck(
                    () -> {
                        if (pipelineFailure.get() != null) {
                            throw new RuntimeException(pipelineFailure.get());
                        }
                        return outCaptor.toString().trim().split("\n").length
                                >= expectedTotalEvents;
                    },
                    "collect streaming events",
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1));
        } finally {
            executeThread.interrupt();
            executeThread.join(Duration.ofSeconds(30).toMillis());
        }

        if (pipelineFailure.get() != null) {
            throw new RuntimeException("Pipeline execution failed", pipelineFailure.get());
        }

        String outputEvents = outCaptor.toString();

        // Verify CreateTableEvents for captured tables (one per sink subtask)
        assertThat(outputEvents)
                .contains(
                        IntStream.range(0, PARALLELISM)
                                .boxed()
                                .flatMap(
                                        subtask ->
                                                IntStream.rangeClosed(1, capturedTableCount)
                                                        .mapToObj(
                                                                i ->
                                                                        String.format(
                                                                                "%d> CreateTableEvent{tableId=%s.dbo.TABLE%d",
                                                                                subtask,
                                                                                DATABASE_NAME,
                                                                                i)))
                                .toArray(String[]::new));

        // Verify snapshot events
        IntStream.rangeClosed(1, capturedTableCount)
                .forEach(
                        i ->
                                assertThat(outputEvents)
                                        .contains(
                                                String.format(
                                                        "DataChangeEvent{tableId=%s.dbo.TABLE%d",
                                                        DATABASE_NAME, i)));

        // Verify streaming events
        assertThat(outputEvents).contains("op=UPDATE");
        assertThat(outputEvents).contains("100, New Record");
        assertThat(outputEvents).contains("op=DELETE");
    }
}
