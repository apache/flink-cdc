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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.RecordDataTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.collect.AbstractCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CheckpointedCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIteratorAdapter;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_ID;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.loopCheck;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests to cover various newly added tables during capture process in pipeline mode. */
class MysqlPipelineNewlyAddedTableITCase extends MySqlSourceTestBase {
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final ScheduledExecutorService mockBinlogExecutor = Executors.newScheduledThreadPool(1);

    @BeforeEach
    void before() throws SQLException {
        TestValuesTableFactory.clearAllData();
        customDatabase.createAndInitialize();

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            // prepare initial data for given table
            String tableId = customDatabase.getDatabaseName() + ".produce_binlog_table";
            connection.execute(
                    format("CREATE TABLE %s ( id BIGINT PRIMARY KEY, cnt BIGINT);", tableId));
            connection.execute(
                    format("INSERT INTO  %s VALUES (0, 100), (1, 101), (2, 102);", tableId));
            connection.commit();

            // mock continuous binlog during the newly added table capturing process
            mockBinlogExecutor.schedule(
                    () -> {
                        try {
                            connection.execute(
                                    format("UPDATE  %s SET  cnt = cnt +1 WHERE id < 2;", tableId));
                            connection.commit();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    },
                    500,
                    TimeUnit.MICROSECONDS);
        }
    }

    @AfterEach
    void after() {
        mockBinlogExecutor.shutdown();
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    @Test
    void testScanBinlogNewlyAddedTableEnabled() throws Exception {
        List<String> tables = Collections.singletonList("address_\\.*");
        Map<String, String> options = new HashMap<>();
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");
        options.put(
                SCAN_STARTUP_TIMESTAMP_MILLIS.key(), String.valueOf(System.currentTimeMillis()));

        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(tables, 4, options, false, true);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.enableCheckpointing(200);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation()
                        .getOutputType()
                        .createSerializer(env.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        env.executeAsync("AddNewlyTablesWhenReadingBinlog");
        initialAddressTables(getConnection(), Collections.singletonList("address_beijing"));
        initialAddressTables(getConnection(), Collections.singletonList("address_shanghai"));
        List<Event> actual = fetchResults(iterator, 8);
        List<String> tableNames =
                actual.stream()
                        .filter((event) -> event instanceof CreateTableEvent)
                        .map((event) -> ((SchemaChangeEvent) event).tableId().getTableName())
                        .collect(Collectors.toList());
        assertThat(tableNames).hasSize(2);
        assertThat(tableNames.get(0)).isEqualTo("address_beijing");
        assertThat(tableNames.get(1)).isEqualTo("address_shanghai");
    }

    @Test
    void testScanBinlogNewlyAddedTableEnabledAndExcludeTables() throws Exception {
        List<String> tables = Collections.singletonList("address_\\.*");
        Map<String, String> options = new HashMap<>();
        options.put(TABLES_EXCLUDE.key(), customDatabase.getDatabaseName() + ".address_beijing");
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");
        options.put(
                SCAN_STARTUP_TIMESTAMP_MILLIS.key(), String.valueOf(System.currentTimeMillis()));

        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(tables, 4, options, false, true);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.enableCheckpointing(200);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation()
                        .getOutputType()
                        .createSerializer(env.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        env.executeAsync("AddNewlyTablesWhenReadingBinlog");
        initialAddressTables(
                getConnection(), Lists.newArrayList("address_beijing", "address_shanghai"));
        List<Event> actual = fetchResults(iterator, 4);
        List<String> tableNames =
                actual.stream()
                        .filter((event) -> event instanceof CreateTableEvent)
                        .map((event) -> ((SchemaChangeEvent) event).tableId().getTableName())
                        .collect(Collectors.toList());
        assertThat(tableNames).hasSize(1);
        assertThat(tableNames.get(0)).isEqualTo("address_shanghai");
    }

    @Test
    void testAddNewTableOneByOneSingleParallelism() throws Exception {
        TestParam testParam =
                TestParam.newBuilder(
                                Collections.singletonList("address_hangzhou"),
                                4,
                                Arrays.asList("address_hangzhou", "address_beijing"),
                                4)
                        .setFirstRoundInitTables(
                                Arrays.asList("address_hangzhou", "address_beijing"))
                        .build();

        testAddNewTable(testParam, 1);
    }

    @Test
    void testAddNewTableOneByOne() throws Exception {
        TestParam testParam =
                TestParam.newBuilder(
                                Collections.singletonList("address_hangzhou"),
                                4,
                                Arrays.asList("address_hangzhou", "address_beijing"),
                                4)
                        .setFirstRoundInitTables(
                                Arrays.asList("address_hangzhou", "address_beijing"))
                        .build();

        testAddNewTable(testParam, DEFAULT_PARALLELISM);
    }

    @Test
    void testAddNewTableByPatternSingleParallelism() throws Exception {
        TestParam testParam =
                TestParam.newBuilder(
                                Collections.singletonList("address_\\.*"),
                                8,
                                Collections.singletonList("address_\\.*"),
                                8)
                        .setFirstRoundInitTables(
                                Arrays.asList("address_hangzhou", "address_beijing"))
                        .setSecondRoundInitTables(
                                Arrays.asList("address_shanghai", "address_suzhou"))
                        .build();

        testAddNewTable(testParam, 1);
    }

    @Test
    void testAddNewTableByPattern() throws Exception {
        TestParam testParam =
                TestParam.newBuilder(
                                Collections.singletonList("address_\\.*"),
                                8,
                                Collections.singletonList("address_\\.*"),
                                12)
                        .setFirstRoundInitTables(
                                Arrays.asList("address_hangzhou", "address_beijing"))
                        .setSecondRoundInitTables(
                                Arrays.asList(
                                        "address_shanghai", "address_suzhou", "address_shenzhen"))
                        .build();

        testAddNewTable(testParam, DEFAULT_PARALLELISM);
    }

    @Test
    void testSavepointRestoreReplaysCreateTableEventBeforeNewBinlog() throws Exception {
        String tableName = "address_hangzhou";
        initialAddressTables(getConnection(), Collections.singletonList(tableName));

        Path savepointDir = Files.createTempDirectory("restore-create-table-test");
        String savepointDirectory = savepointDir.toAbsolutePath().toString();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(null, 1);
        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(
                        Collections.singletonList(tableName), 1, new HashMap<>(), false, false);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation()
                        .getOutputType()
                        .createSerializer(env.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        JobClient jobClient = env.executeAsync("BeforeRestoreReplayCreateTable");
        iterator.setJobClient(jobClient);

        List<Event> initialEvents = fetchResults(iterator, 4);
        multiAssert(initialEvents, Collections.singletonList(tableName));

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            connection.execute(
                    format(
                            "INSERT INTO %s.%s VALUES "
                                    + "(417122095255614379, 'China', 'hangzhou', 'hangzhou West Town address 4');",
                            customDatabase.getDatabaseName(), tableName));
            connection.commit();
        }
        assertThat(fetchResults(iterator, 1).get(0)).isInstanceOf(DataChangeEvent.class);

        String savepointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();
        iterator.close();

        StreamExecutionEnvironment restoredEnv = getStreamExecutionEnvironment(savepointPath, 1);
        FlinkSourceProvider restoredSourceProvider =
                getFlinkSourceProvider(
                        Collections.singletonList(tableName), 1, new HashMap<>(), false, false);
        DataStreamSource<Event> restoredSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(
                        restoredEnv, restoredSource, resultBuffer, serializer, accumulatorName);
        JobClient restoredClient = restoredEnv.executeAsync("AfterRestoreReplayCreateTable");
        restoredIterator.setJobClient(restoredClient);

        try {
            assertThat(fetchResultsWithTimeout(restoredIterator, 1, 30, TimeUnit.SECONDS))
                    .containsExactly(
                            getCreateTableEvent(
                                    TableId.tableId(customDatabase.getDatabaseName(), tableName)));

            try (MySqlConnection connection = getConnection()) {
                connection.setAutoCommit(false);
                connection.execute(
                        format(
                                "INSERT INTO %s.%s VALUES "
                                        + "(417122095255614380, 'China', 'hangzhou', 'hangzhou West Town address 5');",
                                customDatabase.getDatabaseName(), tableName));
                connection.commit();
            }

            assertThat(fetchResultsWithTimeout(restoredIterator, 1, 30, TimeUnit.SECONDS).get(0))
                    .isInstanceOf(DataChangeEvent.class);
        } finally {
            restoredClient.cancel().get();
            restoredIterator.close();
        }
    }

    @Test
    void testFullPipelineRestorePropagatesComputedColumnProjectionChangeToSink() throws Exception {
        String tableName = "address_hangzhou";
        String tableSelector = customDatabase.getDatabaseName() + ".address_\\.*";
        String initialProjection = "*";
        String updatedProjection = "*, CURRENT_TIMESTAMP AS confluent__last_updated";
        String pipelineUidPrefix = "mysql-computed-column-restore";
        String initialJobName = "mysql-values-before-restore";
        String restoredJobName = "mysql-values-after-restore";
        TableId tableId = TableId.tableId(customDatabase.getDatabaseName(), tableName);

        initialAddressTables(getConnection(), Collections.singletonList(tableName));
        ValuesDatabase.clear();

        Path savepointDir = Files.createTempDirectory("restore-full-pipeline-transform-test");
        String savepointDirectory = savepointDir.toAbsolutePath().toString();

        try {
            StreamExecutionEnvironment initialEnv = getStreamExecutionEnvironment(null, 1);
            RestartStrategyUtils.configureNoRestartStrategy(initialEnv);
            composePipeline(
                    initialEnv,
                    buildPipelineDef(
                            tableName,
                            tableSelector,
                            initialProjection,
                            pipelineUidPrefix,
                            initialJobName));
            JobClient initialClient = initialEnv.executeAsync(initialJobName);
            try {
                waitForJobRunning(initialClient, 30, TimeUnit.SECONDS);
                waitForValuesSchema(
                        tableId, Collections.singletonList("confluent__last_updated"), false, 30);

                try (MySqlConnection connection = getConnection()) {
                    connection.setAutoCommit(false);
                    connection.execute(
                            format(
                                    "INSERT INTO %s.%s VALUES "
                                            + "(417122095255614382, 'China', 'hangzhou', 'hangzhou West Town address 6');",
                                    customDatabase.getDatabaseName(), tableName));
                    connection.commit();
                }

                waitForValuesResultContains(
                        tableId, 30, "417122095255614382", "hangzhou West Town address 6");

                String savepointPath = triggerSavepointWithRetry(initialClient, savepointDirectory);
                initialClient.cancel().get();

                StreamExecutionEnvironment restoredEnv =
                        getStreamExecutionEnvironment(savepointPath, 1);
                RestartStrategyUtils.configureNoRestartStrategy(restoredEnv);
                composePipeline(
                        restoredEnv,
                        buildPipelineDef(
                                tableName,
                                tableSelector,
                                updatedProjection,
                                pipelineUidPrefix,
                                restoredJobName));
                JobClient restoredClient = restoredEnv.executeAsync(restoredJobName);
                try {
                    waitForJobRunning(restoredClient, 30, TimeUnit.SECONDS);
                    waitForValuesSchema(
                            tableId,
                            Collections.singletonList("confluent__last_updated"),
                            true,
                            30);

                    try (MySqlConnection connection = getConnection()) {
                        connection.setAutoCommit(false);
                        connection.execute(
                                format(
                                        "INSERT INTO %s.%s VALUES "
                                                + "(417122095255614383, 'China', 'hangzhou', 'hangzhou West Town address 7');",
                                        customDatabase.getDatabaseName(), tableName));
                        connection.commit();
                    }

                    waitForValuesResultContains(
                            tableId,
                            30,
                            "417122095255614383",
                            "hangzhou West Town address 7",
                            "confluent__last_updated=");
                    assertThat(findValuesResult(tableId, "417122095255614383"))
                            .contains("confluent__last_updated=")
                            .doesNotContain("confluent__last_updated=;");
                } finally {
                    cancelJobOrThrowFailure(restoredClient);
                }
            } finally {
                cancelJobOrThrowFailure(initialClient);
            }
        } finally {
            ValuesDatabase.clear();
        }
    }

    @Test
    void testSavepointRestoreReplaysTransformedCreateTableEventForComputedColumnProjectionChange()
            throws Exception {
        String tableName = "address_hangzhou";
        String initialProjection = "*";
        String updatedProjection = "*, CURRENT_TIMESTAMP AS confluent__last_updated";
        initialAddressTables(getConnection(), Collections.singletonList(tableName));

        Path savepointDir = Files.createTempDirectory("restore-transform-create-table-test");
        String savepointDirectory = savepointDir.toAbsolutePath().toString();
        Path initialOutputPath = Files.createTempFile("restore-transform-initial-", ".log");
        Path restoredOutputPath = Files.createTempFile("restore-transform-restored-", ".log");
        TableId tableId = TableId.tableId(customDatabase.getDatabaseName(), tableName);
        String tableIdString = tableId.toString();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(null, 1);
        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(
                        Collections.singletonList(tableName), 1, new HashMap<>(), false, false);
        DataStream<Event> initialPostTransformStream =
                createPostTransformStream(env, sourceProvider, tableName, initialProjection);
        addEventFileSink(initialPostTransformStream, initialOutputPath);
        JobClient jobClient = env.executeAsync("BeforeRestoreReplayTransformedCreateTable");

        waitForLineCount(initialOutputPath, 4, 30, TimeUnit.SECONDS);
        List<String> initialLines = readOutputLines(initialOutputPath);
        assertThat(initialLines.get(0))
                .contains("CREATE|" + tableIdString)
                .doesNotContain("confluent__last_updated");

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            connection.execute(
                    format(
                            "INSERT INTO %s.%s VALUES "
                                    + "(417122095255614380, 'China', 'hangzhou', 'hangzhou West Town address 4');",
                            customDatabase.getDatabaseName(), tableName));
            connection.commit();
        }
        waitForLineCount(initialOutputPath, 5, 30, TimeUnit.SECONDS);
        assertThat(readOutputLines(initialOutputPath).get(4))
                .contains(
                        "DATA|"
                                + tableIdString
                                + "|INSERT|before=null|after=[417122095255614380, China, hangzhou, hangzhou West Town address 4]");

        String savepointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();

        StreamExecutionEnvironment restoredEnv = getStreamExecutionEnvironment(savepointPath, 1);
        FlinkSourceProvider restoredSourceProvider =
                getFlinkSourceProvider(
                        Collections.singletonList(tableName), 1, new HashMap<>(), false, false);
        DataStream<Event> restoredPostTransformStream =
                createPostTransformStream(
                        restoredEnv, restoredSourceProvider, tableName, updatedProjection);
        addEventFileSink(restoredPostTransformStream, restoredOutputPath);
        JobClient restoredClient =
                restoredEnv.executeAsync("AfterRestoreReplayTransformedCreateTable");

        try {
            waitForLineCount(restoredOutputPath, 1, 30, TimeUnit.SECONDS);
            List<String> restoredLines = readOutputLines(restoredOutputPath);
            assertThat(restoredLines.get(0))
                    .contains("CREATE|" + tableIdString)
                    .contains("confluent__last_updated");

            try (MySqlConnection connection = getConnection()) {
                connection.setAutoCommit(false);
                connection.execute(
                        format(
                                "INSERT INTO %s.%s VALUES "
                                        + "(417122095255614381, 'China', 'hangzhou', 'hangzhou West Town address 5');",
                                customDatabase.getDatabaseName(), tableName));
                connection.commit();
            }

            waitForLineCount(restoredOutputPath, 2, 30, TimeUnit.SECONDS);
            String restoredDataLine = readOutputLines(restoredOutputPath).get(1);
            assertThat(restoredDataLine)
                    .contains(
                            "DATA|"
                                    + tableIdString
                                    + "|INSERT|before=null|after=[417122095255614381, China, hangzhou, hangzhou West Town address 5, ")
                    .doesNotContain(", null]");
        } finally {
            restoredClient.cancel().get();
        }
    }

    private DataStream<Event> createPostTransformStream(
            StreamExecutionEnvironment env,
            FlinkSourceProvider sourceProvider,
            String tableName,
            String projection) {
        String tablePattern = customDatabase.getDatabaseName() + "." + tableName;
        DataStream<Event> sourceStream =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .uid("mysql-source");

        DataStream<Event> preTransformStream =
                sourceStream
                        .transform(
                                "Transform:Schema",
                                new EventTypeInfo(),
                                PreTransformOperator.newBuilder()
                                        .addTransform(tablePattern, projection, null)
                                        .build())
                        .uid("mysql-pre-transform");

        DataStream<Event> postTransformStream =
                preTransformStream
                        .transform(
                                "Transform:Data",
                                new EventTypeInfo(),
                                PostTransformOperator.newBuilder()
                                        .addTransform(tablePattern, projection, null)
                                        .addTimezone("UTC")
                                        .build())
                        .uid("mysql-post-transform");

        return postTransformStream;
    }

    private <T> List<T> fetchResultsWithTimeout(
            CollectResultIterator<T> iterator, int size, long timeout, TimeUnit unit)
            throws Exception {
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "mysql-pipeline-fetch-results");
                            thread.setDaemon(true);
                            return thread;
                        });
        Future<List<T>> future = executor.submit(() -> fetchResults(iterator, size));
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            iterator.close();
            future.cancel(true);
            throw e;
        } finally {
            executor.shutdownNow();
        }
    }

    private void composePipeline(StreamExecutionEnvironment env, PipelineDef pipelineDef) {
        FlinkPipelineComposer.ofApplicationCluster(env).compose(pipelineDef);
    }

    private void addEventFileSink(DataStream<Event> stream, Path outputPath) {
        stream.addSink(new EventFileSink(outputPath.toString()))
                .name("mysql-event-file-sink")
                .uid("mysql-event-file-sink");
    }

    private List<String> readOutputLines(Path outputPath) throws IOException {
        if (!Files.exists(outputPath)) {
            return Collections.emptyList();
        }
        return Files.readAllLines(outputPath, StandardCharsets.UTF_8).stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    private void waitForLineCount(
            Path outputPath, int expectedLineCount, long timeout, TimeUnit unit) throws Exception {
        loopCheck(
                () -> {
                    try {
                        return readOutputLines(outputPath).size() >= expectedLineCount;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                "collect " + expectedLineCount + " events from " + outputPath,
                Duration.ofMillis(unit.toMillis(timeout)),
                Duration.ofMillis(200));
    }

    private void waitForJobRunning(JobClient client, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isTerminalState()) {
                try {
                    client.getJobExecutionResult().get();
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Job terminated before RUNNING with status " + status, e);
                }
                throw new IllegalStateException(
                        "Job terminated before RUNNING with status " + status);
            }
            Thread.sleep(200L);
        }
        throw new TimeoutException("Timed out waiting for job to enter RUNNING.");
    }

    private void cancelJobOrThrowFailure(JobClient client) throws Exception {
        JobStatus status = client.getJobStatus().get();
        if (status == JobStatus.CANCELED || status == JobStatus.FINISHED) {
            return;
        }
        if (status == JobStatus.FAILED) {
            client.getJobExecutionResult().get();
            return;
        }
        client.cancel().get();
    }

    private void waitForValuesSchema(
            TableId tableId,
            List<String> expectedColumns,
            boolean shouldContain,
            long timeoutSeconds)
            throws Exception {
        loopCheck(
                () -> {
                    try {
                        List<String> columnNames =
                                ValuesDatabase.getTableSchema(tableId).getColumnNames();
                        return shouldContain
                                ? columnNames.containsAll(expectedColumns)
                                : expectedColumns.stream().noneMatch(columnNames::contains);
                    } catch (Throwable t) {
                        return false;
                    }
                },
                "values schema update for " + tableId,
                Duration.ofSeconds(timeoutSeconds),
                Duration.ofMillis(200));
    }

    private void waitForValuesResultContains(TableId tableId, long timeoutSeconds, String... parts)
            throws Exception {
        loopCheck(
                () -> {
                    try {
                        return findValuesResult(tableId, parts) != null;
                    } catch (Throwable t) {
                        return false;
                    }
                },
                "values sink row for " + tableId,
                Duration.ofSeconds(timeoutSeconds),
                Duration.ofMillis(200));
    }

    private String findValuesResult(TableId tableId, String... parts) {
        return ValuesDatabase.getResults(tableId).stream()
                .filter(row -> Arrays.stream(parts).allMatch(part -> row.contains(part)))
                .findFirst()
                .orElse(null);
    }

    private PipelineDef buildPipelineDef(
            String tableName,
            String tableSelector,
            String projection,
            String pipelineUidPrefix,
            String pipelineName) {
        org.apache.flink.cdc.common.configuration.Configuration sourceConfig =
                new org.apache.flink.cdc.common.configuration.Configuration();
        sourceConfig.set(HOSTNAME, MYSQL_CONTAINER.getHost());
        sourceConfig.set(PORT, MYSQL_CONTAINER.getDatabasePort());
        sourceConfig.set(USERNAME, TEST_USER);
        sourceConfig.set(PASSWORD, TEST_PASSWORD);
        sourceConfig.set(SERVER_TIME_ZONE, "UTC");
        sourceConfig.set(TABLES, customDatabase.getDatabaseName() + "." + tableName);
        sourceConfig.set(SERVER_ID, getServerId(1));

        SourceDef sourceDef =
                new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "MySQL Source", sourceConfig);

        org.apache.flink.cdc.common.configuration.Configuration sinkConfig =
                new org.apache.flink.cdc.common.configuration.Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.PRINT_ENABLED, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, ValuesDataSink.SinkApi.SINK_V2);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Values Sink", sinkConfig);

        TransformDef transformDef =
                new TransformDef(
                        tableSelector, projection, null, "", "", "", ",", "", "SOFT_DELETE");

        org.apache.flink.cdc.common.configuration.Configuration pipelineConfig =
                new org.apache.flink.cdc.common.configuration.Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_NAME, pipelineName);
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_OPERATOR_UID_PREFIX, pipelineUidPrefix);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);

        return new PipelineDef(
                sourceDef,
                sinkDef,
                Collections.emptyList(),
                Collections.singletonList(transformDef),
                Collections.emptyList(),
                pipelineConfig);
    }

    private void testAddNewTable(TestParam testParam, int parallelism) throws Exception {
        // step 1: create mysql tables
        if (CollectionUtils.isNotEmpty(testParam.getFirstRoundInitTables())) {
            initialAddressTables(getConnection(), testParam.getFirstRoundInitTables());
        }
        Path savepointDir = Files.createTempDirectory("add-new-table-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;
        StreamExecutionEnvironment env =
                getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
        // step 2: listen tables first time
        List<String> listenTablesFirstRound = testParam.getFirstRoundListenTables();

        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(
                        listenTablesFirstRound, parallelism, new HashMap<>(), true, false);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation()
                        .getOutputType()
                        .createSerializer(env.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        JobClient jobClient = env.executeAsync("beforeAddNewTable");
        iterator.setJobClient(jobClient);

        List<Event> actual = fetchResults(iterator, testParam.getFirstRoundFetchSize());
        Optional<String> listenByPattern =
                listenTablesFirstRound.stream()
                        .filter(table -> StringUtils.contains(table, "\\.*"))
                        .findAny();
        multiAssert(
                actual,
                listenByPattern.isPresent()
                        ? testParam.getFirstRoundInitTables()
                        : listenTablesFirstRound);

        // step 3: create new tables if needed
        if (CollectionUtils.isNotEmpty(testParam.getSecondRoundInitTables())) {
            initialAddressTables(getConnection(), testParam.getSecondRoundInitTables());
        }

        // sleep 1s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
        // Otherwise, the restart job won't read newly added tables, and this test will be stuck.
        Thread.sleep(1000L);
        // step 4: trigger a savepoint and cancel the job
        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();
        iterator.close();

        // step 5: restore from savepoint
        StreamExecutionEnvironment restoredEnv =
                getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
        List<String> listenTablesSecondRound = testParam.getSecondRoundListenTables();
        FlinkSourceProvider restoredSourceProvider =
                getFlinkSourceProvider(
                        listenTablesSecondRound, parallelism, new HashMap<>(), true, false);
        DataStreamSource<Event> restoreSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(restoredEnv, restoreSource, resultBuffer, serializer, accumulatorName);
        JobClient restoreClient = restoredEnv.executeAsync("AfterAddNewTable");
        restoredIterator.setJobClient(restoreClient);

        List<String> newlyAddTables =
                listenTablesSecondRound.stream()
                        .filter(table -> !listenTablesFirstRound.contains(table))
                        .collect(Collectors.toList());
        // it means listen by pattern when newlyAddTables is empty
        if (CollectionUtils.isEmpty(newlyAddTables)) {
            newlyAddTables = testParam.getSecondRoundInitTables();
        }
        List<Event> newlyTableEvent =
                fetchResults(restoredIterator, testParam.getSecondRoundFetchSize());
        multiAssert(newlyTableEvent, newlyAddTables);
        restoreClient.cancel().get();
        restoredIterator.close();
    }

    private void multiAssert(List<Event> actualEvents, List<String> listenTables) {
        List<Event> expectedCreateTableEvents = new ArrayList<>();
        List<Event> expectedDataChangeEvents = new ArrayList<>();
        for (String table : listenTables) {
            expectedCreateTableEvents.add(
                    getCreateTableEvent(TableId.tableId(customDatabase.getDatabaseName(), table)));
            expectedDataChangeEvents.addAll(
                    getSnapshotExpected(TableId.tableId(customDatabase.getDatabaseName(), table)));
        }
        // compare create table events
        List<Event> actualCreateTableEvents =
                actualEvents.stream()
                        .filter(event -> event instanceof CreateTableEvent)
                        .collect(Collectors.toList());
        assertThat(actualCreateTableEvents)
                .containsExactlyInAnyOrder(expectedCreateTableEvents.toArray(new Event[0]));

        // compare data change events
        List<Event> actualDataChangeEvents =
                actualEvents.stream()
                        .filter(event -> event instanceof DataChangeEvent)
                        .collect(Collectors.toList());
        assertThat(actualDataChangeEvents)
                .containsExactlyInAnyOrder(expectedDataChangeEvents.toArray(new Event[0]));
    }

    private CreateTableEvent getCreateTableEvent(TableId tableId) {
        return getCreateTableEvent(tableId, false);
    }

    private CreateTableEvent getCreateTableEvent(TableId tableId, boolean withComputedTimestamp) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("country", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("city", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("detail_address", DataTypes.VARCHAR(1024));
        if (withComputedTimestamp) {
            schemaBuilder.physicalColumn("confluent__last_updated", DataTypes.TIMESTAMP_LTZ(3));
        }
        return new CreateTableEvent(
                tableId, schemaBuilder.primaryKey(Collections.singletonList("id")).build());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(1024)
                        },
                        new String[] {"id", "country", "city", "detail_address"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        String cityName = tableId.getTableName().split("_")[1];
        return Arrays.asList(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    416874195632735147L,
                                    BinaryStringData.fromString("China"),
                                    BinaryStringData.fromString(cityName),
                                    BinaryStringData.fromString(cityName + " West Town address 1")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    416927583791428523L,
                                    BinaryStringData.fromString("China"),
                                    BinaryStringData.fromString(cityName),
                                    BinaryStringData.fromString(cityName + " West Town address 2")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    417022095255614379L,
                                    BinaryStringData.fromString("China"),
                                    BinaryStringData.fromString(cityName),
                                    BinaryStringData.fromString(cityName + " West Town address 3")
                                })));
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient
                        .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private void initialAddressTables(JdbcConnection connection, List<String> addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = customDatabase.getDatabaseName() + "." + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE IF NOT EXISTS "
                                + tableId
                                + "("
                                + "  id BIGINT NOT NULL PRIMARY KEY,"
                                + "  country VARCHAR(255) NOT NULL,"
                                + "  city VARCHAR(255) NOT NULL,"
                                + "  detail_address VARCHAR(1024)"
                                + ");");
                connection.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                        + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                        + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                                tableId, cityName, cityName, cityName, cityName, cityName,
                                cityName));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            List<String> tables,
            int parallelism,
            Map<String, String> additionalOptions,
            boolean enableScanNewlyAddedTable,
            boolean enableBinlogScanNewlyAddedTable) {
        List<String> fullTableNames =
                tables.stream()
                        .map(table -> customDatabase.getDatabaseName() + "." + table)
                        .collect(Collectors.toList());
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(TABLES.key(), StringUtils.join(fullTableNames, ","));
        options.put(SERVER_ID.key(), getServerId(parallelism));
        if (enableScanNewlyAddedTable) {
            options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        }
        if (enableBinlogScanNewlyAddedTable) {
            options.put(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        }
        options.putAll(additionalOptions);
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        org.apache.flink.cdc.common.configuration.Configuration.fromMap(options),
                        null,
                        this.getClass().getClassLoader());

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);

        return (FlinkSourceProvider) dataSource.getEventSourceProvider();
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env,
            DataStream<T> source,
            AbstractCollectResultBuffer<T> buffer,
            TypeSerializer<T> serializer,
            String accumulatorName) {
        CollectSinkOperatorFactory<T> sinkFactory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectStreamSink<T> sink = new CollectStreamSink<>(source, sinkFactory);
        // Set both name and uid to the same value. The uid is used by Flink to generate
        // OperatorID via StreamGraphHasherV2.generateUserSpecifiedHash(uid), and the same
        // uid string must be passed to CollectResultIteratorAdapter for coordinator lookup.
        String operatorUid = "Data stream collect sink";
        sink.name(operatorUid).uid(operatorUid);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) sinkFactory.getOperator();
        env.addOperator(sink.getTransformation());
        CollectResultIterator<T> iterator =
                new CollectResultIteratorAdapter<>(
                        buffer, operatorUid, operator, accumulatorName, 0);
        env.registerCollectIterator(iterator);
        return iterator;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) {
        Configuration configuration = new Configuration();
        if (finishedSavePointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        configuration.set(
                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(500L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 1000L);
        return env;
    }

    private static class TestParam {
        private final List<String> firstRoundInitTables;
        private final List<String> firstRoundListenTables;
        private final Integer firstRoundFetchSize;
        private final List<String> secondRoundInitTables;
        private final List<String> secondRoundListenTables;
        private final Integer secondRoundFetchSize;

        private TestParam(Builder builder) {
            this.firstRoundInitTables = builder.firstRoundInitTables;
            this.firstRoundListenTables = builder.firstRoundListenTables;
            this.firstRoundFetchSize = builder.firstRoundFetchSize;
            this.secondRoundInitTables = builder.secondRoundInitTables;
            this.secondRoundListenTables = builder.secondRoundListenTables;
            this.secondRoundFetchSize = builder.secondRoundFetchSize;
        }

        public static Builder newBuilder(
                List<String> firstRoundListenTables,
                Integer firstRoundFetchSize,
                List<String> secondRoundListenTables,
                Integer secondRoundFetchSize) {
            return new Builder(
                    firstRoundListenTables,
                    firstRoundFetchSize,
                    secondRoundListenTables,
                    secondRoundFetchSize);
        }

        public static class Builder {
            private List<String> firstRoundInitTables;
            private final List<String> firstRoundListenTables;
            private final Integer firstRoundFetchSize;

            private List<String> secondRoundInitTables;
            private final List<String> secondRoundListenTables;
            private final Integer secondRoundFetchSize;

            public Builder(
                    List<String> firstRoundListenTables,
                    Integer firstRoundFetchSize,
                    List<String> secondRoundListenTables,
                    Integer secondRoundFetchSize) {
                this.firstRoundListenTables = firstRoundListenTables;
                this.firstRoundFetchSize = firstRoundFetchSize;
                this.secondRoundListenTables = secondRoundListenTables;
                this.secondRoundFetchSize = secondRoundFetchSize;
            }

            public TestParam build() {
                return new TestParam(this);
            }

            public Builder setFirstRoundInitTables(List<String> firstRoundInitTables) {
                this.firstRoundInitTables = firstRoundInitTables;
                return this;
            }

            public Builder setSecondRoundInitTables(List<String> secondRoundInitTables) {
                this.secondRoundInitTables = secondRoundInitTables;
                return this;
            }
        }

        public List<String> getFirstRoundInitTables() {
            return firstRoundInitTables;
        }

        public List<String> getFirstRoundListenTables() {
            return firstRoundListenTables;
        }

        public Integer getFirstRoundFetchSize() {
            return firstRoundFetchSize;
        }

        public List<String> getSecondRoundInitTables() {
            return secondRoundInitTables;
        }

        public List<String> getSecondRoundListenTables() {
            return secondRoundListenTables;
        }

        public Integer getSecondRoundFetchSize() {
            return secondRoundFetchSize;
        }
    }

    private static class EventFileSink extends RichSinkFunction<Event> {
        private final String outputFilePath;

        private transient BufferedWriter writer;
        private transient Map<TableId, Schema> schemaMap;

        private EventFileSink(String outputFilePath) {
            this.outputFilePath = outputFilePath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Path outputPath = Path.of(outputFilePath);
            Files.createDirectories(outputPath.getParent());
            writer =
                    Files.newBufferedWriter(
                            outputPath,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
            schemaMap = new HashMap<>();
        }

        @Override
        public void invoke(Event event, Context context) throws Exception {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemaMap.put(createTableEvent.tableId(), createTableEvent.getSchema());
            }
            writer.write(formatEvent(event));
            writer.newLine();
            writer.flush();
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.close();
            }
        }

        private String formatEvent(Event event) {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                return "CREATE|" + createTableEvent.tableId() + "|" + createTableEvent.getSchema();
            }
            if (event instanceof DataChangeEvent) {
                DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                Schema schema = schemaMap.get(dataChangeEvent.tableId());
                return "DATA|"
                        + dataChangeEvent.tableId()
                        + "|"
                        + dataChangeEvent.op()
                        + "|before="
                        + formatRecord(dataChangeEvent.before(), schema)
                        + "|after="
                        + formatRecord(dataChangeEvent.after(), schema);
            }
            return event.toString();
        }

        private String formatRecord(
                org.apache.flink.cdc.common.data.RecordData recordData, Schema schema) {
            if (recordData == null) {
                return "null";
            }
            if (schema == null) {
                return recordData.toString();
            }
            Object[] fields =
                    RecordDataTestUtils.recordFields(recordData, (RowType) schema.toRowDataType());
            return Arrays.toString(fields);
        }
    }
}
