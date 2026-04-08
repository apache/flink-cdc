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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.testutils.SqlServerSourceTestUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.AbstractCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CheckpointedCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** IT tests to cover various newly added tables during capture process in pipeline mode. */
class SqlServerPipelineNewlyAddedTableITCase extends SqlServerTestBase {

    private static final String DATABASE_NAME = "newly_added_table_test";
    private static final String SCHEMA_NAME = "dbo";
    private static final int DEFAULT_PARALLELISM = 4;

    private final ScheduledExecutorService mockCdcExecutor = Executors.newScheduledThreadPool(1);

    @BeforeEach
    void before() throws SQLException {
        TestValuesTableFactory.clearAllData();
        initializeDatabase();
    }

    @AfterEach
    void after() {
        mockCdcExecutor.shutdown();
    }

    private void initializeDatabase() throws SQLException {
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

            // Create produce_cdc_table for background CDC activity
            statement.execute(
                    "CREATE TABLE dbo.produce_cdc_table (id BIGINT NOT NULL PRIMARY KEY, cnt BIGINT);");
            statement.execute(
                    "INSERT INTO dbo.produce_cdc_table VALUES (0, 100), (1, 101), (2, 102);");
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                            + "@source_name = 'produce_cdc_table', @role_name = NULL, @supports_net_changes = 0;");

            // Mock continuous CDC during the newly added table capturing process
            mockCdcExecutor.schedule(
                    () -> {
                        try (Connection conn = getJdbcConnection();
                                Statement stmt = conn.createStatement()) {
                            stmt.execute(String.format("USE %s;", DATABASE_NAME));
                            stmt.execute(
                                    "UPDATE dbo.produce_cdc_table SET cnt = cnt + 1 WHERE id < 2;");
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    },
                    500,
                    TimeUnit.MILLISECONDS);
        }
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
    void testAddNewTableWithExclude() throws Exception {
        // Initialize all tables first
        initialAddressTables(
                Arrays.asList("address_hangzhou", "address_beijing", "address_excluded"));

        Map<String, String> options = new HashMap<>();
        options.put(TABLES_EXCLUDE.key(), DATABASE_NAME + "." + SCHEMA_NAME + ".address_excluded");

        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(
                        Collections.singletonList("address_\\.*"),
                        DEFAULT_PARALLELISM,
                        options,
                        true);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.enableCheckpointing(200);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        SqlServerDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        JobClient jobClient = env.executeAsync("TestExcludeTables");
        iterator.setJobClient(jobClient);

        // Expect 2 tables (hangzhou, beijing), each with CreateTableEvent + 3 DataChangeEvents = 8
        // total
        List<Event> actual = SqlServerSourceTestUtils.fetchResults(iterator, 8);

        List<String> tableNames =
                actual.stream()
                        .filter(event -> event instanceof CreateTableEvent)
                        .map(event -> ((SchemaChangeEvent) event).tableId().getTableName())
                        .collect(Collectors.toList());

        assertThat(tableNames).hasSize(2);
        assertThat(tableNames).containsExactlyInAnyOrder("address_hangzhou", "address_beijing");
        assertThat(tableNames).doesNotContain("address_excluded");

        jobClient.cancel().get();
        iterator.close();
    }

    private void testAddNewTable(TestParam testParam, int parallelism) throws Exception {
        // step 1: create sqlserver tables
        if (CollectionUtils.isNotEmpty(testParam.getFirstRoundInitTables())) {
            initialAddressTables(testParam.getFirstRoundInitTables());
        }
        Path savepointDir = Files.createTempDirectory("add-new-table-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;
        StreamExecutionEnvironment env =
                getStreamExecutionEnvironment(finishedSavePointPath, parallelism);

        // step 2: listen tables first time
        List<String> listenTablesFirstRound = testParam.getFirstRoundListenTables();

        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(listenTablesFirstRound, parallelism, new HashMap<>(), true);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        SqlServerDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(env, source, resultBuffer, serializer, accumulatorName);
        JobClient jobClient = env.executeAsync("beforeAddNewTable");
        iterator.setJobClient(jobClient);

        List<Event> actual =
                SqlServerSourceTestUtils.fetchResults(iterator, testParam.getFirstRoundFetchSize());
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
            initialAddressTables(testParam.getSecondRoundInitTables());
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
                getFlinkSourceProvider(listenTablesSecondRound, parallelism, new HashMap<>(), true);
        DataStreamSource<Event> restoreSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        SqlServerDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(restoredEnv, restoreSource, resultBuffer, serializer, accumulatorName);
        JobClient restoreClient = restoredEnv.executeAsync("AfterAddNewTable");

        List<String> newlyAddTables =
                listenTablesSecondRound.stream()
                        .filter(table -> !listenTablesFirstRound.contains(table))
                        .collect(Collectors.toList());
        // it means listen by pattern when newlyAddTables is empty
        if (CollectionUtils.isEmpty(newlyAddTables)) {
            newlyAddTables = testParam.getSecondRoundInitTables();
        }
        List<Event> newlyTableEvent =
                SqlServerSourceTestUtils.fetchResults(
                        restoredIterator, testParam.getSecondRoundFetchSize());
        multiAssert(newlyTableEvent, newlyAddTables);
        restoreClient.cancel().get();
        restoredIterator.close();
    }

    private void multiAssert(List<Event> actualEvents, List<String> listenTables) {
        List<Event> expectedCreateTableEvents = new ArrayList<>();
        List<Event> expectedDataChangeEvents = new ArrayList<>();
        for (String table : listenTables) {
            expectedCreateTableEvents.add(
                    getCreateTableEvent(TableId.tableId(DATABASE_NAME, SCHEMA_NAME, table)));
            expectedDataChangeEvents.addAll(
                    getSnapshotExpected(TableId.tableId(DATABASE_NAME, SCHEMA_NAME, table)));
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
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("country", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("city", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("detail_address", DataTypes.VARCHAR(1024))
                        .primaryKey(Collections.singletonList("id"))
                        .build();
        return new CreateTableEvent(tableId, schema);
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
                return jobClient.triggerSavepoint(savepointDirectory).get();
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

    private void initialAddressTables(List<String> addressTables) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE %s;", DATABASE_NAME));
            for (String tableName : addressTables) {
                String cityName = tableName.split("_")[1];
                // Create table
                statement.execute(
                        String.format(
                                "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '%s') "
                                        + "CREATE TABLE %s.%s ("
                                        + "  id BIGINT NOT NULL PRIMARY KEY,"
                                        + "  country VARCHAR(255) NOT NULL,"
                                        + "  city VARCHAR(255) NOT NULL,"
                                        + "  detail_address VARCHAR(1024)"
                                        + ");",
                                tableName, SCHEMA_NAME, tableName));

                // Insert data
                statement.execute(
                        String.format(
                                "INSERT INTO %s.%s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                        + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                        + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                                SCHEMA_NAME,
                                tableName,
                                cityName,
                                cityName,
                                cityName,
                                cityName,
                                cityName,
                                cityName));

                // Enable CDC for the table
                statement.execute(
                        String.format(
                                "EXEC sys.sp_cdc_enable_table @source_schema = '%s', "
                                        + "@source_name = '%s', @role_name = NULL, @supports_net_changes = 0;",
                                SCHEMA_NAME, tableName));
            }
        }
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            List<String> tables,
            int parallelism,
            Map<String, String> additionalOptions,
            boolean enableScanNewlyAddedTable) {
        List<String> fullTableNames =
                tables.stream()
                        .map(table -> DATABASE_NAME + "." + SCHEMA_NAME + "." + table)
                        .collect(Collectors.toList());
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(TABLES.key(), StringUtils.join(fullTableNames, ","));
        if (enableScanNewlyAddedTable) {
            options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        }
        options.putAll(additionalOptions);
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        org.apache.flink.cdc.common.configuration.Configuration.fromMap(options),
                        null,
                        this.getClass().getClassLoader());

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);

        return (FlinkSourceProvider) dataSource.getEventSourceProvider();
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env,
            DataStreamSource<T> source,
            AbstractCollectResultBuffer<T> buffer,
            TypeSerializer<T> serializer,
            String accumulatorName) {
        CollectSinkOperatorFactory<T> sinkFactory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) sinkFactory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        buffer, operator.getOperatorIdFuture(), accumulatorName, 0);
        CollectStreamSink<T> sink = new CollectStreamSink<>(source, sinkFactory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        env.registerCollectIterator(iterator);
        return iterator;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) {
        Configuration configuration = new Configuration();
        if (finishedSavePointPath != null) {
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(500L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
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
}
