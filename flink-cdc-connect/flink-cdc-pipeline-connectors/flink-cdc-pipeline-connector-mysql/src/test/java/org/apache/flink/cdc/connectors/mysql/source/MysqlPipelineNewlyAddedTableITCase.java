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
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
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

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
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
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
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
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
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
