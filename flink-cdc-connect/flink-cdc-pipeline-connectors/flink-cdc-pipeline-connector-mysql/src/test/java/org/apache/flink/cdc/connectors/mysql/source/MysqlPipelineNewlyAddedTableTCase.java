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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
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
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests to cover various newly added tables during capture process in pipeline mode. */
public class MysqlPipelineNewlyAddedTableTCase extends MySqlSourceTestBase {
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final ScheduledExecutorService mockBinlogExecutor = Executors.newScheduledThreadPool(1);

    @Before
    public void before() throws SQLException {
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

    @After
    public void after() {
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
    public void testAddNewTableOneByOne() throws Exception {
        // step 1: create mysql tables with all tables included
        initialAddressTables(getConnection(), "address_hangzhou", "address_beijing");
        Path savepointDir = Files.createTempDirectory("add-new-table-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;
        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath);
        // step 2: only listen one table at first
        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(customDatabase.getDatabaseName() + ".address_hangzhou");
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

        List<Event> actual = fetchResults(iterator, 4);
        TableId tableId = TableId.tableId(customDatabase.getDatabaseName(), "address_hangzhou");
        assertThat(actual.get(0)).isEqualTo(getCreateTableEvent(tableId));
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);
        assertThat(actual.subList(1, 4))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));

        // step 3: trigger a savepoint and cancel the job
        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();
        iterator.close();

        // step 4: add newly table when restore from savepoint
        StreamExecutionEnvironment restoredEnv =
                getStreamExecutionEnvironment(finishedSavePointPath);
        FlinkSourceProvider restoredSourceProvider =
                getFlinkSourceProvider(
                        customDatabase.getDatabaseName() + ".address_hangzhou",
                        customDatabase.getDatabaseName() + ".address_beijing");
        DataStreamSource<Event> restoreSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(restoredEnv, restoreSource, resultBuffer, serializer, accumulatorName);
        JobClient restoreClient = restoredEnv.executeAsync("AfterAddNewTable");

        List<Event> newlyTableEvent = fetchResults(restoredIterator, 4);
        tableId = TableId.tableId(customDatabase.getDatabaseName(), "address_beijing");
        assertThat(newlyTableEvent.get(0)).isEqualTo(getCreateTableEvent(tableId));

        expectedSnapshot = getSnapshotExpected(tableId);
        assertThat(newlyTableEvent.subList(1, 4))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));

        restoreClient.cancel().get();
    }

    @Test
    public void testAddNewTableByPattern() throws Exception {
        // step 1: create mysql tables with all tables included
        initialAddressTables(getConnection(), "address_hangzhou", "address_beijing");
        Path savepointDir = Files.createTempDirectory("add-new-table-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;
        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath);
        // step 2: listen all table like address_* before creating new tables
        FlinkSourceProvider sourceProvider =
                getFlinkSourceProvider(customDatabase.getDatabaseName() + ".address_\\.*");
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

        List<Event> actual = fetchResults(iterator, 8);
        TableId tableId1 = TableId.tableId(customDatabase.getDatabaseName(), "address_hangzhou");
        TableId tableId2 = TableId.tableId(customDatabase.getDatabaseName(), "address_beijing");

        List<Event> actualCreateTableEvents =
                actual.stream()
                        .filter(event -> event instanceof CreateTableEvent)
                        .collect(Collectors.toList());

        assertThat(actualCreateTableEvents)
                .containsExactlyInAnyOrder(
                        Arrays.asList(getCreateTableEvent(tableId1), getCreateTableEvent(tableId2))
                                .toArray(new Event[0]));

        List<Event> allExpectedSnapshot = new ArrayList<>();
        allExpectedSnapshot.addAll(getSnapshotExpected(tableId1));
        allExpectedSnapshot.addAll(getSnapshotExpected(tableId2));

        List<Event> allActualDataChangeEvents =
                actual.stream()
                        .filter(event -> event instanceof DataChangeEvent)
                        .collect(Collectors.toList());

        assertThat(allActualDataChangeEvents)
                .containsExactlyInAnyOrder(allExpectedSnapshot.toArray(new Event[0]));

        // step 3: create some new tables
        initialAddressTables(getConnection(), "address_shanghai", "address_suzhou");
        // step 4: trigger a savepoint and cancel the job
        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();
        iterator.close();

        // step 5: restore from savepoint
        StreamExecutionEnvironment restoredEnv =
                getStreamExecutionEnvironment(finishedSavePointPath);
        FlinkSourceProvider restoredSourceProvider =
                getFlinkSourceProvider(customDatabase.getDatabaseName() + ".address_\\.*");
        DataStreamSource<Event> restoreSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(restoredEnv, restoreSource, resultBuffer, serializer, accumulatorName);
        JobClient restoreClient = restoredEnv.executeAsync("AfterAddNewTable");

        List<Event> newlyTableEvent = fetchResults(restoredIterator, 8);
        tableId1 = TableId.tableId(customDatabase.getDatabaseName(), "address_shanghai");
        tableId2 = TableId.tableId(customDatabase.getDatabaseName(), "address_suzhou");

        actualCreateTableEvents =
                newlyTableEvent.stream()
                        .filter(event -> event instanceof CreateTableEvent)
                        .collect(Collectors.toList());

        assertThat(actualCreateTableEvents)
                .containsExactlyInAnyOrder(
                        Arrays.asList(getCreateTableEvent(tableId1), getCreateTableEvent(tableId2))
                                .toArray(new Event[0]));

        allExpectedSnapshot = new ArrayList<>();
        allExpectedSnapshot.addAll(getSnapshotExpected(tableId1));
        allExpectedSnapshot.addAll(getSnapshotExpected(tableId2));

        allActualDataChangeEvents =
                newlyTableEvent.stream()
                        .filter(event -> event instanceof DataChangeEvent)
                        .collect(Collectors.toList());

        assertThat(allActualDataChangeEvents)
                .containsExactlyInAnyOrder(allExpectedSnapshot.toArray(new Event[0]));
        restoreClient.cancel().get();
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

    private void initialAddressTables(JdbcConnection connection, String... addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = customDatabase.getDatabaseName() + "." + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE "
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

    private FlinkSourceProvider getFlinkSourceProvider(String... tables) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(TABLES.key(), StringUtils.join(tables, ","));
        options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        Factory.Context context =
                new MySqlDataSourceFactoryTest.MockContext(
                        org.apache.flink.cdc.common.configuration.Configuration.fromMap(options));

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

    private StreamExecutionEnvironment getStreamExecutionEnvironment(String finishedSavePointPath) {
        Configuration configuration = new Configuration();
        if (finishedSavePointPath != null) {
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(500L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        return env;
    }
}
