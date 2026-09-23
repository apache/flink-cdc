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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.AbstractCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CheckpointedCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.DECODING_PLUGIN_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PG_PORT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SLOT_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** IT tests to cover newly added tables during capture process in Postgres pipeline mode. */
class PostgresPipelineNewlyAddedTableITCase extends PostgresTestBase {

    private static final String SCHEMA_NAME = "customer";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER, "postgres", SCHEMA_NAME, TEST_USER, TEST_PASSWORD);

    private String slotName;

    @BeforeEach
    void before() throws SQLException {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    private PostgresConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", customDatabase.getHost());
        properties.put("port", String.valueOf(customDatabase.getDatabasePort()));
        properties.put("user", customDatabase.getUsername());
        properties.put("password", customDatabase.getPassword());
        properties.put("dbname", customDatabase.getDatabaseName());
        return createConnection(properties);
    }

    @Test
    void testAddNewTableByPatternSingleParallelism() throws Exception {
        // step 1: create the first table
        initialAddressTables(getConnection(), Collections.singletonList("address_hangzhou"));

        Path savepointDir = Files.createTempDirectory("add-new-table-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;

        // step 2: listen to the pattern in the first round with single parallelism
        // so that each table emits exactly one CreateTableEvent.
        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        List<String> tables = Collections.singletonList("address_\\.*");
        FlinkSourceProvider sourceProvider = getFlinkSourceProvider(tables, 1);
        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
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

        // step 3: assert fetched snapshot data of the first table
        List<Event> actual = fetchResults(iterator, 4);
        multiAssert(actual, Collections.singletonList("address_hangzhou"));

        // step 4: create a newly added table matching the pattern
        initialAddressTables(getConnection(), Collections.singletonList("address_beijing"));

        // sleep 1s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
        // Otherwise, the restart job won't read newly added tables, and this test will be stuck.
        Thread.sleep(1000L);

        // step 5: trigger a savepoint and cancel the job
        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();
        iterator.close();

        // step 6: restore from savepoint with the same pattern
        StreamExecutionEnvironment restoredEnv =
                getStreamExecutionEnvironment(finishedSavePointPath, 1);
        FlinkSourceProvider restoredSourceProvider = getFlinkSourceProvider(tables, 1);
        DataStreamSource<Event> restoreSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        CollectResultIterator<Event> restoredIterator =
                addCollector(restoredEnv, restoreSource, resultBuffer, serializer, accumulatorName);
        restoredEnv.executeAsync("AfterAddNewTable");

        // step 7: assert the newly added table is captured after restore
        List<Event> newlyTableEvent = fetchResults(restoredIterator, 4);
        multiAssert(newlyTableEvent, Collections.singletonList("address_beijing"));
    }

    private void multiAssert(List<Event> actualEvents, List<String> listenTables) {
        List<Event> expectedCreateTableEvents = new ArrayList<>();
        List<Event> expectedDataChangeEvents = new ArrayList<>();
        for (String table : listenTables) {
            expectedCreateTableEvents.add(getCreateTableEvent(TableId.tableId(SCHEMA_NAME, table)));
            expectedDataChangeEvents.addAll(
                    getSnapshotExpected(TableId.tableId(SCHEMA_NAME, table)));
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

    private void initialAddressTables(PostgresConnection connection, List<String> addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                String tableId =
                        customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
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
                connection.execute(format("ALTER TABLE %s REPLICA IDENTITY FULL", tableId));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private FlinkSourceProvider getFlinkSourceProvider(List<String> tables, int parallelism) {
        List<String> fullTableNames =
                tables.stream()
                        .map(
                                table ->
                                        customDatabase.getDatabaseName()
                                                + "."
                                                + SCHEMA_NAME
                                                + "."
                                                + table)
                        .collect(Collectors.toList());
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(SERVER_TIME_ZONE.key(), "UTC");
        options.put(TABLES.key(), String.join(",", fullTableNames));
        options.put(SLOT_NAME.key(), slotName);
        options.put(DECODING_PLUGIN_NAME.key(), "pgoutput");
        options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");

        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        org.apache.flink.cdc.common.configuration.Configuration.fromMap(options),
                        null,
                        this.getClass().getClassLoader());

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        PostgresDataSource dataSource = (PostgresDataSource) factory.createDataSource(context);

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
            configuration.setString("execution.savepoint.path", finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(500L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 1000L);
        return env;
    }

    private List<Event> fetchResults(CollectResultIterator<Event> iterator, int size)
            throws Exception {
        List<Event> result = new ArrayList<>(size);
        while (size > 0 && iterator.hasNext()) {
            Event event = iterator.next();
            if (event instanceof CreateTableEvent || event instanceof DataChangeEvent) {
                result.add(event);
                size--;
            }
        }
        return result;
    }
}
