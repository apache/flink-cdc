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
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Savepoint/restore IT for SQL Server pipeline source schema evolution. */
class SqlServerPipelineSavepointRestoreITCase extends SqlServerTestBase {

    private static final String DATABASE_NAME = "customer";
    private static final Map<String, BlockingQueue<Event>> EVENTS_BY_SINK =
            new ConcurrentHashMap<>();

    @BeforeEach
    void before() {
        initializeSqlServerTable(DATABASE_NAME);
        TestValuesTableFactory.clearAllData();
        EVENTS_BY_SINK.clear();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void testSchemaEvolutionContinuesAfterSavepointRestore() throws Exception {
        TableId tableId = TableId.tableId(DATABASE_NAME, "dbo", "customers");
        Schema schemaV1 = schemaV1();
        Schema schemaV2 = schemaV2();
        Schema schemaV3 = schemaV3();
        Schema schemaV4 = schemaV4();

        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.customers")
                                .startupOptions(StartupOptions.initial())
                                .includeSchemaChanges(true)
                                .serverTimeZone("UTC");

        String beforeSinkId = "before-" + UUID.randomUUID();
        String afterSinkId = "after-" + UUID.randomUUID();
        EVENTS_BY_SINK.put(beforeSinkId, new LinkedBlockingQueue<>());
        EVENTS_BY_SINK.put(afterSinkId, new LinkedBlockingQueue<>());

        Path savepointDir = Files.createTempDirectory("sqlserver-schema-restore-test");
        String savepointDirectory = savepointDir.toAbsolutePath().toString();

        JobClient firstJobClient = null;
        JobClient restoredJobClient = null;
        try {
            StreamExecutionEnvironment firstEnv = getStreamExecutionEnvironment(null, 1);
            DataStreamSource<Event> firstSource =
                    createSource(firstEnv, configFactory, SqlServerDataSourceFactory.IDENTIFIER);
            firstSource.addSink(new EventQueueSink(beforeSinkId)).name("Event queue sink");
            firstJobClient = firstEnv.executeAsync("SqlServerSchemaRestoreBefore");

            List<Event> firstRoundEvents = drainSinkResults(beforeSinkId, 22);
            assertThat(
                            firstRoundEvents.stream()
                                    .filter(CreateTableEvent.class::isInstance)
                                    .collect(Collectors.toList()))
                    .containsExactly(new CreateTableEvent(tableId, schemaV1));
            assertThat(
                            firstRoundEvents.stream()
                                    .filter(event -> !(event instanceof CreateTableEvent))
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrderElementsOf(getSnapshotExpected(tableId, schemaV1));

            Thread.sleep(2000L);
            String savepointPath = triggerSavepointWithRetry(firstJobClient, savepointDirectory);
            firstJobClient.cancel().get();
            firstJobClient = null;

            StreamExecutionEnvironment restoredEnv =
                    getStreamExecutionEnvironment(savepointPath, 1);
            DataStreamSource<Event> restoredSource =
                    createSource(restoredEnv, configFactory, SqlServerDataSourceFactory.IDENTIFIER);
            restoredSource.addSink(new EventQueueSink(afterSinkId)).name("Event queue sink");
            restoredJobClient = restoredEnv.executeAsync("SqlServerSchemaRestoreAfter");

            Thread.sleep(5000L);

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("USE " + DATABASE_NAME);
                statement.execute("ALTER TABLE dbo.customers ADD ext INT");
                statement.execute(
                        "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                                + "@source_name = 'customers', @role_name = NULL, "
                                + "@supports_net_changes = 0, @capture_instance = 'dbo_customers_v2';");
                statement.execute(
                        "INSERT INTO dbo.customers VALUES "
                                + "(10000, 'Alice', 'Beijing', '123567891234', 17);");
            }

            assertThat(drainSinkResults(afterSinkId, 3))
                    .containsExactly(
                            new AddColumnEvent(
                                    tableId,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    new PhysicalColumn(
                                                            "ext", DataTypes.INT(), null),
                                                    AddColumnEvent.ColumnPosition.AFTER,
                                                    "phone_number"))),
                            new CreateTableEvent(tableId, schemaV2),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    generate(
                                            schemaV2,
                                            10000,
                                            "Alice",
                                            "Beijing",
                                            "123567891234",
                                            17)));

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("USE " + DATABASE_NAME);
                statement.execute(
                        "EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', "
                                + "@source_name = 'customers', "
                                + "@capture_instance = 'dbo_customers';");
                statement.execute("ALTER TABLE dbo.customers ALTER COLUMN ext FLOAT");
                statement.execute(
                        "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                                + "@source_name = 'customers', @role_name = NULL, "
                                + "@supports_net_changes = 0, @capture_instance = 'dbo_customers_v3';");
                statement.execute(
                        "INSERT INTO dbo.customers VALUES "
                                + "(10001, 'Bob', 'Chongqing', '123567891234', 2.718281828);");
            }

            assertThat(drainSinkResults(afterSinkId, 2))
                    .containsExactly(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    Collections.singletonMap("ext", (DataType) DataTypes.DOUBLE()),
                                    Collections.singletonMap("ext", (DataType) DataTypes.INT()),
                                    Collections.emptyMap()),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    generate(
                                            schemaV3,
                                            10001,
                                            "Bob",
                                            "Chongqing",
                                            "123567891234",
                                            2.718281828)));

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("USE " + DATABASE_NAME);
                statement.execute(
                        "EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', "
                                + "@source_name = 'customers', "
                                + "@capture_instance = 'dbo_customers_v2';");
                statement.execute("ALTER TABLE dbo.customers DROP COLUMN ext");
                statement.execute(
                        "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                                + "@source_name = 'customers', @role_name = NULL, "
                                + "@supports_net_changes = 0, @capture_instance = 'dbo_customers_v4';");
                statement.execute(
                        "INSERT INTO dbo.customers VALUES "
                                + "(10002, 'Cicada', 'Urumqi', '123567891234');");
            }

            assertThat(drainSinkResults(afterSinkId, 2))
                    .containsExactly(
                            new DropColumnEvent(tableId, Collections.singletonList("ext")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    generate(schemaV4, 10002, "Cicada", "Urumqi", "123567891234")));
        } finally {
            if (restoredJobClient != null) {
                restoredJobClient.cancel().get();
            }
            if (firstJobClient != null) {
                firstJobClient.cancel().get();
            }
        }
    }

    private DataStreamSource<Event> createSource(
            StreamExecutionEnvironment env,
            SqlServerSourceConfigFactory configFactory,
            String sourceName) {
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new SqlServerDataSource(configFactory).getEventSourceProvider();
        return env.fromSource(
                sourceProvider.getSource(),
                WatermarkStrategy.noWatermarks(),
                sourceName,
                new EventTypeInfo());
    }

    private List<Event> drainSinkResults(String sinkId, int size)
            throws InterruptedException, TimeoutException {
        BlockingQueue<Event> queue = Objects.requireNonNull(EVENTS_BY_SINK.get(sinkId));
        List<Event> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Event event = queue.poll(60L, TimeUnit.SECONDS);
            if (event == null) {
                throw new TimeoutException(
                        String.format(
                                "Timed out waiting for event %d/%d from sink %s.",
                                i + 1, size, sinkId));
            }
            result.add(event);
        }
        return result;
    }

    private Schema schemaV1() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                .physicalColumn("address", DataTypes.VARCHAR(1024))
                .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                .primaryKey(Collections.singletonList("id"))
                .build();
    }

    private Schema schemaV2() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                .physicalColumn("address", DataTypes.VARCHAR(1024))
                .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                .physicalColumn("ext", DataTypes.INT())
                .primaryKey(Collections.singletonList("id"))
                .build();
    }

    private Schema schemaV3() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                .physicalColumn("address", DataTypes.VARCHAR(1024))
                .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                .physicalColumn("ext", DataTypes.DOUBLE())
                .primaryKey(Collections.singletonList("id"))
                .build();
    }

    private Schema schemaV4() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                .physicalColumn("address", DataTypes.VARCHAR(1024))
                .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                .primaryKey(Collections.singletonList("id"))
                .build();
    }

    private List<Event> getSnapshotExpected(TableId tableId, Schema schema) {
        return Stream.of(
                        generate(schema, 101, "user_1", "Shanghai", "123567891234"),
                        generate(schema, 102, "user_2", "Shanghai", "123567891234"),
                        generate(schema, 103, "user_3", "Shanghai", "123567891234"),
                        generate(schema, 109, "user_4", "Shanghai", "123567891234"),
                        generate(schema, 110, "user_5", "Shanghai", "123567891234"),
                        generate(schema, 111, "user_6", "Shanghai", "123567891234"),
                        generate(schema, 118, "user_7", "Shanghai", "123567891234"),
                        generate(schema, 121, "user_8", "Shanghai", "123567891234"),
                        generate(schema, 123, "user_9", "Shanghai", "123567891234"),
                        generate(schema, 1009, "user_10", "Shanghai", "123567891234"),
                        generate(schema, 1010, "user_11", "Shanghai", "123567891234"),
                        generate(schema, 1011, "user_12", "Shanghai", "123567891234"),
                        generate(schema, 1012, "user_13", "Shanghai", "123567891234"),
                        generate(schema, 1013, "user_14", "Shanghai", "123567891234"),
                        generate(schema, 1014, "user_15", "Shanghai", "123567891234"),
                        generate(schema, 1015, "user_16", "Shanghai", "123567891234"),
                        generate(schema, 1016, "user_17", "Shanghai", "123567891234"),
                        generate(schema, 1017, "user_18", "Shanghai", "123567891234"),
                        generate(schema, 1018, "user_19", "Shanghai", "123567891234"),
                        generate(schema, 1019, "user_20", "Shanghai", "123567891234"),
                        generate(schema, 2000, "user_21", "Shanghai", "123567891234"))
                .map(record -> DataChangeEvent.insertEvent(tableId, record))
                .collect(Collectors.toList());
    }

    private BinaryRecordData generate(Schema schema, Object... fields) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        return generator.generate(
                Stream.of(fields)
                        .map(
                                field ->
                                        field instanceof String
                                                ? BinaryStringData.fromString((String) field)
                                                : field)
                        .toArray());
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        while (retryTimes < 600) {
            try {
                return jobClient
                        .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, CheckpointException.class)
                        .filter(
                                exception ->
                                        exception
                                                .getMessage()
                                                .contains("Checkpoint triggering task"))
                        .isPresent()) {
                    Thread.sleep(100L);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        throw new AssertionError(
                String.format(
                        "Failed to trigger savepoint in directory '%s' after %d retries.",
                        savepointDirectory, retryTimes));
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String savepointPath, int parallelism) {
        Configuration configuration = new Configuration();
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(500L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 1000L);
        return env;
    }

    private static class EventQueueSink implements SinkFunction<Event> {

        private final String sinkId;

        private EventQueueSink(String sinkId) {
            this.sinkId = sinkId;
        }

        @Override
        public void invoke(Event value, Context context) {
            Objects.requireNonNull(EVENTS_BY_SINK.get(sinkId)).add(value);
        }
    }
}
