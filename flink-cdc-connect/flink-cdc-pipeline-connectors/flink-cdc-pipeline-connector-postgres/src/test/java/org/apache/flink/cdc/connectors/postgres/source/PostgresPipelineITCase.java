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
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.AbstractCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CheckpointedCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIteratorAdapter;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for Postgres source. */
public class PostgresPipelineITCase extends PostgresTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPipelineITCase.class);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER, "inventory", "inventory", TEST_USER, TEST_PASSWORD);
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private String slotName;

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        inventoryDatabase.removeSlot(slotName);
    }

    static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(false, false),
                Arguments.of(true, false),
                Arguments.of(false, true));
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryDatabase.getDatabaseName())
                                .tableList("inventory.products")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(inventoryDatabase.getDatabaseName());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId("inventory", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(inventoryDatabase.checkSlot(slotName)).isEqualTo(slotName);
    }

    @Test
    public void testLatestOffsetStartupMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryDatabase.getDatabaseName())
                                .tableList("inventory.products")
                                .startupOptions(StartupOptions.latest())
                                .serverTimeZone("UTC");
        configFactory.database(inventoryDatabase.getDatabaseName());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");

        // Create a temporary directory for savepoint
        Path savepointDir = Files.createTempDirectory("postgres-savepoint-test");
        final String savepointDirectory = savepointDir.toAbsolutePath().toString();
        String finishedSavePointPath = null;

        // Listen to tables first time
        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 4);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();

        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
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

        JobClient jobClient = env.executeAsync("beforeSavepoint");
        iterator.setJobClient(jobClient);

        // Insert two records while the pipeline is running
        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, inventoryDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "INSERT INTO inventory.products (name, description, weight) "
                            + "VALUES ('scooter', 'Small 2-wheel scooter', 3.14)");
            stmt.execute(
                    "INSERT INTO inventory.products (name, description, weight) "
                            + "VALUES ('football', 'A leather football', 0.45)");
        }

        // Wait for the pipeline to process the insert events
        Thread.sleep(5000);

        // Trigger a savepoint and cancel the job
        LOG.info("Triggering savepoint");
        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        LOG.info("Savepoint created at: {}", finishedSavePointPath);
        jobClient.cancel().get();
        iterator.close();

        // Restore from savepoint
        LOG.info("Restoring from savepoint: {}", finishedSavePointPath);
        StreamExecutionEnvironment restoredEnv =
                getStreamExecutionEnvironment(finishedSavePointPath, 4);
        FlinkSourceProvider restoredSourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();

        DataStreamSource<Event> restoredSource =
                restoredEnv.fromSource(
                        restoredSourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> restoredSerializer =
                restoredSource
                        .getTransformation()
                        .getOutputType()
                        .createSerializer(restoredEnv.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> restoredResultBuffer =
                new CheckpointedCollectResultBuffer<>(restoredSerializer);
        String restoredAccumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> restoredIterator =
                addCollector(
                        restoredEnv,
                        restoredSource,
                        restoredResultBuffer,
                        restoredSerializer,
                        restoredAccumulatorName);

        JobClient restoredJobClient = restoredEnv.executeAsync("afterSavepoint");
        restoredIterator.setJobClient(restoredJobClient);

        // Insert data into the table after restoration
        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, inventoryDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "INSERT INTO inventory.products (name, description, weight) "
                            + "VALUES ('new_product_1', 'New product description', 1.0)");
        }

        // Wait for the pipeline to stabilize and process events
        Thread.sleep(10000);

        // Fetch results and check for CreateTableEvent and data change events
        List<Event> restoreAfterEvents = new ArrayList<>();
        while (restoreAfterEvents.size() < 2 && restoredIterator.hasNext()) {
            restoreAfterEvents.add(restoredIterator.next());
        }
        restoredIterator.close();
        restoredJobClient.cancel().get();

        // Check if CreateTableEvent for new_products is present
        boolean hasCreateTableEvent =
                restoreAfterEvents.stream().anyMatch(event -> event instanceof CreateTableEvent);
        assertThat(hasCreateTableEvent).isTrue();

        // Check if data change event for new_products is present
        boolean hasProductDataEvent =
                restoreAfterEvents.stream().anyMatch(event -> event instanceof DataChangeEvent);
        assertThat(hasProductDataEvent).isTrue();
    }

    // Helper method to trigger a savepoint with retry mechanism
    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws Exception {
        int retryCount = 0;
        final int maxRetries = 600;
        while (retryCount < maxRetries) {
            try {
                return jobClient
                        .stopWithSavepoint(true, savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                retryCount++;
                LOG.error(
                        "Retry {}/{}: Failed to trigger savepoint: {}",
                        retryCount,
                        maxRetries,
                        e.getMessage());
                if (retryCount >= maxRetries) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
        throw new Exception("Failed to trigger savepoint after " + maxRetries + " retries");
    }

    // Helper method to get a configured StreamExecutionEnvironment
    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) {
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();
        if (finishedSavePointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(500L);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        return env;
    }

    // Helper method to add a collector sink and get the iterator
    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env,
            DataStreamSource<T> source,
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

    @ParameterizedTest
    @MethodSource("provideParameters")
    public void testInitialStartupModeWithOpts(
            boolean unboundedChunkFirst, boolean isTableIdIncludeDatabase) throws Exception {
        inventoryDatabase.createAndInitialize();
        org.apache.flink.cdc.common.configuration.Configuration sourceConfiguration =
                new org.apache.flink.cdc.common.configuration.Configuration();
        sourceConfiguration.set(PostgresDataSourceOptions.HOSTNAME, POSTGRES_CONTAINER.getHost());
        sourceConfiguration.set(
                PostgresDataSourceOptions.PG_PORT,
                POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT));
        sourceConfiguration.set(PostgresDataSourceOptions.USERNAME, TEST_USER);
        sourceConfiguration.set(PostgresDataSourceOptions.PASSWORD, TEST_PASSWORD);
        sourceConfiguration.set(PostgresDataSourceOptions.SLOT_NAME, slotName);
        sourceConfiguration.set(PostgresDataSourceOptions.DECODING_PLUGIN_NAME, "pgoutput");
        sourceConfiguration.set(
                PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP, false);
        sourceConfiguration.set(
                PostgresDataSourceOptions.TABLES,
                inventoryDatabase.getDatabaseName() + ".inventory.products");
        sourceConfiguration.set(PostgresDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfiguration.set(PostgresDataSourceOptions.METADATA_LIST, "op_ts");
        sourceConfiguration.set(
                PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED,
                unboundedChunkFirst);
        sourceConfiguration.set(
                PostgresDataSourceOptions.TABLE_ID_INCLUDE_DATABASE, isTableIdIncludeDatabase);

        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration,
                        new org.apache.flink.cdc.common.configuration.Configuration(),
                        this.getClass().getClassLoader());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId;
        if (isTableIdIncludeDatabase) {
            tableId = TableId.tableId(inventoryDatabase.getDatabaseName(), "inventory", "products");
        } else {
            tableId = TableId.tableId("inventory", "products");
        }
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("op_ts", "0");

        // generate snapshot data
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(),
                                            dataChangeEvent.after(),
                                            meta);
                                })
                        .collect(Collectors.toList());

        String startTime = String.valueOf(System.currentTimeMillis());
        Thread.sleep(1000);

        List<Event> expectedlog = new ArrayList<>();

        try (Connection connection =
                        getJdbcConnection(POSTGRES_CONTAINER, inventoryDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(45),
                                DataTypes.DOUBLE()
                            },
                            new String[] {"id", "name", "description", "weight"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            statement.execute(
                    String.format(
                            "INSERT INTO %s VALUES (default,'scooter','c-2',5.5);",
                            "inventory.products")); // 110
            expectedlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO %s VALUES (default,'football','c-11',6.6);",
                            "inventory.products")); // 111
            expectedlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE %s SET description='c-12' WHERE id=110;",
                            "inventory.products"));

            expectedlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-12"),
                                        5.5
                                    })));

            statement.execute(
                    String.format("DELETE FROM %s WHERE id = 111;", "inventory.products"));
            expectedlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6
                                    })));
        }

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        int snapshotRecordsCount = expectedSnapshot.size();
        int logRecordsCount = expectedlog.size();

        // Ditto, CreateTableEvent might be emitted in multiple partitions.
        List<Event> actual =
                fetchResultsExcept(
                        events, snapshotRecordsCount + logRecordsCount, createTableEvent);

        List<Event> actualSnapshotEvents = actual.subList(0, snapshotRecordsCount);
        List<Event> actuallogEvents = actual.subList(snapshotRecordsCount, actual.size());

        assertThat(actualSnapshotEvents).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        assertThat(actuallogEvents).hasSize(logRecordsCount);

        for (int i = 0; i < logRecordsCount; i++) {
            if (expectedlog.get(i) instanceof SchemaChangeEvent) {
                assertThat(actuallogEvents.get(i)).isEqualTo(expectedlog.get(i));
            } else {
                DataChangeEvent expectedEvent = (DataChangeEvent) expectedlog.get(i);
                DataChangeEvent actualEvent = (DataChangeEvent) actuallogEvents.get(i);
                assertThat(actualEvent.op()).isEqualTo(expectedEvent.op());
                assertThat(actualEvent.before()).isEqualTo(expectedEvent.before());
                assertThat(actualEvent.after()).isEqualTo(expectedEvent.after());
                assertThat(actualEvent.meta().get("op_ts")).isGreaterThanOrEqualTo(startTime);
            }
        }
    }

    @Test
    public void testSnapshotOnlyMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryDatabase.getDatabaseName())
                                .tableList("inventory.products")
                                .startupOptions(StartupOptions.snapshot())
                                .skipSnapshotBackfill(false)
                                .serverTimeZone("UTC");
        configFactory.database(inventoryDatabase.getDatabaseName());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId("inventory", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        Thread.sleep(10000);
        assertThat(inventoryDatabase.checkSlot(slotName))
                .isEqualTo(String.format("Replication slot \"%s\" does not exist", slotName));
    }

    @Test
    public void testSchemaChangeWithDataInserts() throws Exception {
        inventoryDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryDatabase.getDatabaseName())
                                .tableList("inventory.products")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(inventoryDatabase.getDatabaseName());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");
        configFactory.enableSchemaChange(true);

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();

        StreamExecutionEnvironment testEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        testEnv.setParallelism(1);
        testEnv.enableCheckpointing(1000);

        DataStreamSource<Event> source =
                testEnv.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation().getOutputType().createSerializer(testEnv.getConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(testEnv, source, resultBuffer, serializer, accumulatorName);

        JobClient jobClient = testEnv.executeAsync("testSchemaChangeWithDataInserts");
        iterator.setJobClient(jobClient);

        try {
            // Wait for snapshot phase to complete (9 rows in products)
            List<Event> snapshotEvents =
                    fetchEvent(iterator, 9, (event) -> !(event instanceof CreateTableEvent));
            assertThat(snapshotEvents).hasSize(9);

            // Wait for stream phase to stabilize
            Thread.sleep(1000);
            TableId tableId = TableId.tableId("inventory", "products");

            // Build expected event list in order
            List<Event> expectedLog = new ArrayList<>();

            // --- Original schema: (id, name, description, weight) ---
            RowType rowType1 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.DOUBLE()
                            },
                            new String[] {"id", "name", "description", "weight"});
            BinaryRecordDataGenerator gen1 = new BinaryRecordDataGenerator(rowType1);

            // --- After ADD COLUMN: (id, name, description, weight, category) ---
            RowType rowType2 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.DOUBLE(),
                                DataTypes.VARCHAR(100)
                            },
                            new String[] {"id", "name", "description", "weight", "category"});
            BinaryRecordDataGenerator gen2 = new BinaryRecordDataGenerator(rowType2);

            // --- After DROP COLUMN 'description': (id, name, weight, category) ---
            RowType rowType3 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.DOUBLE(),
                                DataTypes.VARCHAR(100)
                            },
                            new String[] {"id", "name", "weight", "category"});
            BinaryRecordDataGenerator gen3 = new BinaryRecordDataGenerator(rowType3);

            // --- After ALTER COLUMN TYPE weight -> DECIMAL(10,2): (id, name, weight, category) ---
            RowType rowType4 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.VARCHAR(100)
                            },
                            new String[] {"id", "name", "weight", "category"});
            BinaryRecordDataGenerator gen4 = new BinaryRecordDataGenerator(rowType4);

            // --- After RENAME COLUMN category -> product_category: (id, name, weight,
            // product_category) ---
            RowType rowType5 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.VARCHAR(100)
                            },
                            new String[] {"id", "name", "weight", "product_category"});
            BinaryRecordDataGenerator gen5 = new BinaryRecordDataGenerator(rowType5);

            try (Connection conn =
                            getJdbcConnection(
                                    POSTGRES_CONTAINER, inventoryDatabase.getDatabaseName());
                    Statement stmt = conn.createStatement()) {

                // Insert before ADD COLUMN (id=110)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'before_add_col', 'desc1', 1.0)");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen1.generate(
                                        new Object[] {
                                            110,
                                            BinaryStringData.fromString("before_add_col"),
                                            BinaryStringData.fromString("desc1"),
                                            1.0
                                        })));

                // ADD COLUMN category
                stmt.execute("ALTER TABLE inventory.products ADD COLUMN category VARCHAR(100)");
                expectedLog.add(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                org.apache.flink.cdc.common.schema.Column
                                                        .physicalColumn(
                                                                "category",
                                                                DataTypes.VARCHAR(100),
                                                                null,
                                                                null)))));

                // Insert after ADD COLUMN (id=111)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'after_add_col', 'desc2', 2.0, 'electronics')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen2.generate(
                                        new Object[] {
                                            111,
                                            BinaryStringData.fromString("after_add_col"),
                                            BinaryStringData.fromString("desc2"),
                                            2.0,
                                            BinaryStringData.fromString("electronics")
                                        })));

                // Insert before DROP COLUMN (id=112)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'before_drop_col', 'desc3', 3.0, 'books')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen2.generate(
                                        new Object[] {
                                            112,
                                            BinaryStringData.fromString("before_drop_col"),
                                            BinaryStringData.fromString("desc3"),
                                            3.0,
                                            BinaryStringData.fromString("books")
                                        })));

                // DROP COLUMN description
                stmt.execute("ALTER TABLE inventory.products DROP COLUMN description");
                expectedLog.add(
                        new DropColumnEvent(tableId, Collections.singletonList("description")));

                // Insert after DROP COLUMN (id=113)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'after_drop_col', 4.0, 'toys')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen3.generate(
                                        new Object[] {
                                            113,
                                            BinaryStringData.fromString("after_drop_col"),
                                            4.0,
                                            BinaryStringData.fromString("toys")
                                        })));

                // Insert before ALTER COLUMN TYPE (id=114)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'before_alter_type', 5.0, 'food')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen3.generate(
                                        new Object[] {
                                            114,
                                            BinaryStringData.fromString("before_alter_type"),
                                            5.0,
                                            BinaryStringData.fromString("food")
                                        })));

                // ALTER COLUMN TYPE weight -> DECIMAL(10,2)
                stmt.execute(
                        "ALTER TABLE inventory.products ALTER COLUMN weight TYPE DECIMAL(10,2)");
                expectedLog.add(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("weight", DataTypes.DECIMAL(10, 2))));

                // Insert after ALTER COLUMN TYPE (id=115)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'after_alter_type', 6.00, 'sports')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen4.generate(
                                        new Object[] {
                                            115,
                                            BinaryStringData.fromString("after_alter_type"),
                                            DecimalData.fromBigDecimal(
                                                    new java.math.BigDecimal("6.00"), 10, 2),
                                            BinaryStringData.fromString("sports")
                                        })));

                // Insert before RENAME COLUMN (id=116)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'before_rename_col', 7.00, 'clothing')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen4.generate(
                                        new Object[] {
                                            116,
                                            BinaryStringData.fromString("before_rename_col"),
                                            DecimalData.fromBigDecimal(
                                                    new java.math.BigDecimal("7.00"), 10, 2),
                                            BinaryStringData.fromString("clothing")
                                        })));

                // RENAME COLUMN category -> product_category
                stmt.execute(
                        "ALTER TABLE inventory.products RENAME COLUMN category TO product_category");
                expectedLog.add(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("category", "product_category")));

                // Insert after RENAME COLUMN (id=117)
                stmt.execute(
                        "INSERT INTO inventory.products VALUES (default, 'after_rename_col', 8.00, 'garden')");
                expectedLog.add(
                        DataChangeEvent.insertEvent(
                                tableId,
                                gen5.generate(
                                        new Object[] {
                                            117,
                                            BinaryStringData.fromString("after_rename_col"),
                                            DecimalData.fromBigDecimal(
                                                    new java.math.BigDecimal("8.00"), 10, 2),
                                            BinaryStringData.fromString("garden")
                                        })));
            }

            // Collect streaming events (filter out CreateTableEvents)
            List<Event> actualLog = fetchEvent(iterator, expectedLog.size(), (event) -> true);

            assertThat(actualLog).isEqualTo(expectedLog);

        } finally {
            try {
                iterator.close();
                jobClient.cancel().get();
            } catch (Exception e) {
                LOG.warn("Failed to cancel job: {}", e.getMessage());
            }
        }
    }

    @Test
    public void testDatabaseNameWithHyphenEndToEnd() throws Exception {
        // Create a real database with hyphen to verify full CDC sync works
        // This test verifies the fix for FLINK-38512
        String hyphenDbName = "test-db-with-hyphen";

        // Create the database with hyphen (need to quote the name)
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            // Drop if exists and create new database with hyphen in name
            statement.execute("DROP DATABASE IF EXISTS \"" + hyphenDbName + "\"");
            statement.execute("CREATE DATABASE \"" + hyphenDbName + "\"");
        }

        // Connect to the new database and create a table with data
        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        hyphenDbName);
        try (Connection connection =
                        java.sql.DriverManager.getConnection(jdbcUrl, TEST_USER, TEST_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))");
            statement.execute(
                    "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')");
        }

        // Create PostgresDataSource using PostgresSourceConfigFactory
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(hyphenDbName)
                                .tableList("public.test_table")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(hyphenDbName);
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");

        PostgresDataSource dataSource = new PostgresDataSource(configFactory);

        // Verify the configuration works
        assertThat(dataSource.getPostgresSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("public.test_table"));
        assertThat(dataSource.getPostgresSourceConfig().getDatabaseList()).contains(hyphenDbName);

        // Now actually read data using the Flink streaming API
        StreamExecutionEnvironment testEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        testEnv.setParallelism(1);
        testEnv.enableCheckpointing(1000);
        RestartStrategyUtils.configureNoRestartStrategy(testEnv);

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        DataStreamSource<Event> source =
                testEnv.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        TypeSerializer<Event> serializer =
                source.getTransformation()
                        .getOutputType()
                        .createSerializer(testEnv.getConfig().getSerializerConfig());
        CheckpointedCollectResultBuffer<Event> resultBuffer =
                new CheckpointedCollectResultBuffer<>(serializer);
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectResultIterator<Event> iterator =
                addCollector(testEnv, source, resultBuffer, serializer, accumulatorName);

        JobClient jobClient = testEnv.executeAsync("testDatabaseNameWithHyphen");
        iterator.setJobClient(jobClient);

        try {
            // Collect events and verify data
            List<Event> collectedEvents = new ArrayList<>();
            int expectedDataCount = 3; // We inserted 3 rows
            int dataCount = 0;
            int maxEvents = 10; // Safety limit

            while (iterator.hasNext() && collectedEvents.size() < maxEvents) {
                Event event = iterator.next();
                collectedEvents.add(event);
                if (event instanceof DataChangeEvent) {
                    dataCount++;
                    if (dataCount >= expectedDataCount) {
                        break;
                    }
                }
            }

            // Verify we received CreateTableEvent and DataChangeEvents
            assertThat(collectedEvents).isNotEmpty();

            // Check for CreateTableEvent
            long createTableEventCount =
                    collectedEvents.stream().filter(e -> e instanceof CreateTableEvent).count();
            assertThat(createTableEventCount).isGreaterThanOrEqualTo(1);

            // Check for DataChangeEvents (INSERT events from snapshot)
            List<DataChangeEvent> dataChangeEvents =
                    collectedEvents.stream()
                            .filter(e -> e instanceof DataChangeEvent)
                            .map(e -> (DataChangeEvent) e)
                            .collect(Collectors.toList());

            assertThat(dataChangeEvents).hasSize(expectedDataCount);

            // Verify the table ID in events
            for (DataChangeEvent dce : dataChangeEvents) {
                assertThat(dce.tableId().getSchemaName()).isEqualTo("public");
                assertThat(dce.tableId().getTableName()).isEqualTo("test_table");
            }

            // Verify the data content - we should have 3 INSERT events with ids 1, 2, 3
            List<Integer> actualIds =
                    dataChangeEvents.stream()
                            .map(
                                    dce -> {
                                        RecordData after = dce.after();
                                        return after.getInt(0); // id column
                                    })
                            .sorted()
                            .collect(Collectors.toList());
            assertThat(actualIds).containsExactly(1, 2, 3);
        } finally {
            // Cancel the job with a bounded wait so cleanup always runs
            try {
                iterator.close();
                jobClient.cancel().get();
            } catch (Exception e) {
                LOG.warn("Failed to cancel job: {}", e.getMessage());
            }

            // Wait for the job to fully stop and release the replication slot
            Thread.sleep(3000);

            // Cleanup - drop replication slot, terminate connections and drop database
            try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                    Statement statement = connection.createStatement()) {
                try {
                    statement.execute(
                            String.format("SELECT pg_drop_replication_slot('%s')", slotName));
                } catch (SQLException e) {
                    LOG.warn("Failed to drop replication slot: {}", e.getMessage());
                }
                statement.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '"
                                + hyphenDbName
                                + "'");
                Thread.sleep(500);
                statement.execute("DROP DATABASE IF EXISTS \"" + hyphenDbName + "\"");
            }
        }
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (!event.equals(sideEvent)) {
                result.add(event);
                size--;
            } else {
                sideResults.add(sideEvent);
            }
        }
        // Also ensure we've received at least one or many side events.
        assertThat(sideResults).isNotEmpty();
        return result;
    }

    private List<Event> fetchEvent(Iterator<Event> iter, int size, Predicate<Event> predicate) {
        List<Event> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Event event = iter.next();
            if (predicate.test(event)) {
                result.add(event);
                size--;
            }
        }
        return result;
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"id", "name", "description", "weight"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    3.14
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    8.1
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    0.8
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenter's hammer"),
                                    0.75
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenter's hammer"),
                                    0.875
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenter's hammer"),
                                    1.0
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    5.3
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    0.1
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    22.2
                                })));
        return snapshotExpected;
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn(
                                "id",
                                DataTypes.INT().notNull(),
                                null,
                                "nextval(\'inventory.products_id_seq\'::regclass)")
                        .physicalColumn(
                                "name",
                                DataTypes.VARCHAR(255).notNull(),
                                null,
                                "'flink'::character varying")
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }
}
