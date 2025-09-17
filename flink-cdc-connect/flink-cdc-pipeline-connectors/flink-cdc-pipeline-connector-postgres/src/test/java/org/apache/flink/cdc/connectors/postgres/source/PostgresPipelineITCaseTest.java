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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
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
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for Postgres source. */
public class PostgresPipelineITCaseTest extends PostgresTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPipelineITCaseTest.class);

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
        env.setRestartStrategy(RestartStrategies.noRestart());
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws SQLException {
        inventoryDatabase.removeSlot(slotName);
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

    @ParameterizedTest(name = "unboundedChunkFirst: {0}")
    @ValueSource(booleans = {true, false})
    public void testInitialStartupModeWithOpts(boolean unboundedChunkFirst) throws Exception {
        inventoryDatabase.createAndInitialize();
        Configuration sourceConfiguration = new Configuration();
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

        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration, new Configuration(), this.getClass().getClassLoader());
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

        TableId tableId = TableId.tableId("inventory", "products");
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
