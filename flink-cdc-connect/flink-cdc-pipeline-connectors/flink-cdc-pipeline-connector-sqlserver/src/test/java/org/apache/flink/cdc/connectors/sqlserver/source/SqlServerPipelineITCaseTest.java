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
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Integration tests for SQL Server pipeline source. */
public class SqlServerPipelineITCaseTest extends SqlServerTestBase {
    private static final String DATABASE_NAME = "inventory";

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @SuppressWarnings("deprecation")
    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
        initializeSqlServerTable(DATABASE_NAME);
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.products")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new SqlServerDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SqlServerDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId(DATABASE_NAME, "dbo", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    @ParameterizedTest(name = "unboundedChunkFirst: {0}")
    @ValueSource(booleans = {true, false})
    public void testInitialStartupModeWithOpts(boolean unboundedChunkFirst) throws Exception {
        Configuration sourceConfiguration = new Configuration();
        sourceConfiguration.set(
                SqlServerDataSourceOptions.HOSTNAME, MSSQL_SERVER_CONTAINER.getHost());
        sourceConfiguration.set(
                SqlServerDataSourceOptions.PORT,
                MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT));
        sourceConfiguration.set(
                SqlServerDataSourceOptions.USERNAME, MSSQL_SERVER_CONTAINER.getUsername());
        sourceConfiguration.set(
                SqlServerDataSourceOptions.PASSWORD, MSSQL_SERVER_CONTAINER.getPassword());
        sourceConfiguration.set(
                SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP, false);
        sourceConfiguration.set(SqlServerDataSourceOptions.TABLES, DATABASE_NAME + ".dbo.products");
        sourceConfiguration.set(SqlServerDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfiguration.set(
                SqlServerDataSourceOptions.METADATA_LIST,
                "database_name,schema_name,table_name,op_ts");
        sourceConfiguration.set(
                SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED,
                unboundedChunkFirst);

        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration, new Configuration(), this.getClass().getClassLoader());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new SqlServerDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SqlServerDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId(DATABASE_NAME, "dbo", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("database_name", DATABASE_NAME);
        meta.put("schema_name", "dbo");
        meta.put("table_name", "products");
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

        List<Event> expectedLog = new ArrayList<>();

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);

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
            statement.execute(
                    "INSERT INTO dbo.products(name,description,weight) VALUES ('scooter','c-2',5.5)"); // 110
            expectedLog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5d
                                    })));
            statement.execute(
                    "INSERT INTO dbo.products(name,description,weight) VALUES ('football','c-11',6.6)"); // 111
            expectedLog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6d
                                    })));
            statement.execute("UPDATE dbo.products SET description='c-12' WHERE id=110");

            expectedLog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5d
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-12"),
                                        5.5d
                                    })));

            statement.execute("DELETE FROM dbo.products WHERE id = 111");
            expectedLog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6d
                                    })));
        }

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        int snapshotRecordsCount = expectedSnapshot.size();
        int logRecordsCount = expectedLog.size();

        // Ditto, CreateTableEvent might be emitted in multiple partitions.
        List<Event> actual =
                fetchResultsExcept(
                        events, snapshotRecordsCount + logRecordsCount, createTableEvent);

        List<Event> actualSnapshotEvents = actual.subList(0, snapshotRecordsCount);
        List<Event> actualLogEvents = actual.subList(snapshotRecordsCount, actual.size());

        assertThat(actualSnapshotEvents).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        assertThat(actualLogEvents).hasSize(logRecordsCount);

        for (int i = 0; i < logRecordsCount; i++) {
            if (expectedLog.get(i) instanceof SchemaChangeEvent) {
                assertThat(actualLogEvents.get(i)).isEqualTo(expectedLog.get(i));
            } else {
                DataChangeEvent expectedEvent = (DataChangeEvent) expectedLog.get(i);
                DataChangeEvent actualEvent = (DataChangeEvent) actualLogEvents.get(i);
                assertThat(actualEvent.op()).isEqualTo(expectedEvent.op());
                assertThat(actualEvent.before()).isEqualTo(expectedEvent.before());
                assertThat(actualEvent.after()).isEqualTo(expectedEvent.after());
                assertThat(actualEvent.meta().get("database_name")).isEqualTo(DATABASE_NAME);
                assertThat(actualEvent.meta().get("schema_name")).isEqualTo("dbo");
                assertThat(actualEvent.meta().get("table_name")).isEqualTo("products");
                assertThat(actualEvent.meta().get("op_ts")).isGreaterThanOrEqualTo(startTime);
            }
        }
    }

    @Test
    public void testSnapshotOnlyMode() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.products")
                                .startupOptions(StartupOptions.snapshot())
                                .skipSnapshotBackfill(false)
                                .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new SqlServerDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SqlServerDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId(DATABASE_NAME, "dbo", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (sideEvent.getClass().isInstance(event)) {
                sideResults.add(event);
            } else {
                result.add(event);
                size--;
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
                                    3.14d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    8.1d
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
                                    0.8d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenter's hammer"),
                                    0.75d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenter's hammer"),
                                    0.875d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenter's hammer"),
                                    1.0d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    5.3d
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
                                    0.1d
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    22.2d
                                })));
        return snapshotExpected;
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.FLOAT())
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }
}
