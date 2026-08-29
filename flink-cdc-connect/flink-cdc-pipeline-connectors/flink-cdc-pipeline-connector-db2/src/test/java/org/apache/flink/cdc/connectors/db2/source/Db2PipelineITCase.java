/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.Db2TestBase;
import org.apache.flink.cdc.connectors.db2.factory.Db2DataSourceFactory;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Db2Container;

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

/** Integration tests for Db2 pipeline source. */
public class Db2PipelineITCase extends Db2TestBase {

    private static final String SCHEMA_NAME = "DB2INST1";

    private static final String TABLE_NAME = "CUSTOMERS";

    private static final TableId TABLE_ID = TableId.tableId(SCHEMA_NAME, TABLE_NAME);

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @SuppressWarnings("deprecation")
    @BeforeEach
    public void before() {
        env.setParallelism(4);
        env.enableCheckpointing(200);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        initializeDb2Table("customers", TABLE_NAME);
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        Db2SourceConfigFactory configFactory =
                (Db2SourceConfigFactory)
                        new Db2SourceConfigFactory()
                                .hostname(DB2_CONTAINER.getHost())
                                .port(DB2_CONTAINER.getMappedPort(Db2Container.DB2_PORT))
                                .username(DB2_CONTAINER.getUsername())
                                .password(DB2_CONTAINER.getPassword())
                                .databaseList(DB2_CONTAINER.getDatabaseName())
                                .tableList(SCHEMA_NAME + "." + TABLE_NAME)
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new Db2DataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                Db2DataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        CreateTableEvent createTableEvent = getCustomersCreateTableEvent();
        List<Event> expectedSnapshot = getSnapshotExpected();

        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    @Test
    public void testInitialStartupModeWithMetadata() throws Exception {
        org.apache.flink.cdc.common.configuration.Configuration sourceConfiguration =
                new org.apache.flink.cdc.common.configuration.Configuration();
        sourceConfiguration.set(Db2DataSourceOptions.HOSTNAME, DB2_CONTAINER.getHost());
        sourceConfiguration.set(
                Db2DataSourceOptions.PORT, DB2_CONTAINER.getMappedPort(Db2Container.DB2_PORT));
        sourceConfiguration.set(Db2DataSourceOptions.USERNAME, DB2_CONTAINER.getUsername());
        sourceConfiguration.set(Db2DataSourceOptions.PASSWORD, DB2_CONTAINER.getPassword());
        sourceConfiguration.set(Db2DataSourceOptions.DATABASE, DB2_CONTAINER.getDatabaseName());
        sourceConfiguration.set(Db2DataSourceOptions.TABLES, SCHEMA_NAME + "." + TABLE_NAME);
        sourceConfiguration.set(Db2DataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfiguration.set(
                Db2DataSourceOptions.METADATA_LIST, "database_name,schema_name,table_name,op_ts");

        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration,
                        new org.apache.flink.cdc.common.configuration.Configuration(),
                        this.getClass().getClassLoader());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new Db2DataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                Db2DataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        CreateTableEvent createTableEvent = getCustomersCreateTableEvent();

        Map<String, String> meta = new HashMap<>();
        meta.put("database_name", DB2_CONTAINER.getDatabaseName());
        meta.put("schema_name", SCHEMA_NAME);
        meta.put("table_name", TABLE_NAME);
        meta.put("op_ts", "0");

        List<Event> expectedSnapshot =
                getSnapshotExpected().stream()
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

        RowType rowType = getCustomersRowType();
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO "
                            + SCHEMA_NAME
                            + "."
                            + TABLE_NAME
                            + " VALUES (1031, 'user_22', 'Berlin', '123567891234')");
            expectedLog.add(
                    DataChangeEvent.insertEvent(
                            TABLE_ID,
                            generator.generate(
                                    new Object[] {
                                        1031,
                                        BinaryStringData.fromString("user_22"),
                                        BinaryStringData.fromString("Berlin"),
                                        BinaryStringData.fromString("123567891234")
                                    })));
            statement.execute(
                    "UPDATE "
                            + SCHEMA_NAME
                            + "."
                            + TABLE_NAME
                            + " SET ADDRESS='Hangzhou' WHERE ID = 1031");
            expectedLog.add(
                    DataChangeEvent.updateEvent(
                            TABLE_ID,
                            generator.generate(
                                    new Object[] {
                                        1031,
                                        BinaryStringData.fromString("user_22"),
                                        BinaryStringData.fromString("Berlin"),
                                        BinaryStringData.fromString("123567891234")
                                    }),
                            generator.generate(
                                    new Object[] {
                                        1031,
                                        BinaryStringData.fromString("user_22"),
                                        BinaryStringData.fromString("Hangzhou"),
                                        BinaryStringData.fromString("123567891234")
                                    })));
            statement.execute("DELETE FROM " + SCHEMA_NAME + "." + TABLE_NAME + " WHERE ID = 1031");
            expectedLog.add(
                    DataChangeEvent.deleteEvent(
                            TABLE_ID,
                            generator.generate(
                                    new Object[] {
                                        1031,
                                        BinaryStringData.fromString("user_22"),
                                        BinaryStringData.fromString("Hangzhou"),
                                        BinaryStringData.fromString("123567891234")
                                    })));
        }

        int snapshotRecordsCount = expectedSnapshot.size();
        int logRecordsCount = expectedLog.size();

        List<Event> actual =
                fetchResultsExcept(
                        events, snapshotRecordsCount + logRecordsCount, createTableEvent);

        List<Event> actualSnapshotEvents = actual.subList(0, snapshotRecordsCount);
        List<Event> actualLogEvents = actual.subList(snapshotRecordsCount, actual.size());

        assertThat(actualSnapshotEvents).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        assertThat(actualLogEvents).hasSize(logRecordsCount);

        for (int i = 0; i < logRecordsCount; i++) {
            DataChangeEvent expectedEvent = (DataChangeEvent) expectedLog.get(i);
            DataChangeEvent actualEvent = (DataChangeEvent) actualLogEvents.get(i);
            assertThat(actualEvent.op()).isEqualTo(expectedEvent.op());
            assertThat(actualEvent.before()).isEqualTo(expectedEvent.before());
            assertThat(actualEvent.after()).isEqualTo(expectedEvent.after());
            assertThat(actualEvent.meta().get("database_name"))
                    .isEqualTo(DB2_CONTAINER.getDatabaseName());
            assertThat(actualEvent.meta().get("schema_name")).isEqualTo(SCHEMA_NAME);
            assertThat(actualEvent.meta().get("table_name")).isEqualTo(TABLE_NAME);
            assertThat(actualEvent.meta().get("op_ts")).isGreaterThanOrEqualTo(startTime);
        }
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

    private static RowType getCustomersRowType() {
        return RowType.of(
                new DataType[] {
                    DataTypes.INT().notNull(),
                    DataTypes.VARCHAR(255).notNull(),
                    DataTypes.VARCHAR(1024),
                    DataTypes.VARCHAR(512)
                },
                new String[] {"ID", "NAME", "ADDRESS", "PHONE_NUMBER"});
    }

    private List<Event> getSnapshotExpected() {
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(getCustomersRowType());
        List<Event> snapshotExpected = new ArrayList<>();
        int[] ids = {
            101, 102, 103, 109, 110, 111, 118, 121, 123, 1009, 1010, 1011, 1012, 1013, 1014, 1015,
            1016, 1017, 1018, 1019, 2000
        };
        for (int i = 0; i < ids.length; i++) {
            snapshotExpected.add(
                    DataChangeEvent.insertEvent(
                            TABLE_ID,
                            generator.generate(
                                    new Object[] {
                                        ids[i],
                                        BinaryStringData.fromString("user_" + (i + 1)),
                                        BinaryStringData.fromString("Shanghai"),
                                        BinaryStringData.fromString("123567891234")
                                    })));
        }
        return snapshotExpected;
    }

    private CreateTableEvent getCustomersCreateTableEvent() {
        return new CreateTableEvent(
                TABLE_ID,
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT().notNull())
                        .physicalColumn("NAME", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("ADDRESS", DataTypes.VARCHAR(1024))
                        .physicalColumn("PHONE_NUMBER", DataTypes.VARCHAR(512))
                        .primaryKey(Collections.singletonList("ID"))
                        .build());
    }
}
