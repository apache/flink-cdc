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

package com.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import com.apache.flink.cdc.connectors.oracle.factory.OracleDataSourceFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.DATABASE;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.HOSTNAME;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.METADATA_LIST;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PASSWORD;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PORT;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCHEMALIST;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES;
import static com.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link OracleDataSource}. */
public class OraclePipelineITCase extends OracleSourceTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeEach
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterEach
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void before() throws Exception {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        Connection conn = getJdbcConnectionAsDBA();
        conn.createStatement().execute("GRANT ANALYZE ANY TO " + CONNECTOR_USER);
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        env.setParallelism(1);
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCHEMALIST.key(), "DEBEZIUM");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(CONNECTOR_USER);
        configFactory.password(CONNECTOR_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.schemaList("DEBEZIUM");
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList("DEBEZIUM.PRODUCTS");
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSource(
                                        configFactory,
                                        context.getFactoryConfiguration(),
                                        new String[] {"DEBEZIUM.PRODUCTS"},
                                        new ArrayList<>())
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(10_000);

        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = new ArrayList<>();
        expectedSnapshot.add(createTableEvent);
        expectedSnapshot.addAll(getSnapshotExpected(tableId));

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.FLOAT()
                            },
                            new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO %s.products VALUES (110,'jack','13V jack',5.5)",
                            "debezium")); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("13V jack"),
                                        7.7E-44f
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.products VALUES (111,'football','31o football',6.6f)",
                            "debezium")); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("31o football"),
                                        1.4051863E-36f
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE %s.products SET NAME='peter', WEIGHT=6.7 WHERE id=110",
                            "debezium"));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("13V jack"),
                                        7.7E-44f
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("peter"),
                                        BinaryStringData.fromString("13V jack"),
                                        9.4E-44f
                                    })));
            statement.execute(String.format("DELETE FROM %s.products WHERE id = 111", "debezium"));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("31o football"),
                                        1.4051863E-36f
                                    })));
        }
        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual =
                fetchResultsExcept(
                        events, expectedSnapshot.size() + expectedBinlog.size(), createTableEvent);
        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps = new HashMap<>();
        String tableIdStr = ((CreateTableEvent) actual.get(0)).tableId().toString();
        TableId tableId2 =
                TableId.tableId(
                        tableIdStr.split("\\.")[0].toUpperCase(),
                        tableIdStr.split("\\.")[1].toUpperCase());
        Schema schema = createTableEvent.getSchema();
        fieldGetterMaps.put(tableId2, SchemaUtils.createFieldGetters(schema));
        StringBuilder actualSnapshotStr =
                getResultString(actual.subList(0, expectedSnapshot.size()), fieldGetterMaps, false);
        StringBuilder expectedSnapshotStr =
                getResultString(expectedSnapshot, fieldGetterMaps, false);
        assertThat(actualSnapshotStr.toString()).isEqualTo(expectedSnapshotStr.toString());
        StringBuilder actualBinlogStr =
                getResultString(
                        actual.subList(expectedSnapshot.size(), actual.size()),
                        fieldGetterMaps,
                        false);
        StringBuilder expectedBinlogStr = getResultString(expectedBinlog, fieldGetterMaps, false);
        assertThat(actualBinlogStr.toString()).isEqualTo(expectedBinlogStr.toString());
    }

    @NotNull
    private StringBuilder getResultString(
            List<Event> events,
            Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps,
            boolean metaFlag) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (Event event : events) {
            if (event instanceof DataChangeEvent) {
                list.add(
                        convertEventToStr(
                                event,
                                fieldGetterMaps.get(((ChangeEvent) event).tableId()),
                                metaFlag));
            } else {
                list.add(event.toString());
            }
        }
        Collections.sort(list);
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
            sb.append("\r\n");
        }
        return sb;
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
        //        assertThat(sideResults).isNotEmpty();
        return result;
    }

    public static String convertEventToStr(
            Event event, List<RecordData.FieldGetter> fieldGetters, boolean metaFlag) {
        if (event instanceof SchemaChangeEvent) {
            return event.toString();
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            String eventStr =
                    "DataChangeEvent{"
                            + "tableId="
                            + dataChangeEvent.tableId()
                            + ", before="
                            + getFields(fieldGetters, dataChangeEvent.before())
                            + ", after="
                            + getFields(fieldGetters, dataChangeEvent.after())
                            + ", op="
                            + dataChangeEvent.op()
                            + ", meta="
                            + (metaFlag ? describeMeta() : dataChangeEvent.describeMeta())
                            + '}';
            return eventStr;
        }
        return "Event{}";
    }

    public static String describeMeta() {
        StringBuilder stringBuilder = new StringBuilder("(");
        Map metaMap = new HashMap<String, String>();
        metaMap.put("op_ts", "0");
        stringBuilder.append(metaMap);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    private static List<Object> getFields(
            List<RecordData.FieldGetter> fieldGetters, RecordData recordData) {
        List<Object> fields = new ArrayList<>(fieldGetters.size());
        if (recordData == null) {
            return fields;
        }
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            fields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return fields;
    }

    @Test
    public void testInitialStartupModeWithOpTs() throws Exception {
        env.setParallelism(1);
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCHEMALIST.key(), "DEBEZIUM");
        options.put(METADATA_LIST.key(), "op_ts");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(10_000);
        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        CreateTableEvent createTableEvent =
                getProductsCreateTableEvent(TableId.tableId("debezium", "products"));
        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("op_ts", "0");
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
        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.FLOAT()
                            },
                            new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO %s.PRODUCTS VALUES (110,'scooter','c-10',5.5)",
                            "DEBEZIUM")); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-10"),
                                        7.7E-44f
                                    }),
                            meta));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.PRODUCTS VALUES (111,'football','c-11',6.6)",
                            "DEBEZIUM")); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        9.2E-44f
                                    }),
                            meta));
            statement.execute(
                    String.format(
                            "UPDATE %s.PRODUCTS SET NAME='jack', WEIGHT=6.7 WHERE id=110",
                            "DEBEZIUM"));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-10"),
                                        7.7E-44f
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("c-10"),
                                        9.4E-44f
                                    }),
                            meta));
            statement.execute(String.format("DELETE FROM %s.PRODUCTS WHERE id = 111", "DEBEZIUM"));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        9.2E-44f
                                    }),
                            meta));
        }
        List<Event> actual =
                fetchResults(events, 1 + expectedSnapshot.size() + expectedBinlog.size());
        assertThat(actual.get(0).toString()).isEqualTo(createTableEvent.toString());

        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps = new HashMap<>();
        String tableIdStr = ((CreateTableEvent) actual.get(0)).tableId().toString();
        TableId tableId2 =
                TableId.tableId(
                        tableIdStr.split("\\.")[0].toUpperCase(),
                        tableIdStr.split("\\.")[1].toUpperCase());
        Schema schema = createTableEvent.getSchema();
        fieldGetterMaps.put(tableId2, SchemaUtils.createFieldGetters(schema));
        StringBuilder actualSnapshotStr =
                getResultString(actual.subList(1, 10), fieldGetterMaps, false);
        StringBuilder expectedSnapshotStr =
                getResultString(expectedSnapshot, fieldGetterMaps, false);
        assertThat(actualSnapshotStr.toString()).isEqualTo(expectedSnapshotStr.toString());

        StringBuilder actualBinlogStr =
                getResultString(
                        actual.subList(expectedSnapshot.size() + 1, actual.size()),
                        fieldGetterMaps,
                        true);
        StringBuilder expectedBinlogStr = getResultString(expectedBinlog, fieldGetterMaps, true);
        assertThat(actualBinlogStr.toString()).isEqualTo(expectedBinlogStr.toString());
        List<Event> actualBinlolgs = actual.subList(expectedSnapshot.size() + 1, actual.size());
        for (int i = 0; i < expectedBinlog.size(); i++) {
            if (actualBinlolgs.get(i) instanceof DataChangeEvent) {
                assertThat(
                                Long.parseLong(
                                        ((DataChangeEvent) actualBinlolgs.get(i))
                                                .meta()
                                                .get("op_ts")))
                        .isGreaterThanOrEqualTo(
                                Long.parseLong(
                                        ((DataChangeEvent) expectedBinlog.get(i))
                                                .meta()
                                                .get("op_ts")));
            }
        }
    }

    @Test
    public void testParseAlterStatement() throws Exception {
        env.setParallelism(1);
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCHEMALIST.key(), "DEBEZIUM");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(TEST_USER);
        configFactory.password(TEST_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.schemaList("DEBEZIUM");
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList("DEBEZIUM.PRODUCTS");
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSource(
                                        configFactory,
                                        context.getFactoryConfiguration(),
                                        new String[] {"DEBEZIUM.PRODUCTS"},
                                        new ArrayList<>())
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId));
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(), dataChangeEvent.after());
                                })
                        .collect(Collectors.toList());
        expected.addAll(expectedSnapshot);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expected.addAll(executeAlterAndProvideExpected(tableId, statement));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS1 VARCHAR(45)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS2 VARCHAR(55)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS1", DataTypes.VARCHAR(45))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS2", DataTypes.VARCHAR(55))))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS3 VARCHAR(45)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS4 VARCHAR(55)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS3", DataTypes.VARCHAR(45))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS4", DataTypes.VARCHAR(55))))));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS5 LONG NULL", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS5", DataTypes.BIGINT())))));
        }
        List<Event> actual = fetchResults(events, expected.size());
        List<String> actualStr =
                actual.stream().map(e -> e.toString()).collect(Collectors.toList());
        List<String> expectedStr =
                expected.stream().map(e -> e.toString()).collect(Collectors.toList());
        assertThat(actualStr)
                .containsExactlyInAnyOrder(expectedStr.toArray(new String[expectedStr.size()]));
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(255))
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.FLOAT())
                        .primaryKey(Arrays.asList("id"))
                        .build());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.FLOAT()
                        },
                        new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101L,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    4.4E-43f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102L,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    1.14E-43f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103L,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    1.1E-44f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenters hammer"),
                                    1.05E-43f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenters hammer"),
                                    1.226E-42f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenters hammer"),
                                    1.4E-45f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107L,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    7.4E-44f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108L,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    1.4E-45f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109L,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    3.11E-43f
                                })));
        return snapshotExpected;
    }

    /**
     * * The final schema of table products is as follows.
     *
     * <pre>
     * CREATE TABLE products (
     *   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
     *   name VARCHAR(255) NOT NULL DEFAULT 'flink',
     *   weight FLOAT,
     *   col1 VARCHAR(45),
     *   col2 VARCHAR(55)
     * );
     * </pre>
     */
    private List<Event> executeAlterAndProvideExpected(TableId tableId, Statement statement)
            throws SQLException {
        List<Event> expected = new ArrayList<>();
        statement.execute(
                String.format(
                        "alter table %s.products modify DESCRIPTION VARCHAR(255) DEFAULT NULL",
                        "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products RENAME COLUMN DESCRIPTION TO DESCPT", "debezium"));

        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESCRIPTION", DataTypes.VARCHAR(255))));
        expected.add(
                new RenameColumnEvent(tableId, Collections.singletonMap("DESCRIPTION", "DESCPT")));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products MODIFY DESCPT VARCHAR(400) DEFAULT NULL",
                        "debezium"));
        statement.execute(
                String.format("ALTER TABLE %s.products RENAME COLUMN DESCPT TO DESC2", "debezium"));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESCPT", DataTypes.VARCHAR(400))));
        expected.add(new RenameColumnEvent(tableId, Collections.singletonMap("DESCPT", "DESC2")));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD DESC1 VARCHAR(45) DEFAULT NULL", "debezium"));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("DESC1", DataTypes.VARCHAR(45))))));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD COL1 VARCHAR(45) DEFAULT NULL", "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD COL2 VARCHAR(55) DEFAULT NULL", "debezium"));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("COL1", DataTypes.VARCHAR(45))))));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("COL2", DataTypes.VARCHAR(55))))));

        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN DESC1", "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products MODIFY DESC2 VARCHAR(65) DEFAULT NULL",
                        "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("DESC1")));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESC2", DataTypes.VARCHAR(65))));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products RENAME COLUMN DESC2 TO DESCRIPTION", "debezium"));
        expected.add(
                new RenameColumnEvent(tableId, Collections.singletonMap("DESC2", "DESCRIPTION")));

        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN COL1", "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("COL1")));
        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN COL2", "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("COL2")));

        // Should not catch SchemaChangeEvent of tables other than products
        statement.execute(
                String.format(
                        "ALTER TABLE %s.category ADD DESC1 VARCHAR(45) DEFAULT NULL", "debezium"));
        return expected;
    }

    public static <T> List<T> fetchResults(Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            result.add(event);
            size--;
        }
        return result;
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
