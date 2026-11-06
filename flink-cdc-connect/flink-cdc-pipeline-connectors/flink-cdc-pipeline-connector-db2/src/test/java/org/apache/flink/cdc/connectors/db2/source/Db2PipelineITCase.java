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
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
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
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.CloseableIterator;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.Db2Container;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for Db2 pipeline source. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Db2PipelineITCase extends Db2TestBase {

    private static final String SCHEMA_NAME = "DB2INST1";

    private static final String TABLE_NAME = "CUSTOMERS";

    private static final TableId TABLE_ID = TableId.tableId(SCHEMA_NAME, TABLE_NAME);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private CloseableIterator<Event> activeEvents;

    @AfterEach
    void closeSource() throws Exception {
        if (activeEvents != null) {
            activeEvents.close();
        }
    }

    @SuppressWarnings("deprecation")
    @BeforeEach
    public void before() {
        env.setParallelism(4);
        env.enableCheckpointing(200);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        initializeDb2Table("customers", TABLE_NAME);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInitialStartupMode(boolean includeSchemaChanges) throws Exception {
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
                                .includeSchemaChanges(includeSchemaChanges)
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

        activeEvents = events;
        CreateTableEvent createTableEvent = getCustomersCreateTableEvent();
        List<Event> expectedSnapshot = getSnapshotExpected();

        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    @Test
    void testInitialStartupModeWithMetadata() throws Exception {
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

        activeEvents = events;
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
            // The Db2 connector resolves LSN timestamps with the JVM default time zone,
            // which CI randomizes, so only assert op_ts is a valid change timestamp here.
            assertThat(Long.parseLong(actualEvent.meta().get("op_ts"))).isGreaterThan(0L);
        }
    }

    @Test
    void testFullColumnTypes() throws Exception {
        initializeDb2Table("column_type_test", "FULL_TYPES");
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT().notNull())
                        .physicalColumn("SMALL_C", DataTypes.SMALLINT())
                        .physicalColumn("INT_C", DataTypes.INT())
                        .physicalColumn("BIG_C", DataTypes.BIGINT())
                        .physicalColumn("REAL_C", DataTypes.FLOAT())
                        .physicalColumn("DOUBLE_C", DataTypes.DOUBLE())
                        .physicalColumn("NUMERIC_C", DataTypes.DECIMAL(10, 5))
                        .physicalColumn("DECIMAL_C", DataTypes.DECIMAL(10, 1))
                        .physicalColumn("VARCHAR_C", DataTypes.VARCHAR(200))
                        .physicalColumn("CHAR_C", DataTypes.CHAR(1))
                        .physicalColumn("CHARACTER_C", DataTypes.CHAR(3))
                        .physicalColumn("TIMESTAMP_C", DataTypes.TIMESTAMP(6))
                        .physicalColumn("DATE_C", DataTypes.DATE())
                        .physicalColumn("TIME_C", DataTypes.TIME(0))
                        .physicalColumn("DEFAULT_NUMERIC_C", DataTypes.DECIMAL(5, 0))
                        .physicalColumn("TIMESTAMP_PRECISION_C", DataTypes.TIMESTAMP(9))
                        .physicalColumn("CHAR_BITS_C", DataTypes.BYTES())
                        .physicalColumn("VAR_BITS_C", DataTypes.BYTES())
                        .primaryKey("ID")
                        .build();

        TableId tableId = TableId.tableId(SCHEMA_NAME, "FULL_TYPES");
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, expectedSchema);
        try (CloseableIterator<Event> events =
                executeAndCollectSource(
                        sourceConfigFactory("FULL_TYPES", StartupOptions.initial()))) {
            List<Event> snapshot = fetchResultsExcept(events, 1, createTableEvent);
            DataChangeEvent snapshotEvent = (DataChangeEvent) snapshot.get(0);
            assertThat(snapshotEvent.tableId()).isEqualTo(tableId);
            assertThat(snapshotEvent.op()).isEqualTo(OperationType.INSERT);
            assertFullTypesRow(snapshotEvent.after(), 1, "123.12345");

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO DB2INST1.FULL_TYPES SELECT 2, SMALL_C, INT_C, BIG_C, REAL_C,"
                                + " DOUBLE_C, NUMERIC_C, DECIMAL_C, VARCHAR_C, CHAR_C, CHARACTER_C,"
                                + " TIMESTAMP_C, DATE_C, TIME_C, DEFAULT_NUMERIC_C,"
                                + " TIMESTAMP_PRECISION_C, CHAR_BITS_C, VAR_BITS_C"
                                + " FROM DB2INST1.FULL_TYPES WHERE ID = 1");
                statement.execute(
                        "UPDATE DB2INST1.FULL_TYPES SET NUMERIC_C = 321.54321 WHERE ID = 2");
                statement.execute("DELETE FROM DB2INST1.FULL_TYPES WHERE ID = 2");
            }

            DataChangeEvent insert = nextDataChange(events, createTableEvent);
            assertThat(insert.op()).isEqualTo(OperationType.INSERT);
            assertFullTypesRow(insert.after(), 2, "123.12345");
            DataChangeEvent update = nextDataChange(events, createTableEvent);
            assertThat(update.op()).isEqualTo(OperationType.UPDATE);
            assertFullTypesRow(update.before(), 2, "123.12345");
            assertFullTypesRow(update.after(), 2, "321.54321");
            DataChangeEvent delete = nextDataChange(events, createTableEvent);
            assertThat(delete.op()).isEqualTo(OperationType.DELETE);
            assertFullTypesRow(delete.before(), 2, "321.54321");
        }
    }

    private static DataChangeEvent nextDataChange(
            Iterator<Event> events, CreateTableEvent expectedSchema) {
        assertThat(events.hasNext()).isTrue();
        Event event = events.next();
        while (event instanceof CreateTableEvent) {
            assertThat(event).isEqualTo(expectedSchema);
            assertThat(events.hasNext()).isTrue();
            event = events.next();
        }
        assertThat(event).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent change = (DataChangeEvent) event;
        assertThat(change.tableId()).isEqualTo(expectedSchema.tableId());
        return change;
    }

    private static void assertFullTypesRow(RecordData row, int id, String numericValue) {
        assertThat(row.getArity()).isEqualTo(18);
        assertThat(row.getInt(0)).isEqualTo(id);
        assertThat(row.getShort(1)).isEqualTo((short) 32767);
        assertThat(row.getInt(2)).isEqualTo(65535);
        assertThat(row.getLong(3)).isEqualTo(2147483647L);
        assertThat(row.getFloat(4)).isEqualTo(5.5f);
        assertThat(row.getDouble(5)).isEqualTo(6.6d);
        assertThat(row.getDecimal(6, 10, 5).toBigDecimal()).isEqualByComparingTo(numericValue);
        assertThat(row.getDecimal(7, 10, 1).toBigDecimal()).isEqualByComparingTo("404.4");
        assertThat(row.getString(8)).isEqualTo(BinaryStringData.fromString("Hello World"));
        assertThat(row.getString(9)).isEqualTo(BinaryStringData.fromString("a"));
        assertThat(row.getString(10)).isEqualTo(BinaryStringData.fromString("abc"));
        assertThat(row.getTimestamp(11, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2020, 7, 17, 18, 0, 22, 123_000_000));
        assertThat(row.getDate(12).toLocalDate()).isEqualTo(LocalDate.of(2020, 7, 17));
        assertThat(row.getTime(13).toLocalTime()).isEqualTo(LocalTime.of(18, 0, 22));
        assertThat(row.getDecimal(14, 5, 0).toBigDecimal()).isEqualByComparingTo("500");
        assertThat(row.getTimestamp(15, 9).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2020, 7, 17, 18, 0, 22, 123_456_789));
        assertThat(row.getBinary(16)).isEqualTo(new byte[] {0x41, 0x42, 0x43, 0x44});
        assertThat(row.getBinary(17)).isEqualTo(new byte[] {0x41, 0x42, 0x43, 0x44, 0x45});
    }

    @Test
    void testUnsupportedColumnTypesFailFast() throws Exception {
        initializeDb2Table("unsupported_column_type_test", "UNSUPPORTED_TYPES");
        Db2SourceConfig sourceConfig =
                sourceConfigFactory("UNSUPPORTED_TYPES", StartupOptions.initial()).create(0);
        Db2Dialect dialect = new Db2Dialect(sourceConfig);
        TableId tableId = TableId.tableId(SCHEMA_NAME, "UNSUPPORTED_TYPES");

        try (JdbcConnection jdbc = Db2SchemaUtils.createDb2Connection(sourceConfig)) {
            jdbc.query(
                    "SELECT DECFLOAT16_C, DECFLOAT34_C FROM DB2INST1.UNSUPPORTED_TYPES",
                    rs -> {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("1234.5678");
                        assertThat(rs.getBigDecimal(2)).isEqualByComparingTo("1234.5678");
                    });
            Table table =
                    dialect.queryTableSchema(
                                    jdbc,
                                    Db2SchemaUtils.toDbzTableId(
                                            tableId,
                                            DB2_CONTAINER
                                                    .getDatabaseName()
                                                    .toUpperCase(Locale.ROOT)))
                            .getTable();
            for (String columnName :
                    Arrays.asList(
                            "DECFLOAT16_C",
                            "DECFLOAT34_C",
                            "XML_C",
                            "CLOB_C",
                            "DBCLOB_C",
                            "BLOB_C")) {
                Column column = table.columnWithName(columnName);
                assertThat(column).isNotNull();
                assertThatThrownBy(() -> Db2SchemaUtils.toColumn(column))
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContaining(column.typeName());
            }
            assertThatThrownBy(() -> Db2SchemaUtils.getTableSchema(tableId, jdbc, dialect))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .rootCause()
                    .hasMessageContaining("Doesn't support Db2 type");
        }
    }

    private static Db2SourceConfigFactory sourceConfigFactory(
            String tableName, StartupOptions startupOptions) {
        return (Db2SourceConfigFactory)
                new Db2SourceConfigFactory()
                        .hostname(DB2_CONTAINER.getHost())
                        .port(DB2_CONTAINER.getMappedPort(Db2Container.DB2_PORT))
                        .username(DB2_CONTAINER.getUsername())
                        .password(DB2_CONTAINER.getPassword())
                        .databaseList(DB2_CONTAINER.getDatabaseName())
                        .tableList(SCHEMA_NAME + "." + tableName)
                        .startupOptions(startupOptions)
                        .serverTimeZone("UTC");
    }

    private CloseableIterator<Event> executeAndCollectSource(Db2SourceConfigFactory configFactory)
            throws Exception {
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new Db2DataSource(configFactory).getEventSourceProvider();
        return env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        Db2DataSourceFactory.IDENTIFIER,
                        new EventTypeInfo())
                .executeAndCollect();
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
        assertThat(sideResults)
                .isNotEmpty()
                .allSatisfy(event -> assertThat(event).isEqualTo(sideEvent));
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
                        .physicalColumn("NAME", DataTypes.VARCHAR(255).notNull(), null, "'flink'")
                        .physicalColumn("ADDRESS", DataTypes.VARCHAR(1024))
                        .physicalColumn("PHONE_NUMBER", DataTypes.VARCHAR(512))
                        .primaryKey(Collections.singletonList("ID"))
                        .build());
    }
}
