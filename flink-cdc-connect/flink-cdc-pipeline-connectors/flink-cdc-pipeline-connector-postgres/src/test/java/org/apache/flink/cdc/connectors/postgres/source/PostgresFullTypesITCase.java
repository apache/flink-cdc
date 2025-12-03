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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryMapData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Test cases for {@link PostgresTestBase} with full types. */
public class PostgresFullTypesITCase extends PostgresTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresFullTypesITCase.class);

    /** Use postgis plugin to test the GIS type. */
    protected static final DockerImageName POSTGIS_IMAGE =
            DockerImageName.parse("postgis/postgis:14-3.5").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGIS_CONTAINER =
            new PostgreSQLContainer<>(POSTGIS_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_wal_senders=20");

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private String slotName;

    @BeforeAll
    static void startContainers() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGIS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping containers...");
        if (POSTGIS_CONTAINER != null) {
            POSTGIS_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void before() {
        POSTGIS_CONTAINER.setWaitStrategy(
                Wait.forLogMessage(".*database system is ready to accept connections.*", 1));
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws SQLException {
        String sql = String.format("SELECT pg_drop_replication_slot('%s')", slotName);
        try (Connection connection =
                        PostgresTestBase.getJdbcConnection(POSTGIS_CONTAINER, "postgres");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            LOG.error("Drop replication slot failed.", e);
        }
    }

    @Test
    public void testFullTypes() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.full_types")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    new byte[] {50},
                    (short) 32767,
                    65535,
                    2147483647L,
                    DecimalData.fromBigDecimal(new BigDecimal("5.5"), 2, 1)
                            .toBigDecimal()
                            .floatValue(),
                    DecimalData.fromBigDecimal(new BigDecimal("6.6"), 2, 1)
                            .toBigDecimal()
                            .doubleValue(),
                    DecimalData.fromBigDecimal(new BigDecimal("123.12345"), 10, 5),
                    DecimalData.fromBigDecimal(new BigDecimal("404.4"), 5, 1),
                    true,
                    BinaryStringData.fromString("Hello World"),
                    BinaryStringData.fromString("a"),
                    BinaryStringData.fromString("abc"),
                    BinaryStringData.fromString("abcd..xyz"),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")),
                    DateData.fromEpochDay(18460),
                    TimeData.fromMillisOfDay(64822000),
                    DecimalData.fromBigDecimal(new BigDecimal("500"), 10, 0),
                    BinaryStringData.fromString(
                            "{\"coordinates\":\"[[174.9479,-36.7208]]\",\"type\":\"Point\",\"srid\":3187}"),
                    BinaryStringData.fromString(
                            "{\"coordinates\":\"[[169.1321,-44.7032],[167.8974,-44.6414]]\",\"type\":\"MultiLineString\",\"srid\":4326}"),
                    true,
                    new byte[] {10},
                    new byte[] {42},
                    BinaryStringData.fromString("abc"),
                    1209600000000L,
                    BinaryStringData.fromString(
                            "{\"order_id\": 10248, \"product\": \"Notebook\", \"quantity\": 5}"),
                    BinaryStringData.fromString(
                            "{\"product\": \"Pen\", \"order_id\": 10249, \"quantity\": 10}"),
                    BinaryStringData.fromString(
                            "<user>\n"
                                    + "<id>123</id>\n"
                                    + "<name>Alice</name>\n"
                                    + "<email>alice@example.com</email>\n"
                                    + "<preferences>\n"
                                    + "<theme>dark</theme>\n"
                                    + "<notifications>true</notifications>\n"
                                    + "</preferences>\n"
                                    + "</user>"),
                    BinaryStringData.fromString(
                            "{\"coordinates\":\"[[3.456,7.89]]\",\"type\":\"Point\",\"srid\":0}"),
                    BinaryStringData.fromString("foo.bar.baz"),
                    BinaryStringData.fromString("JohnDoe"),
                    BinaryStringData.fromString("{\"size\":\"L\",\"color\":\"blue\"}"),
                    BinaryStringData.fromString("192.168.1.1"),
                    BinaryStringData.fromString("[1,10)"),
                    BinaryStringData.fromString("[1000000000,5000000000)"),
                    BinaryStringData.fromString("[5.5,20.75)"),
                    BinaryStringData.fromString(
                            "[\"2023-08-01 08:00:00\",\"2023-08-01 12:00:00\")"),
                    BinaryStringData.fromString("[2023-08-01,2023-08-15)"),
                    BinaryStringData.fromString("pending"),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(recordFields(snapshotRecord, COMMON_TYPES))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testTimeTypesWithTemporalModeAdaptive() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("time.precision.mode", "adaptive");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.time_types")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2,
                    DateData.fromEpochDay(18460),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123456")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TIME_TYPES_WITH_ADAPTIVE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testTimeTypesWithTemporalModeMicroSeconds() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("time.precision.mode", "adaptive_time_microseconds");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.time_types")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2,
                    DateData.fromEpochDay(18460),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123456")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TIME_TYPES_WITH_ADAPTIVE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testTimeTypesWithTemporalModeConnect() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("time.precision.mode", "connect");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.time_types")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2,
                    DateData.fromEpochDay(18460),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123")),
                    TimeData.fromLocalTime(LocalTime.parse("18:00:22.123")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TIME_TYPES_WITH_ADAPTIVE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testHandlingDecimalModePrecise() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "precise");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_table")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("67.8912"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("987.65"), 5, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("12.3"), 3, 1),
                    DecimalData.fromBigDecimal(new BigDecimal("100.50"), 38, 2),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_PRECISE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testHandlingDecimalModeDouble() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "double");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_table")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    1, 123.45, 67.8912, 987.65, 12.3, 100.50,
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_DOUBLE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testHandlingDecimalModeString() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "string");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_table")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    BinaryStringData.fromString("123.45"),
                    BinaryStringData.fromString("67.8912"),
                    BinaryStringData.fromString("987.65"),
                    BinaryStringData.fromString("12.3"),
                    BinaryStringData.fromString("100.5"),
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_STRING))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testZeroHandlingDecimalModePrecise() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "precise");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_zero")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2,
                    DecimalData.fromBigDecimal(new BigDecimal("99999999.99"), 10, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("9999.9999"), 8, 4),
                    null,
                    null,
                    null,
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_PRECISE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testZeroHandlingDecimalModeDouble() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "double");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_zero")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2, 99999999.99, 9999.9999, null, null, null,
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_DOUBLE))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testZeroHandlingDecimalModeString() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "decimal_mode_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "string");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("test_decimal.decimal_test_zero")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    2,
                    BinaryStringData.fromString("99999999.99"),
                    BinaryStringData.fromString("9999.9999"),
                    null,
                    null,
                    null,
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TYPES_WITH_STRING))
                .isEqualTo(expectedSnapshot);
    }

    @Test
    public void testHstoreHandlingModeMap() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("hstore.handling.mode", "map");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.hstore_types")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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
        Map<BinaryStringData, BinaryStringData> expectedMap = new HashMap<>();
        expectedMap.put(BinaryStringData.fromString("a"), BinaryStringData.fromString("1"));
        expectedMap.put(BinaryStringData.fromString("b"), BinaryStringData.fromString("2"));

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Object[] snapshotObjects = recordFields(snapshotRecord, HSTORE_TYPES_WITH_ADAPTIVE);
        Map<String, String> snapshotMap =
                (Map<String, String>)
                        ((BinaryMapData) snapshotObjects[1])
                                .toJavaMap(DataTypes.STRING(), DataTypes.STRING());
        Assertions.assertThat(expectedMap).isEqualTo(snapshotMap);
    }

    @Test
    public void testJsonTypes() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("hstore.handling.mode", "map");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.json_types")
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debeziumProps)
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    BinaryStringData.fromString("{\"key1\":\"value1\"}"),
                    BinaryStringData.fromString("{\"key1\":\"value1\",\"key2\":\"value2\"}"),
                    BinaryStringData.fromString(
                            "[{\"key1\":\"value1\",\"key2\":{\"key2_1\":\"value2_1\",\"key2_2\":\"value2_2\"},\"key3\":[\"value3\"],\"key4\":[\"value4_1\",\"value4_2\"]},{\"key5\":\"value5\"}]"),
                    BinaryStringData.fromBytes("{\"key1\": \"value1\"}".getBytes()),
                    BinaryStringData.fromBytes(
                            "{\"key1\": \"value1\", \"key2\": \"value2\"}".getBytes()),
                    BinaryStringData.fromBytes(
                            "[{\"key1\": \"value1\", \"key2\": {\"key2_1\": \"value2_1\", \"key2_2\": \"value2_2\"}, \"key3\": [\"value3\"], \"key4\": [\"value4_1\", \"value4_2\"]}, {\"key5\": \"value5\"}]"
                                    .getBytes()),
                    1L
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, JSON_TYPES)).isEqualTo(expectedSnapshot);
    }

    @Test
    public void testArrayTypes() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.array_types")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Object[] actualSnapshotObjects = recordFields(snapshotRecord, ARRAY_TYPES);

        Assertions.assertThat(actualSnapshotObjects[0]).isEqualTo(1); // id column

        ArrayData actualTagsArray = (ArrayData) actualSnapshotObjects[1];
        Assertions.assertThat(actualTagsArray.getString(0))
                .isEqualTo(BinaryStringData.fromString("electronics"));
        Assertions.assertThat(actualTagsArray.getString(1))
                .isEqualTo(BinaryStringData.fromString("gadget"));
        Assertions.assertThat(actualTagsArray.getString(2))
                .isEqualTo(BinaryStringData.fromString("sale"));

        ArrayData actualScoresArray = (ArrayData) actualSnapshotObjects[2];
        Assertions.assertThat(actualScoresArray.getInt(0)).isEqualTo(85);
        Assertions.assertThat(actualScoresArray.getInt(1)).isEqualTo(90);
        Assertions.assertThat(actualScoresArray.getInt(2)).isEqualTo(78);

        ArrayData actualIntArray = (ArrayData) actualSnapshotObjects[3];
        Assertions.assertThat(actualIntArray.getInt(0)).isEqualTo(42);
    }

    @Test
    public void testArrayTypesUnsupportedMatrix() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");

        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.array_types_unsupported_matrix")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
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
        try {
            List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        } catch (Exception e) {
            Assertions.assertThat(getRootCause(e))
                    .hasMessage(
                            "Unable convert multidimensional array value '[null, null]' to a flat array.");
        }
    }

    public Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private <T> Tuple2<List<T>, List<CreateTableEvent>> fetchResultsAndCreateTableEvent(
            Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        List<CreateTableEvent> createTableEvents = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (event instanceof CreateTableEvent) {
                createTableEvents.add((CreateTableEvent) event);
            } else {
                result.add(event);
                size--;
            }
        }
        return Tuple2.of(result, createTableEvents);
    }

    private Object[] recordFields(RecordData record, RowType rowType) {
        int fieldNum = record.getArity();
        List<DataType> fieldTypes = rowType.getChildren();
        Object[] fields = new Object[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            if (record.isNullAt(i)) {
                fields[i] = null;
            } else {
                DataType type = fieldTypes.get(i);
                RecordData.FieldGetter fieldGetter = RecordData.createFieldGetter(type, i);
                Object o = fieldGetter.getFieldOrNull(record);
                fields[i] = o;
            }
        }
        return fields;
    }

    private Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC+8")).toInstant();
    }

    private static final RowType COMMON_TYPES =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.BYTES(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(10, 5),
                    DataTypes.DECIMAL(10, 1),
                    DataTypes.BOOLEAN(),
                    DataTypes.STRING(),
                    DataTypes.CHAR(1),
                    DataTypes.CHAR(3),
                    DataTypes.VARCHAR(20),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.DATE(),
                    DataTypes.TIME(0),
                    DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BINARY(8),
                    DataTypes.BINARY(20),
                    DataTypes.CHAR(3),
                    DataTypes.BIGINT(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING());

    private static final RowType TYPES_WITH_PRECISE =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.DECIMAL(10, 2),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(5, 2),
                    DataTypes.DECIMAL(3, 1),
                    DataTypes.DECIMAL(38, 2));

    private static final RowType TYPES_WITH_DOUBLE =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE());

    private static final RowType TYPES_WITH_STRING =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING());

    private static final RowType TIME_TYPES_WITH_ADAPTIVE =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.DATE(),
                    DataTypes.TIME(0),
                    DataTypes.TIME(3),
                    DataTypes.TIME(6),
                    DataTypes.TIMESTAMP(0),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_LTZ(0));

    private static final RowType HSTORE_TYPES_WITH_ADAPTIVE =
            RowType.of(DataTypes.INT(), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));

    private static final RowType JSON_TYPES =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.BIGINT());
    private static final RowType ARRAY_TYPES =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.ARRAY(DataTypes.STRING()),
                    DataTypes.ARRAY(DataTypes.INT()),
                    DataTypes.ARRAY(DataTypes.INT()));

    private static final RowType ARRAY_TYPES_MATRIX =
            RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())));
}
