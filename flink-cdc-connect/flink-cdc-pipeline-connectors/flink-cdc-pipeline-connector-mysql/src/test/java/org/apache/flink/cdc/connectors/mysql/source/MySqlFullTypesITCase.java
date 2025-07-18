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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.RecordDataTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;

/** IT case for MySQL event source. */
class MySqlFullTypesITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    "column_type_test",
                    MySqSourceTestUtils.TEST_USER,
                    MySqSourceTestUtils.TEST_PASSWORD);
    private final UniqueDatabase fullTypesMySql8Database =
            new UniqueDatabase(
                    MYSQL8_CONTAINER,
                    "column_type_test_mysql8",
                    MySqSourceTestUtils.TEST_USER,
                    MySqSourceTestUtils.TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @BeforeEach
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    void testMysql57CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql57Database);
    }

    @Test
    public void testMysql57JsonDataTypes() throws Throwable {
        // Set `useLegacyJsonFormat` as false, so the json string will have no whitespace
        // before value and after comma in json format be formatted with legacy format.
        testJsonDataType(fullTypesMySql57Database, false);
    }

    @Test
    public void testMysql57JsonDataTypesWithUseLegacyJsonFormat() throws Throwable {
        // Set `useLegacyJsonFormat` as true, so the json string will have whitespace before
        // value and after comma in json format be formatted with legacy format.
        testJsonDataType(fullTypesMySql57Database, true);
    }

    @Test
    void testMySql8CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql8Database);
    }

    @Test
    public void testMySql8JsonDataTypes() throws Throwable {
        // Set `useLegacyJsonFormat` as false, so the json string will have no whitespace
        // before value and after comma in json format be formatted with legacy format.
        testJsonDataType(fullTypesMySql8Database, false);
    }

    @Test
    public void testMySql8JsonDataTypesWithUseLegacyJsonFormat() throws Throwable {
        // Set `useLegacyJsonFormat` as true, so the json string will have whitespace before
        // value and after comma in json format be formatted with legacy format.
        testJsonDataType(fullTypesMySql8Database, true);
    }

    @Test
    void testMysql57TimeDataTypes() throws Throwable {
        RowType recordType =
                RowType.of(
                        DataTypes.DECIMAL(20, 0).notNull(),
                        DataTypes.INT(),
                        DataTypes.DATE(),
                        DataTypes.TIME(0),
                        DataTypes.TIME(3),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(0));

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    DateData.fromEpochDay(18460),
                    TimeData.fromNanoOfDay(64822000_000_000L),
                    TimeData.fromNanoOfDay(64822123_000_000L),
                    TimeData.fromNanoOfDay(64822123_456_000L),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    null
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    DateData.fromEpochDay(18460),
                    TimeData.fromNanoOfDay(64822000_000_000L),
                    TimeData.fromNanoOfDay(64822123_000_000L),
                    null,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2000-01-01 00:00:00"))
                };

        testTimeDataTypes(
                fullTypesMySql57Database, recordType, expectedSnapshot, expectedStreamRecord);
    }

    @Test
    void testMysql8TimeDataTypes() throws Throwable {
        UniqueDatabase usedDd = fullTypesMySql8Database;
        RowType recordType =
                RowType.of(
                        DataTypes.DECIMAL(20, 0).notNull(),
                        DataTypes.INT(),
                        DataTypes.DATE(),
                        DataTypes.TIME(0),
                        DataTypes.TIME(3),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.TIMESTAMP_LTZ(0));

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    DateData.fromEpochDay(18460),
                    TimeData.fromNanoOfDay(64822000_000_000L),
                    TimeData.fromNanoOfDay(64822123_000_000L),
                    TimeData.fromNanoOfDay(64822123_456_000L),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22.123")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22.123456")),
                    null
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    DateData.fromEpochDay(18460),
                    TimeData.fromNanoOfDay(64822000_000_000L),
                    TimeData.fromNanoOfDay(64822123_000_000L),
                    null,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22.123")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2000-01-01 00:00:00"))
                };

        testTimeDataTypes(usedDd, recordType, expectedSnapshot, expectedStreamRecord);
    }

    @Test
    void testMysql57PrecisionTypes() throws Throwable {
        testMysqlPrecisionTypes(fullTypesMySql57Database);
    }

    @Test
    void testMysql8PrecisionTypes() throws Throwable {
        testMysqlPrecisionTypes(fullTypesMySql8Database);
    }

    void testMysqlPrecisionTypes(UniqueDatabase database) throws Throwable {
        RowType recordType =
                RowType.of(
                        DataTypes.DECIMAL(20, 0).notNull(),
                        DataTypes.DECIMAL(6, 2),
                        DataTypes.DECIMAL(9, 4),
                        DataTypes.DECIMAL(20, 4),
                        DataTypes.TIME(0),
                        DataTypes.TIME(3),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE());

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.5"), 9, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 20, 4),
                    TimeData.fromNanoOfDay(64800000_000_000L),
                    TimeData.fromNanoOfDay(64822100_000_000L),
                    TimeData.fromNanoOfDay(64822100_000_000L),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:00")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:00")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    2d,
                    3d,
                    5d,
                    7d,
                    11d,
                    13d,
                    17d,
                    19d,
                    23d,
                    29d,
                    31d,
                    37d
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.5"), 9, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 20, 4),
                    TimeData.fromNanoOfDay(64800000_000_000L),
                    TimeData.fromNanoOfDay(64822100_000_000L),
                    null,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:00")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:00")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    2d,
                    3d,
                    5d,
                    7d,
                    11d,
                    13d,
                    17d,
                    19d,
                    23d,
                    29d,
                    31d,
                    37d
                };

        database.createAndInitialize();
        Boolean useLegacyJsonFormat = true;
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(
                                                new String[] {"precision_types"},
                                                database,
                                                useLegacyJsonFormat)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        // skip CreateTableEvent
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, recordType))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE precision_types SET time_6_c = null WHERE id = 1;");
        }

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, recordType))
                .isEqualTo(expectedStreamRecord);
    }

    private void testCommonDataTypes(UniqueDatabase database) throws Exception {
        database.createAndInitialize();
        // Set useLegacyJsonFormat option as true, so the json string will have no whitespace before
        // value and after comma in json format.be formatted with legacy format.
        Boolean useLegacyJsonFormat = true;
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(
                                                new String[] {"common_types"},
                                                database,
                                                useLegacyJsonFormat)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        String expectedPointJsonText = "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}";
        String expectedGeometryJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectLinestringJsonText =
                "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}";
        String expectPolygonJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectMultipointJsonText =
                "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}";
        String expectMultilineJsonText =
                "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}";
        String expectMultipolygonJsonText =
                "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}";
        String expectGeometryCollectionJsonText =
                "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}";

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    (byte) 127,
                    (short) 255,
                    (short) 255,
                    (short) 32767,
                    65535,
                    65535,
                    8388607,
                    16777215,
                    16777215,
                    2147483647,
                    4294967295L,
                    4294967295L,
                    2147483647L,
                    9223372036854775807L,
                    DecimalData.fromBigDecimal(new BigDecimal("18446744073709551615"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("18446744073709551615"), 20, 0),
                    BinaryStringData.fromString("Hello World"),
                    BinaryStringData.fromString("abc"),
                    123.102d,
                    123.102f,
                    123.103f,
                    123.104f,
                    404.4443d,
                    404.4444d,
                    404.4445d,
                    DecimalData.fromBigDecimal(new BigDecimal("123.4567"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4568"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4569"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("346"), 6, 0),
                    // Decimal precision larger than 38 will be treated as string.
                    BinaryStringData.fromString("34567892.1"),
                    false,
                    new byte[] {3},
                    true,
                    true,
                    parseHexBinary("651aed08-390f-4893-b2f1-36923e7b7400".replace("-", "")),
                    new byte[] {4, 4, 4, 4, 4, 4, 4, 4},
                    BinaryStringData.fromString("text"),
                    new byte[] {16},
                    new byte[] {16},
                    new byte[] {16},
                    new byte[] {16},
                    2021,
                    BinaryStringData.fromString("red"),
                    BinaryStringData.fromString("{\"key1\": \"value1\"}"),
                    BinaryStringData.fromString(expectedPointJsonText),
                    BinaryStringData.fromString(expectedGeometryJsonText),
                    BinaryStringData.fromString(expectLinestringJsonText),
                    BinaryStringData.fromString(expectPolygonJsonText),
                    BinaryStringData.fromString(expectMultipointJsonText),
                    BinaryStringData.fromString(expectMultilineJsonText),
                    BinaryStringData.fromString(expectMultipolygonJsonText),
                    BinaryStringData.fromString(expectGeometryCollectionJsonText),
                    BinaryStringData.fromString("long"),
                    BinaryStringData.fromString("long varchar"),
                    BinaryStringData.fromString("")
                };

        // skip CreateTableEvent
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, COMMON_TYPES))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE common_types SET big_decimal_c = null WHERE id = 1;");
        }

        expectedSnapshot[30] = null;
        // Legacy format removes useless space in json string from binlog
        expectedSnapshot[44] = BinaryStringData.fromString("{\"key1\":\"value1\"}");
        Object[] expectedStreamRecord = expectedSnapshot;

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, COMMON_TYPES))
                .isEqualTo(expectedStreamRecord);
    }

    private void testJsonDataType(UniqueDatabase database, Boolean useLegacyJsonFormat)
            throws Exception {
        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(
                                                new String[] {"json_types"},
                                                database,
                                                useLegacyJsonFormat)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    BinaryStringData.fromString("{\"key1\": \"value1\"}"),
                    BinaryStringData.fromString("{\"key1\": \"value1\", \"key2\": \"value2\"}"),
                    BinaryStringData.fromString(
                            "[{\"key1\": \"value1\", \"key2\": {\"key2_1\": \"value2_1\", \"key2_2\": \"value2_2\"}, \"key3\": [\"value3\"], \"key4\": [\"value4_1\", \"value4_2\"]}, {\"key5\": \"value5\"}]"),
                    1
                };

        // skip CreateTableEvent
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, JSON_TYPES))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE json_types SET int_c = null WHERE id = 1;");
        }

        Object[] expectedStreamRecord = expectedSnapshot;
        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();

        expectedSnapshot[4] = null;

        if (useLegacyJsonFormat) {
            // removed whitespace before value and after comma in json format string value
            Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, JSON_TYPES))
                    .containsExactly(
                            DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                            BinaryStringData.fromString("{\"key1\":\"value1\"}"),
                            BinaryStringData.fromString(
                                    "{\"key1\":\"value1\",\"key2\":\"value2\"}"),
                            BinaryStringData.fromString(
                                    "[{\"key1\":\"value1\",\"key2\":{\"key2_1\":\"value2_1\",\"key2_2\":\"value2_2\"},\"key3\":[\"value3\"],\"key4\":[\"value4_1\",\"value4_2\"]},{\"key5\":\"value5\"}]"),
                            null);
        } else {
            Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, JSON_TYPES))
                    .containsExactly(expectedStreamRecord);
        }
    }

    private Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
    }

    private void testTimeDataTypes(
            UniqueDatabase database,
            RowType recordType,
            Object[] expectedSnapshot,
            Object[] expectedStreamRecord)
            throws Exception {
        database.createAndInitialize();
        Boolean useLegacyJsonFormat = true;
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(
                                                new String[] {"time_types"},
                                                database,
                                                useLegacyJsonFormat)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        // skip CreateTableEvents
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, recordType))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE time_types SET time_6_c = null, timestamp_def_c = default WHERE id = 1;");
        }

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, recordType))
                .isEqualTo(expectedStreamRecord);
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            String[] captureTables, UniqueDatabase database, Boolean useLegacyJsonFormat) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(database.getDatabaseName())
                        .tableList(captureTableIds)
                        .includeSchemaChanges(false)
                        .hostname(database.getHost())
                        .port(database.getDatabasePort())
                        .splitSize(10)
                        .fetchSize(2)
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .serverId(MySqSourceTestUtils.getServerId(env.getParallelism()))
                        .useLegacyJsonFormat(useLegacyJsonFormat);
        return (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
    }

    private static final RowType COMMON_TYPES =
            RowType.of(
                    DataTypes.DECIMAL(20, 0).notNull(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.DECIMAL(20, 0),
                    DataTypes.DECIMAL(20, 0),
                    DataTypes.VARCHAR(255),
                    DataTypes.CHAR(3),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(6, 0),
                    // Decimal precision larger than 38 will be treated as string.
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BINARY(1),
                    DataTypes.BOOLEAN(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BINARY(16),
                    DataTypes.BINARY(8),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.INT(),
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

    private static final RowType JSON_TYPES =
            RowType.of(
                    DataTypes.DECIMAL(20, 0).notNull(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.INT());
}
