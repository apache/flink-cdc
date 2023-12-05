/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.MySqlVersion;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.getServerId;
import static com.ververica.cdc.connectors.mysql.testutils.RecordDataTestUtils.recordFields;
import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for MySQL event source. */
public class MySqlFullTypesITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase fullTypesMySql8Database =
            new UniqueDatabase(
                    MYSQL8_CONTAINER, "column_type_test_mysql8", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @Before
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testMysql57CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql57Database);
    }

    @Test
    public void testMySql8CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql8Database);
    }

    @Test
    public void testMysql57TimeDataTypes() throws Throwable {
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
                        DataTypes.TIMESTAMP_LTZ(0));

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    // TIME(6) will lose precision for microseconds.
                    // Because Flink's BinaryWriter force write int value for TIME(6).
                    // See BinaryWriter#write for detail.
                    64822123,
                    TimestampData.fromMillis(1595008822000L),
                    TimestampData.fromMillis(1595008822123L),
                    TimestampData.fromMillis(1595008822123L, 456000),
                    LocalZonedTimestampData.fromEpochMillis(1595008822000L, 0)
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    null,
                    TimestampData.fromMillis(1595008822000L),
                    TimestampData.fromMillis(1595008822123L),
                    TimestampData.fromMillis(1595008822123L, 456000),
                    LocalZonedTimestampData.fromEpochMillis(1595008822000L, 0)
                };

        testTimeDataTypes(
                fullTypesMySql57Database, recordType, expectedSnapshot, expectedStreamRecord);
    }

    @Test
    public void testMysql8TimeDataTypes() throws Throwable {
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
                        DataTypes.TIMESTAMP_LTZ(6));

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    // TIME(6) will lose precision for microseconds.
                    // Because Flink's BinaryWriter force write int value for TIME(6).
                    // See BinaryWriter#write for detail.
                    64822123,
                    TimestampData.fromMillis(1595008822000L),
                    TimestampData.fromMillis(1595008822123L),
                    TimestampData.fromMillis(1595008822123L, 456000),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2020-07-17T18:00:22Z")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2020-07-17T18:00:22.123Z")),
                    LocalZonedTimestampData.fromInstant(
                            Instant.parse("2020-07-17T18:00:22.123456Z"))
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    null,
                    TimestampData.fromMillis(1595008822000L),
                    TimestampData.fromMillis(1595008822123L),
                    TimestampData.fromMillis(1595008822123L, 456000),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2020-07-17T18:00:22Z")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2020-07-17T18:00:22.123Z")),
                    LocalZonedTimestampData.fromInstant(
                            Instant.parse("2020-07-17T18:00:22.123456Z"))
                };

        testTimeDataTypes(
                fullTypesMySql8Database, recordType, expectedSnapshot, expectedStreamRecord);
    }

    private void testCommonDataTypes(UniqueDatabase database) throws Exception {
        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"common_types"}, database)
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
                    BinaryStringData.fromString(expectGeometryCollectionJsonText)
                };

        // skip CreateTableEvent
        List<Event> snapshotResults = fetchResults(iterator, 2);
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(1)).after();
        assertThat(recordFields(snapshotRecord, COMMON_TYPES)).isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE common_types SET big_decimal_c = null WHERE id = 1;");
        }

        expectedSnapshot[30] = null;
        // The json string from binlog will remove useless space
        expectedSnapshot[44] = BinaryStringData.fromString("{\"key1\":\"value1\"}");
        Object[] expectedStreamRecord = expectedSnapshot;

        List<Event> streamResults = fetchResults(iterator, 1);
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        assertThat(recordFields(streamRecord, COMMON_TYPES)).isEqualTo(expectedStreamRecord);
    }

    private void testTimeDataTypes(
            UniqueDatabase database,
            RowType recordType,
            Object[] expectedSnapshot,
            Object[] expectedStreamRecord)
            throws Exception {
        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"time_types"}, database)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        // skip CreateTableEvent
        List<Event> snapshotResults = fetchResults(iterator, 2);
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(1)).after();

        assertThat(recordFields(snapshotRecord, recordType)).isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE time_types SET time_6_c = null WHERE id = 1;");
        }

        List<Event> streamResults = fetchResults(iterator, 1);
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        assertThat(recordFields(streamRecord, recordType)).isEqualTo(expectedStreamRecord);
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            String[] captureTables, UniqueDatabase database) {
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
                        .serverId(getServerId(env.getParallelism()));
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
                    DataTypes.STRING());
}
