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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/** Integration tests for MySQL Table source to handle ancient date and time records. */
public class MySqlAncientDateAndTimeITCase extends MySqlSourceTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlAncientDateAndTimeITCase.class);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    // We need an extra "no_zero_in_date = false" config to insert malformed date and time records.
    private static final MySqlContainer MYSQL_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-allow-ancient-date-time/my.cnf");

    private final UniqueDatabase ancientDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "ancient_date_and_time", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting MySql container...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Container MySql is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping MySql containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Container MySql is stopped.");
    }

    @BeforeEach
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
        ancientDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        ancientDatabase.dropDatabase();
    }

    /**
     * With the TimeAdjuster in Debezium, all date / time records between year 0001 and 0099 will be
     * shifted to 1971 ~ 2069.
     */
    @Test
    public void testAncientDateAndTimeWithTimeAdjuster() throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | 17390     | 11323    | 11720    | 23072    | -557266  | -1         | 18261      |
        // | 2017/8/12 | 2001/1/1 | 2002/2/2 | 2033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                ancientDatabase,
                true,
                Arrays.asList(
                        "[1, 2017-08-12, 2016-07-13T17:17:17, 2015-06-14T17:17:17.100, 2014-05-15T17:17:17.120, 2013-04-16T17:17:17.123, 2012-03-17T17:17:17.123400, 2011-02-18T17:17:17.123450, 2010-01-19T17:17:17.123456]",
                        "[2, null, null, null, null, null, null, null, null]",
                        "[3, 2001-01-01, 2001-01-01T16:16:16, 2001-01-01T16:16:16.100, 2001-01-01T16:16:16.120, 2001-01-01T16:16:16.123, 2001-01-01T16:16:16.123400, 2001-01-01T16:16:16.123450, 2001-01-01T16:16:16.123456]",
                        "[4, 2002-02-02, 2002-02-02T15:15:15, 2002-02-02T15:15:15.100, 2002-02-02T15:15:15.120, 2002-02-02T15:15:15.123, 2002-02-02T15:15:15.123400, 2002-02-02T15:15:15.123450, 2002-02-02T15:15:15.123456]",
                        "[5, 2033-03-03, 2033-03-03T14:14:14, 2033-03-03T14:14:14.100, 2033-03-03T14:14:14.120, 2033-03-03T14:14:14.123, 2033-03-03T14:14:14.123400, 2033-03-03T14:14:14.123450, 2033-03-03T14:14:14.123456]",
                        "[6, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "[7, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "[8, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"),
                Arrays.asList(
                        "[9, 2017-08-12, 2016-07-13T17:17:17, 2015-06-14T17:17:17.100, 2014-05-15T17:17:17.120, 2013-04-16T17:17:17.123, 2012-03-17T17:17:17.123400, 2011-02-18T17:17:17.123450, 2010-01-19T17:17:17.123456]",
                        "[10, null, null, null, null, null, null, null, null]",
                        "[11, 2001-01-01, 2001-01-01T16:16:16, 2001-01-01T16:16:16.100, 2001-01-01T16:16:16.120, 2001-01-01T16:16:16.123, 2001-01-01T16:16:16.123400, 2001-01-01T16:16:16.123450, 2001-01-01T16:16:16.123456]",
                        "[12, 2002-02-02, 2002-02-02T15:15:15, 2002-02-02T15:15:15.100, 2002-02-02T15:15:15.120, 2002-02-02T15:15:15.123, 2002-02-02T15:15:15.123400, 2002-02-02T15:15:15.123450, 2002-02-02T15:15:15.123456]",
                        "[13, 2033-03-03, 2033-03-03T14:14:14, 2033-03-03T14:14:14.100, 2033-03-03T14:14:14.120, 2033-03-03T14:14:14.123, 2033-03-03T14:14:14.123400, 2033-03-03T14:14:14.123450, 2033-03-03T14:14:14.123456]",
                        "[14, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "[15, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "[16, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    @Test
    public void testAncientDateAndTimeWithoutTimeAdjuster() throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | -713095   | -719162  | -718765  | -707413  | -557266  | -1         | 18261      |
        // | 0017/8/12 | 0001/1/1 | 0002/2/2 | 0033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                ancientDatabase,
                false,
                Arrays.asList(
                        "[1, 0017-08-12, 0016-07-13T17:17:17, 0015-06-14T17:17:17.100, 0014-05-15T17:17:17.120, 0013-04-16T17:17:17.123, 0012-03-17T17:17:17.123400, 0011-02-18T17:17:17.123450, 0010-01-19T17:17:17.123456]",
                        "[2, null, null, null, null, null, null, null, null]",
                        "[3, 0001-01-01, 0001-01-01T16:16:16, 0001-01-01T16:16:16.100, 0001-01-01T16:16:16.120, 0001-01-01T16:16:16.123, 0001-01-01T16:16:16.123400, 0001-01-01T16:16:16.123450, 0001-01-01T16:16:16.123456]",
                        "[4, 0002-02-02, 0002-02-02T15:15:15, 0002-02-02T15:15:15.100, 0002-02-02T15:15:15.120, 0002-02-02T15:15:15.123, 0002-02-02T15:15:15.123400, 0002-02-02T15:15:15.123450, 0002-02-02T15:15:15.123456]",
                        "[5, 0033-03-03, 0033-03-03T14:14:14, 0033-03-03T14:14:14.100, 0033-03-03T14:14:14.120, 0033-03-03T14:14:14.123, 0033-03-03T14:14:14.123400, 0033-03-03T14:14:14.123450, 0033-03-03T14:14:14.123456]",
                        "[6, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "[7, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "[8, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"),
                Arrays.asList(
                        "[9, 0017-08-12, 0016-07-13T17:17:17, 0015-06-14T17:17:17.100, 0014-05-15T17:17:17.120, 0013-04-16T17:17:17.123, 0012-03-17T17:17:17.123400, 0011-02-18T17:17:17.123450, 0010-01-19T17:17:17.123456]",
                        "[10, null, null, null, null, null, null, null, null]",
                        "[11, 0001-01-01, 0001-01-01T16:16:16, 0001-01-01T16:16:16.100, 0001-01-01T16:16:16.120, 0001-01-01T16:16:16.123, 0001-01-01T16:16:16.123400, 0001-01-01T16:16:16.123450, 0001-01-01T16:16:16.123456]",
                        "[12, 0002-02-02, 0002-02-02T15:15:15, 0002-02-02T15:15:15.100, 0002-02-02T15:15:15.120, 0002-02-02T15:15:15.123, 0002-02-02T15:15:15.123400, 0002-02-02T15:15:15.123450, 0002-02-02T15:15:15.123456]",
                        "[13, 0033-03-03, 0033-03-03T14:14:14, 0033-03-03T14:14:14.100, 0033-03-03T14:14:14.120, 0033-03-03T14:14:14.123, 0033-03-03T14:14:14.123400, 0033-03-03T14:14:14.123450, 0033-03-03T14:14:14.123456]",
                        "[14, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "[15, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "[16, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    private void runGenericAncientDateAndTimeTest(
            UniqueDatabase database,
            boolean enableTimeAdjuster,
            List<String> expectedSnapshotResults,
            List<String> expectedStreamingResults)
            throws Exception {
        Schema ancientSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("date_col", DataTypes.DATE(), null, "0017-08-12")
                        .physicalColumn(
                                "datetime_0_col",
                                DataTypes.TIMESTAMP(0),
                                null,
                                "0016-07-13 17:17:17")
                        .physicalColumn(
                                "datetime_1_col",
                                DataTypes.TIMESTAMP(1),
                                null,
                                "0015-06-14 17:17:17.1")
                        .physicalColumn(
                                "datetime_2_col",
                                DataTypes.TIMESTAMP(2),
                                null,
                                "0014-05-15 17:17:17.12")
                        .physicalColumn(
                                "datetime_3_col",
                                DataTypes.TIMESTAMP(3),
                                null,
                                "0013-04-16 17:17:17.123")
                        .physicalColumn(
                                "datetime_4_col",
                                DataTypes.TIMESTAMP(4),
                                null,
                                "0012-03-17 17:17:17.1234")
                        .physicalColumn(
                                "datetime_5_col",
                                DataTypes.TIMESTAMP(5),
                                null,
                                "0011-02-18 17:17:17.12345")
                        .physicalColumn(
                                "datetime_6_col",
                                DataTypes.TIMESTAMP(6),
                                null,
                                "0010-01-19 17:17:17.123456")
                        .primaryKey("id")
                        .build();
        List<RecordData.FieldGetter> ancientSchemaFieldGetters =
                SchemaUtils.createFieldGetters(ancientSchema);
        CreateTableEvent ancientCreateTableEvent =
                new CreateTableEvent(
                        TableId.tableId(ancientDatabase.getDatabaseName(), "ancient_times"),
                        ancientSchema);
        try (CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(
                                                new String[] {"ancient_times"},
                                                database,
                                                enableTimeAdjuster)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect()) {

            {
                Tuple2<List<Event>, List<CreateTableEvent>> snapshotResults =
                        fetchResultsAndCreateTableEvent(iterator, expectedSnapshotResults.size());
                Assertions.assertThat(snapshotResults.f1).isSubsetOf(ancientCreateTableEvent);
                Assertions.assertThat(snapshotResults.f0)
                        .map(evt -> (DataChangeEvent) evt)
                        .map(
                                evt ->
                                        SchemaUtils.restoreOriginalData(
                                                        evt.after(), ancientSchemaFieldGetters)
                                                .toString())
                        .containsExactlyInAnyOrderElementsOf(expectedSnapshotResults);
            }

            createBinlogEvents(ancientDatabase);

            {
                Tuple2<List<Event>, List<CreateTableEvent>> streamingResults =
                        fetchResultsAndCreateTableEvent(iterator, expectedSnapshotResults.size());
                Assertions.assertThat(streamingResults.f1).isSubsetOf(ancientCreateTableEvent);
                Assertions.assertThat(streamingResults.f0)
                        .map(evt -> (DataChangeEvent) evt)
                        .map(
                                evt ->
                                        SchemaUtils.restoreOriginalData(
                                                        evt.after(), ancientSchemaFieldGetters)
                                                .toString())
                        .containsExactlyInAnyOrderElementsOf(expectedStreamingResults);
            }
        }
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            String[] captureTables, UniqueDatabase database, boolean enableTimeAdjuster) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        Properties dbzProperties = new Properties();
        dbzProperties.put("enable.time.adjuster", String.valueOf(enableTimeAdjuster));

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
                        .debeziumProperties(dbzProperties);
        return (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
    }

    private static void createBinlogEvents(UniqueDatabase database) throws SQLException {
        // Test reading identical data in binlog stage again
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT,\n"
                            + "    DEFAULT\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0000-00-00',\n"
                            + "    '0000-00-00 00:00:00',\n"
                            + "    '0000-00-00 00:00:00.0',\n"
                            + "    '0000-00-00 00:00:00.00',\n"
                            + "    '0000-00-00 00:00:00.000',\n"
                            + "    '0000-00-00 00:00:00.0000',\n"
                            + "    '0000-00-00 00:00:00.00000',\n"
                            + "    '0000-00-00 00:00:00.000000'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0001-01-01',\n"
                            + "    '0001-01-01 16:16:16',\n"
                            + "    '0001-01-01 16:16:16.1',\n"
                            + "    '0001-01-01 16:16:16.12',\n"
                            + "    '0001-01-01 16:16:16.123',\n"
                            + "    '0001-01-01 16:16:16.1234',\n"
                            + "    '0001-01-01 16:16:16.12345',\n"
                            + "    '0001-01-01 16:16:16.123456'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0002-02-02',\n"
                            + "    '0002-02-02 15:15:15',\n"
                            + "    '0002-02-02 15:15:15.1',\n"
                            + "    '0002-02-02 15:15:15.12',\n"
                            + "    '0002-02-02 15:15:15.123',\n"
                            + "    '0002-02-02 15:15:15.1234',\n"
                            + "    '0002-02-02 15:15:15.12345',\n"
                            + "    '0002-02-02 15:15:15.123456'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0033-03-03',\n"
                            + "    '0033-03-03 14:14:14',\n"
                            + "    '0033-03-03 14:14:14.1',\n"
                            + "    '0033-03-03 14:14:14.12',\n"
                            + "    '0033-03-03 14:14:14.123',\n"
                            + "    '0033-03-03 14:14:14.1234',\n"
                            + "    '0033-03-03 14:14:14.12345',\n"
                            + "    '0033-03-03 14:14:14.123456'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0444-04-04',\n"
                            + "    '0444-04-04 13:13:13',\n"
                            + "    '0444-04-04 13:13:13.1',\n"
                            + "    '0444-04-04 13:13:13.12',\n"
                            + "    '0444-04-04 13:13:13.123',\n"
                            + "    '0444-04-04 13:13:13.1234',\n"
                            + "    '0444-04-04 13:13:13.12345',\n"
                            + "    '0444-04-04 13:13:13.123456'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '1969-12-31',\n"
                            + "    '1969-12-31 12:12:12',\n"
                            + "    '1969-12-31 12:12:12.1',\n"
                            + "    '1969-12-31 12:12:12.12',\n"
                            + "    '1969-12-31 12:12:12.123',\n"
                            + "    '1969-12-31 12:12:12.1234',\n"
                            + "    '1969-12-31 12:12:12.12345',\n"
                            + "    '1969-12-31 12:12:12.123456'\n"
                            + ");");
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '2019-12-31',\n"
                            + "    '2019-12-31 23:11:11',\n"
                            + "    '2019-12-31 23:11:11.1',\n"
                            + "    '2019-12-31 23:11:11.12',\n"
                            + "    '2019-12-31 23:11:11.123',\n"
                            + "    '2019-12-31 23:11:11.1234',\n"
                            + "    '2019-12-31 23:11:11.12345',\n"
                            + "    '2019-12-31 23:11:11.123456'\n"
                            + ");");
        }
    }

    public static <T> Tuple2<List<T>, List<CreateTableEvent>> fetchResultsAndCreateTableEvent(
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
}
