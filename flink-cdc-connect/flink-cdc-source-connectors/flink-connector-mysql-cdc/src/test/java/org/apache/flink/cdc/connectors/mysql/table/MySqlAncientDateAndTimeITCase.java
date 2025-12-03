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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;

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
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

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

    void setup(boolean incrementalSnapshot) {
        TestValuesTableFactory.clearAllData();
        if (incrementalSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
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
    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    public void testAncientDateAndTimeWithTimeAdjuster(boolean incrementalSnapshot)
            throws Exception {
        setup(incrementalSnapshot);
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                incrementalSnapshot,
                true,
                Arrays.asList(
                        "+I[1, 2017-08-12, 2016-07-13T17:17:17, 2015-06-14T17:17:17.100, 2014-05-15T17:17:17.120, 2013-04-16T17:17:17.123, 2012-03-17T17:17:17.123400, 2011-02-18T17:17:17.123450, 2010-01-19T17:17:17.123456]",
                        "+I[2, null, null, null, null, null, null, null, null]",
                        "+I[3, 2001-01-01, 2001-01-01T16:16:16, 2001-01-01T16:16:16.100, 2001-01-01T16:16:16.120, 2001-01-01T16:16:16.123, 2001-01-01T16:16:16.123400, 2001-01-01T16:16:16.123450, 2001-01-01T16:16:16.123456]",
                        "+I[4, 2002-02-02, 2002-02-02T15:15:15, 2002-02-02T15:15:15.100, 2002-02-02T15:15:15.120, 2002-02-02T15:15:15.123, 2002-02-02T15:15:15.123400, 2002-02-02T15:15:15.123450, 2002-02-02T15:15:15.123456]",
                        "+I[5, 2033-03-03, 2033-03-03T14:14:14, 2033-03-03T14:14:14.100, 2033-03-03T14:14:14.120, 2033-03-03T14:14:14.123, 2033-03-03T14:14:14.123400, 2033-03-03T14:14:14.123450, 2033-03-03T14:14:14.123456]",
                        "+I[6, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[7, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[8, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"),
                Arrays.asList(
                        "+I[9, 2017-08-12, 2016-07-13T17:17:17, 2015-06-14T17:17:17.100, 2014-05-15T17:17:17.120, 2013-04-16T17:17:17.123, 2012-03-17T17:17:17.123400, 2011-02-18T17:17:17.123450, 2010-01-19T17:17:17.123456]",
                        "+I[10, null, null, null, null, null, null, null, null]",
                        "+I[11, 2001-01-01, 2001-01-01T16:16:16, 2001-01-01T16:16:16.100, 2001-01-01T16:16:16.120, 2001-01-01T16:16:16.123, 2001-01-01T16:16:16.123400, 2001-01-01T16:16:16.123450, 2001-01-01T16:16:16.123456]",
                        "+I[12, 2002-02-02, 2002-02-02T15:15:15, 2002-02-02T15:15:15.100, 2002-02-02T15:15:15.120, 2002-02-02T15:15:15.123, 2002-02-02T15:15:15.123400, 2002-02-02T15:15:15.123450, 2002-02-02T15:15:15.123456]",
                        "+I[13, 2033-03-03, 2033-03-03T14:14:14, 2033-03-03T14:14:14.100, 2033-03-03T14:14:14.120, 2033-03-03T14:14:14.123, 2033-03-03T14:14:14.123400, 2033-03-03T14:14:14.123450, 2033-03-03T14:14:14.123456]",
                        "+I[14, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[15, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[16, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    public void testAncientDateAndTimeWithoutTimeAdjuster(boolean incrementalSnapshot)
            throws Exception {
        setup(incrementalSnapshot);
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                incrementalSnapshot,
                false,
                Arrays.asList(
                        "+I[1, 0017-08-12, 0016-07-13T17:17:17, 0015-06-14T17:17:17.100, 0014-05-15T17:17:17.120, 0013-04-16T17:17:17.123, 0012-03-17T17:17:17.123400, 0011-02-18T17:17:17.123450, 0010-01-19T17:17:17.123456]",
                        "+I[2, null, null, null, null, null, null, null, null]",
                        "+I[3, 0001-01-01, 0001-01-01T16:16:16, 0001-01-01T16:16:16.100, 0001-01-01T16:16:16.120, 0001-01-01T16:16:16.123, 0001-01-01T16:16:16.123400, 0001-01-01T16:16:16.123450, 0001-01-01T16:16:16.123456]",
                        "+I[4, 0002-02-02, 0002-02-02T15:15:15, 0002-02-02T15:15:15.100, 0002-02-02T15:15:15.120, 0002-02-02T15:15:15.123, 0002-02-02T15:15:15.123400, 0002-02-02T15:15:15.123450, 0002-02-02T15:15:15.123456]",
                        "+I[5, 0033-03-03, 0033-03-03T14:14:14, 0033-03-03T14:14:14.100, 0033-03-03T14:14:14.120, 0033-03-03T14:14:14.123, 0033-03-03T14:14:14.123400, 0033-03-03T14:14:14.123450, 0033-03-03T14:14:14.123456]",
                        "+I[6, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[7, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[8, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"),
                Arrays.asList(
                        "+I[9, 0017-08-12, 0016-07-13T17:17:17, 0015-06-14T17:17:17.100, 0014-05-15T17:17:17.120, 0013-04-16T17:17:17.123, 0012-03-17T17:17:17.123400, 0011-02-18T17:17:17.123450, 0010-01-19T17:17:17.123456]",
                        "+I[10, null, null, null, null, null, null, null, null]",
                        "+I[11, 0001-01-01, 0001-01-01T16:16:16, 0001-01-01T16:16:16.100, 0001-01-01T16:16:16.120, 0001-01-01T16:16:16.123, 0001-01-01T16:16:16.123400, 0001-01-01T16:16:16.123450, 0001-01-01T16:16:16.123456]",
                        "+I[12, 0002-02-02, 0002-02-02T15:15:15, 0002-02-02T15:15:15.100, 0002-02-02T15:15:15.120, 0002-02-02T15:15:15.123, 0002-02-02T15:15:15.123400, 0002-02-02T15:15:15.123450, 0002-02-02T15:15:15.123456]",
                        "+I[13, 0033-03-03, 0033-03-03T14:14:14, 0033-03-03T14:14:14.100, 0033-03-03T14:14:14.120, 0033-03-03T14:14:14.123, 0033-03-03T14:14:14.123400, 0033-03-03T14:14:14.123450, 0033-03-03T14:14:14.123456]",
                        "+I[14, 0444-04-04, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[15, 1969-12-31, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[16, 2019-12-31, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    private void runGenericAncientDateAndTimeTest(
            MySqlContainer container,
            UniqueDatabase database,
            boolean incrementalSnapshot,
            boolean enableTimeAdjuster,
            List<String> expectedSnapshotResults,
            List<String> expectedStreamingResults)
            throws Exception {
        String sourceDDL =
                String.format(
                        "CREATE TABLE ancient_db ("
                                + " `id` INT NOT NULL,"
                                + " date_col DATE,"
                                + " datetime_0_col TIMESTAMP(0),"
                                + " datetime_1_col TIMESTAMP(1),"
                                + " datetime_2_col TIMESTAMP(2),"
                                + " datetime_3_col TIMESTAMP(3),"
                                + " datetime_4_col TIMESTAMP(4),"
                                + " datetime_5_col TIMESTAMP(5),"
                                + " datetime_6_col TIMESTAMP(6),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'debezium.enable.time.adjuster' = '%s'"
                                + ")",
                        container.getHost(),
                        container.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        database.getDatabaseName(),
                        "ancient_times",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        enableTimeAdjuster);

        tEnv.executeSql(sourceDDL);

        TableResult result = tEnv.executeSql("SELECT * FROM ancient_db");
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        List<String> expectedRows = new ArrayList<>(expectedSnapshotResults);

        Assertions.assertThat(fetchRows(iterator, expectedRows.size()))
                .containsExactlyInAnyOrderElementsOf(expectedRows);

        createBinlogEvents(database);

        Assertions.assertThat(fetchRows(iterator, expectedStreamingResults.size()))
                .containsExactlyInAnyOrderElementsOf(expectedStreamingResults);
        result.getJobClient().get().cancel().get();
    }

    private static void createBinlogEvents(UniqueDatabase database) throws SQLException {
        // Test reading identical data in binlog stage again
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO ancient_times VALUES (\n"
                            + "    DEFAULT,\n"
                            + "    '0017-08-12',\n"
                            + "    '0016-07-13 17:17:17',\n"
                            + "    '0015-06-14 17:17:17.1',\n"
                            + "    '0014-05-15 17:17:17.12',\n"
                            + "    '0013-04-16 17:17:17.123',\n"
                            + "    '0012-03-17 17:17:17.1234',\n"
                            + "    '0011-02-18 17:17:17.12345',\n"
                            + "    '0010-01-19 17:17:17.123456'\n"
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

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String getServerId(boolean incrementalSnapshot) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }
}
