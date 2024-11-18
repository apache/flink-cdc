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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/** Integration tests for MySQL sharding tables. */
public class MySqlConnectorShardingTableITCase extends MySqlSourceTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlConnectorShardingTableITCase.class);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase userDatabase1 =
            new UniqueDatabase(MYSQL_CONTAINER, "user_1", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase userDatabase2 =
            new UniqueDatabase(MYSQL_CONTAINER, "user_2", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

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

    public void setup(boolean incrementalSnapshot) {
        TestValuesTableFactory.clearAllData();
        if (incrementalSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    public void testShardingTablesWithTinyInt1(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        fullTypesMySql57Database.createAndInitialize();
        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE %s", fullTypesMySql57Database.getDatabaseName()));
            statement.execute(
                    "CREATE TABLE sharding_table_1("
                            + "id BIGINT,"
                            + "status BOOLEAN," // will be recognized as tinyint(1) in debezium as
                            // it comes from show table command
                            + " PRIMARY KEY (id) "
                            + ")");
            statement.execute("INSERT INTO sharding_table_1 values(1, true),(2, false)");
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE sharding_tables (\n"
                                + "`id` BIGINT,"
                                + "status TINYINT,"
                                + "primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        fullTypesMySql57Database.getUsername(),
                        fullTypesMySql57Database.getPassword(),
                        fullTypesMySql57Database.getDatabaseName(),
                        "sharding_table_.*",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` BIGINT NOT NULL,"
                        + " status TINYINT,"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM sharding_tables");

        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO sharding_table_1 values(3, true),(4, false)");
        }
        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 4);

        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE %s", fullTypesMySql57Database.getDatabaseName()));
            statement.execute(
                    "CREATE TABLE sharding_table_2("
                            + "id BIGINT,"
                            + "status BOOLEAN," // will be recognized as boolean in debezium as it
                            // comes from binlog
                            + " PRIMARY KEY (id) "
                            + ")");
            statement.execute("INSERT INTO sharding_table_2 values(5, true),(6, false)");
        }

        waitForSinkSize("sink", 6);
        String[] expected =
                new String[] {
                    "+I[1, 1]", "+I[2, 0]", "+I[3, 1]", "+I[4, 0]", "+I[5, 1]", "+I[6, 0]",
                };
        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    public void testShardingTablesWithInconsistentSchema(boolean incrementalSnapshot)
            throws Exception {
        setup(incrementalSnapshot);
        userDatabase1.createAndInitialize();
        userDatabase2.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE `user` ("
                                + " `id` DECIMAL(20, 0) NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " email STRING,"
                                + " age INT,"
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
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        userDatabase1.getUsername(),
                        userDatabase1.getPassword(),
                        String.format(
                                "(%s|%s)",
                                userDatabase1.getDatabaseName(), userDatabase2.getDatabaseName()),
                        "user_table_.*",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM `user`");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = userDatabase1.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE user_table_1_1 SET email = 'user_111@bar.org' WHERE id=111;");
        }

        try (Connection connection = userDatabase2.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE user_table_2_2 SET age = 20 WHERE id=221;");
        }

        String[] expected =
                new String[] {
                    "+I[111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                    "-U[111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                    "+U[111, user_111, Shanghai, 123567891234, user_111@bar.org, null]",
                    "+I[121, user_121, Shanghai, 123567891234, null, null]",
                    "+I[211, user_211, Shanghai, 123567891234, null, null]",
                    "+I[221, user_221, Shanghai, 123567891234, null, 18]",
                    "-U[221, user_221, Shanghai, 123567891234, null, 18]",
                    "+U[221, user_221, Shanghai, 123567891234, null, 20]",
                };

        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    // ------------------------------------------------------------------------------------

    private String getServerId(boolean incrementalSnapshot) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }

    protected String getServerId(int base, boolean incrementalSnapshot) {
        if (incrementalSnapshot) {
            return base + "-" + (base + DEFAULT_PARALLELISM);
        }
        return String.valueOf(base);
    }

    private int getSplitSize(boolean incrementalSnapshot) {
        if (incrementalSnapshot) {
            // test parallel read
            return 4;
        }
        return 0;
    }

    private static String buildColumnsDDL(
            String columnPrefix, int start, int end, String dataType) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = start; i < end; i++) {
            stringBuilder.append(columnPrefix).append(i).append(" ").append(dataType).append(",");
        }
        return stringBuilder.toString();
    }

    private static String getIntegerSeqString(int start, int end) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = start; i < end - 1; i++) {
            stringBuilder.append(i).append(", ");
        }
        stringBuilder.append(end - 1);
        return stringBuilder.toString();
    }

    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(100);
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
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

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }
}
