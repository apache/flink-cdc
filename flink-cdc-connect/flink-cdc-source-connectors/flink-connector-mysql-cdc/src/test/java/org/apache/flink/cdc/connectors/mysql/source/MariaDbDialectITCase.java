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

import org.apache.flink.cdc.connectors.mysql.testutils.MariaDbContainer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase.assertEqualsInAnyOrder;

/**
 * IT case for proving the {@code mysql-cdc} connector reads a real MariaDB server when {@code
 * scan.dialect=mariadb}: the snapshot rows are captured, then binlog INSERT/UPDATE/DELETE changes
 * are captured.
 */
public class MariaDbDialectITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbDialectITCase.class);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private MariaDbContainer mariaDbContainer;

    @BeforeEach
    void setup() {
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        mariaDbContainer = new MariaDbContainer();
        mariaDbContainer.withLogConsumer(new Slf4jLogConsumer(LOG));
        LOG.info("Starting MariaDb Container");
        Startables.deepStart(mariaDbContainer).join();
        LOG.info("Started MariaDb Container");
    }

    @AfterEach
    void tearDown() {
        if (mariaDbContainer != null) {
            mariaDbContainer.stop();
        }
    }

    @Test
    void testMariaDbSnapshotAndBinlog() throws Exception {
        LOG.info("Testing MariaDb Snapshot and Binlog");
        initializeMariaDbTable();

        String sourceDDL =
                String.format(
                        "CREATE TABLE products ("
                                + "`id` INT NOT NULL,"
                                + "name STRING,"
                                + "wight DECIMAL(10,3),"
                                + "primary key (`id`) not enforced"
                                + ") with ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.dialect' = 'mariadb',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        mariaDbContainer.getHost(),
                        mariaDbContainer.getDatabasePort(),
                        mariaDbContainer.getUsername(),
                        mariaDbContainer.getPassword(),
                        mariaDbContainer.getDatabaseName(),
                        "products",
                        getServerId());
        tEnv.executeSql(sourceDDL);

        TableResult result = tEnv.executeSql("SELECT `id`, `name`, `wight` FROM products");
        CloseableIterator<Row> collect = result.collect();

        String[] expectSnapshot = {
            "+I[101, scooter, 3.440]", "+I[102, car, 8.111]", "+I[103, hammer, 811.550]"
        };

        assertEqualsInAnyOrder(
                Arrays.asList(expectSnapshot), fetchRows(collect, expectSnapshot.length));

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE products SET `wight` = 5.100 WHERE `id` = 103;");
            statement.execute("INSERT INTO products values (104, 'jacket', 0.200);");
            statement.execute("DELETE FROM products WHERE `id` = 101;");
        }

        String[] expectedBinlog = {
            "-U[103, hammer, 811.550]",
            "+U[103, hammer, 5.100]",
            "+I[104, jacket, 0.200]",
            "-D[101, scooter, 3.440]"
        };
        assertEqualsInAnyOrder(
                Arrays.asList(expectedBinlog), fetchRows(collect, expectedBinlog.length));

        result.getJobClient().get().cancel().get();
    }

    @Test
    void testMariaDbTimestampStartup() throws Exception {
        LOG.info("Testing MariaDb Timestamp start up");
        initializeMariaDbTable();

        Thread.sleep(2000);
        long startTimestampMillis = System.currentTimeMillis();
        Thread.sleep(2000);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO products values (104, 'jacket', 0.200);");
            statement.execute("UPDATE products SET `wight` = 5.100 WHERE `id` = 103;");
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE timestamp_products ("
                                + "`id` INT NOT NULL,"
                                + "name STRING,"
                                + "wight DECIMAL(10,3),"
                                + "primary key (`id`) not enforced"
                                + ") with ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.dialect' = 'mariadb',"
                                + " 'scan.startup.mode' = 'timestamp',"
                                + " 'scan.startup.timestamp-millis' = '%s',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        startTimestampMillis,
                        mariaDbContainer.getHost(),
                        mariaDbContainer.getDatabasePort(),
                        mariaDbContainer.getUsername(),
                        mariaDbContainer.getPassword(),
                        mariaDbContainer.getDatabaseName(),
                        "products",
                        getServerId());
        tEnv.executeSql(sourceDDL);

        TableResult result =
                tEnv.executeSql("SELECT `id`, `name`, `wight` FROM timestamp_products");
        CloseableIterator<Row> collect = result.collect();

        String[] expectedTimestampRows = {
            "+I[104, jacket, 0.200]", "-U[103, hammer, 811.550]", "+U[103, hammer, 5.100]"
        };

        assertEqualsInAnyOrder(
                Arrays.asList(expectedTimestampRows),
                fetchRows(collect, expectedTimestampRows.length));
        result.getJobClient().get().cancel().get();
    }

    private void initializeMariaDbTable() throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE products ("
                            + "id INT NOT NULL PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "wight DECIMAL(10,3));");

            statement.execute(
                    "INSERT INTO products VALUES (101, 'scooter', 3.44),"
                            + "(102, 'car', 8.111),"
                            + "(103, 'hammer', 811.55)");
        }
    }

    private Connection getJdbcConnection() throws Exception {
        return DriverManager.getConnection(
                mariaDbContainer.getJdbcUrl(),
                mariaDbContainer.getUsername(),
                mariaDbContainer.getPassword());
    }

    private String getServerId() {
        Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }
}
