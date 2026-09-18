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

package org.apache.flink.cdc.connectors.mariadb.source;

import org.apache.flink.cdc.connectors.mariadb.testutils.MariaDbContainer;
import org.apache.flink.core.execution.JobClient;
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
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case proving the standalone {@code mariadb-cdc} connector performs an initial snapshot and
 * continues consuming binlog changes from a real MariaDB server. MariaDB GTID recovery is covered
 * by a later change.
 */
class MariaDbSnapshotITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbSnapshotITCase.class);

    private final MariaDbContainer mariaDb = new MariaDbContainer();

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @BeforeEach
    void before() {
        LOG.info("Starting MariaDB container...");
        Startables.deepStart(Stream.of(mariaDb)).join();
        LOG.info("MariaDB container started.");
        env.setParallelism(1);
    }

    @AfterEach
    void after() {
        mariaDb.stop();
    }

    @Test
    void testSnapshotAndStreaming() throws Exception {
        initializeTable();

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE products ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mariadb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = 'products',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '5401-5404',"
                                + " 'scan.startup.mode' = 'initial'"
                                + ")",
                        mariaDb.getHost(),
                        mariaDb.getDatabasePort(),
                        mariaDb.getUsername(),
                        mariaDb.getPassword(),
                        mariaDb.getDatabaseName()));

        TableResult result = tEnv.executeSql("SELECT id, name, weight FROM products");
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        try (CloseableIterator<Row> iterator = result.collect()) {
            List<String> snapshot = fetchRows(iterator, 3);
            assertThat(snapshot)
                    .containsExactlyInAnyOrder(
                            "+I[101, scooter, 3.140]",
                            "+I[102, car battery, 8.100]",
                            "+I[103, hammer, 0.750]");

            makeBinlogChanges();

            List<String> binlog = fetchRows(iterator, 4);
            assertThat(binlog)
                    .containsExactly(
                            "+I[104, drill, 1.250]",
                            "-U[101, scooter, 3.140]",
                            "+U[101, scooter, 3.500]",
                            "-D[102, car battery, 8.100]");
        } finally {
            jobClient.cancel().get();
        }
    }

    private void makeBinlogChanges() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                mariaDb.getJdbcUrl(),
                                mariaDb.getUsername(),
                                mariaDb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO products VALUES (104, 'drill', 1.25)");
            stmt.execute("UPDATE products SET weight = 3.5 WHERE id = 101");
            stmt.execute("DELETE FROM products WHERE id = 102");
        }
    }

    private void initializeTable() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                mariaDb.getJdbcUrl(),
                                mariaDb.getUsername(),
                                mariaDb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE products ("
                            + " id INTEGER NOT NULL PRIMARY KEY,"
                            + " name VARCHAR(255) NOT NULL,"
                            + " weight DECIMAL(10,3))");
            stmt.execute(
                    "INSERT INTO products VALUES"
                            + " (101, 'scooter', 3.14),"
                            + " (102, 'car battery', 8.1),"
                            + " (103, 'hammer', 0.75)");
        }
    }

    private static List<String> fetchRows(CloseableIterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            rows.add(iter.next().toString());
            size--;
        }
        return rows;
    }
}
