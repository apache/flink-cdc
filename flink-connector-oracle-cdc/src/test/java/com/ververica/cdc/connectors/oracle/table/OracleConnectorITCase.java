/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Integration tests for Oracle binlog SQL source. */
public class OracleConnectorITCase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectorITCase.class);

    private OracleContainer oracleContainer;
    private static final DockerImageName OC_IMAGE =
            DockerImageName.parse("thake/oracle-xe-11g").asCompatibleSubstituteFor("oracle");

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    @Before
    public void before() throws Exception {
        oracleContainer =
                new OracleContainer(OC_IMAGE)
                        .withClasspathResourceMapping(
                                "docker/setup.sh", "/etc/logminer_conf.sh", BindMode.READ_WRITE)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(oracleContainer)).join();
        LOG.info("Containers are started.");

        TestValuesTableFactory.clearAllData();
        Container.ExecResult execResult1 =
                oracleContainer.execInContainer("chmod", "+x", "/etc/logminer_conf.sh");

        execResult1.getStdout();
        execResult1.getStderr();

        Container.ExecResult execResult =
                oracleContainer.execInContainer("/bin/sh", "-c", "/etc/logminer_conf.sh");
        execResult.getStdout();
        execResult.getStderr();
        env.setParallelism(1);
    }

    @Test
    public void testConsumingAllEvents()
            throws SQLException, ExecutionException, InterruptedException {
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = 'XE',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        oracleContainer.getHost(),
                        oracleContainer.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        "debezium",
                        "products");
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT NAME, SUM(WEIGHT) FROM debezium_source GROUP BY NAME");

        waitForSnapshotStarted("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.1 WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT=0.5 WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.17 WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
        }

        waitForSinkSize("sink", 20);

        String[] expected =
                new String[] {
                    "+I[scooter, 3.140]",
                    "+I[car battery, 8.100]",
                    "+I[12-pack drill bits, 0.800]",
                    "+I[hammer, 2.625]",
                    "+I[rocks, 5.100]",
                    "+I[jacket, 0.600]",
                    "+I[spare tire, 22.200]"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testStartupFromLatestOffset() throws Exception {
        // database.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = 'XE',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s' ,"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        oracleContainer.getHost(),
                        oracleContainer.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        "debezium",
                        "products");
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        Thread.sleep(5000L);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO debezium.products VALUES (110,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET description='new water resistent white wind breaker', weight=0.5 WHERE id=110");
            statement.execute("UPDATE debezium.products SET weight=5.17 WHERE id=111");
            statement.execute("DELETE FROM debezium.products WHERE id=111");
        }

        waitForSinkSize("sink", 7);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }

    // ------------------------------------------------------------------------------------

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

    public Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), "system", "oracle");
    }
}
