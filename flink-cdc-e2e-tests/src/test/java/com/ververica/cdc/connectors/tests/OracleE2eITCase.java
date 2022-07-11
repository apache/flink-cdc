/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.tests;

import com.ververica.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;
import com.ververica.cdc.connectors.tests.utils.JdbcProxy;
import com.ververica.cdc.connectors.tests.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** End-to-end tests for oracle-cdc connector uber jar. */
public class OracleE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OracleE2eITCase.class);
    private static final String ORACLE_SYSTEM_USER = "system";
    private static final String ORACLE_SYSTEM_PASSWORD = "oracle";
    private static final String ORACLE_TEST_USER = "dbzuser";
    private static final String ORACLE_TEST_PASSWORD = "dbz";
    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String INTER_CONTAINER_ORACLE_ALIAS = "oracle";
    private static final String ORACLE_IMAGE = "jark/oracle-xe-11g-r2-cdc:0.1";
    private static final int ORACLE_PORT = 1521;

    private static final Path oracleCdcJar = TestUtils.getResource("oracle-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    public OracleContainer oracle;

    @Before
    public void before() {
        super.before();
        LOG.info("Starting containers...");
        oracle =
                new OracleContainer(ORACLE_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_ORACLE_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(Stream.of(oracle)).join();
        LOG.info("Containers are started.");
    }

    @After
    public void after() {
        if (oracle != null) {
            oracle.stop();
        }
        super.after();
    }

    @Test
    public void testOracleCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE products_source (",
                        " ID INT NOT NULL,",
                        " NAME STRING,",
                        " DESCRIPTION STRING,",
                        " WEIGHT DECIMAL(10,3),",
                        " primary key (`ID`) not enforced",
                        ") WITH (",
                        " 'connector' = 'oracle-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_ORACLE_ALIAS + "',",
                        " 'port' = '" + ORACLE_PORT + "',",
                        " 'username' = '" + ORACLE_SYSTEM_USER + "',",
                        " 'password' = '" + ORACLE_SYSTEM_PASSWORD + "',",
                        " 'database-name' = 'XE',",
                        " 'schema-name' = 'DEBEZIUM',",
                        " 'debezium.log.mining.strategy' = 'online_catalog',",
                        " 'debezium.log.mining.continuous.mine' = 'true',",
                        " 'table-name' = 'PRODUCTS',",
                        " 'scan.incremental.snapshot.chunk.size' = '4'",
                        ");",
                        "CREATE TABLE products_sink (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = 'products_sink',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT * FROM products_source;");

        submitSQLJob(sqlLines, oracleCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
        Class.forName(ORACLE_DRIVER_CLASS);
        // we need to set this property, otherwise Azure Pipeline will complain
        // "ORA-01882: timezone region not found" error when building the Oracle JDBC connection
        // see https://stackoverflow.com/a/9177263/4915129
        System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
        try (Connection conn = getOracleJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.1 WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT=0.5 WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.17 WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // assert final results
        String mysqlUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        JdbcProxy proxy =
                new JdbcProxy(mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14",
                        "102,car battery,12V car battery,8.1",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8",
                        "104,hammer,12oz carpenters hammer,0.75",
                        "105,hammer,14oz carpenters hammer,0.875",
                        "106,hammer,18oz carpenter hammer,1.0",
                        "107,rocks,box of assorted rocks,5.1",
                        "108,jacket,water resistent black wind breaker,0.1",
                        "109,spare tire,24 inch spare tire,22.2",
                        "111,jacket,new water resistent white wind breaker,0.5");
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight"},
                150000L);
    }

    private Connection getOracleJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                oracle.getJdbcUrl(), ORACLE_TEST_USER, ORACLE_TEST_PASSWORD);
    }
}
