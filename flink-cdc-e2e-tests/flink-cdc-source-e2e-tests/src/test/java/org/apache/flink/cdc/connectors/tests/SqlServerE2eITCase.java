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

package org.apache.flink.cdc.connectors.tests;

import org.apache.flink.cdc.common.test.utils.JdbcProxy;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for sqlserver-cdc connector uber jar. */
@Testcontainers
class SqlServerE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerE2eITCase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String INTER_CONTAINER_SQL_SERVER_ALIAS = "mssqlserver";
    private static final Path sqlServerCdcJar =
            TestUtils.getResource("sqlserver-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
    public static final String MSSQL_SERVER_IMAGE = "mcr.microsoft.com/mssql/server:2019-latest";

    @Container
    public static final MSSQLServerContainer<?> SQL_SERVER_CONTAINER =
            new MSSQLServerContainer<>(MSSQL_SERVER_IMAGE)
                    .withPassword("Password!")
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_PID", "Standard")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_SQL_SERVER_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeEach
    public void before() {
        super.before();
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(SQL_SERVER_CONTAINER)).join();
        LOG.info("Containers are started.");
        initializeSqlServerTable("sqlserver_inventory");
    }

    @AfterEach
    public void after() {
        if (SQL_SERVER_CONTAINER != null) {
            SQL_SERVER_CONTAINER.stop();
        }
        super.after();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSqlServerCDC(boolean parallelismSnapshot) throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'sqlserver-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_SQL_SERVER_ALIAS + "',",
                        " 'port' = '" + SQL_SERVER_CONTAINER.MS_SQL_SERVER_PORT + "',",
                        " 'username' = '" + SQL_SERVER_CONTAINER.getUsername() + "',",
                        " 'password' = '" + SQL_SERVER_CONTAINER.getPassword() + "',",
                        " 'database-name' = 'inventory',",
                        " 'table-name' = 'dbo.products',",
                        " 'scan.incremental.snapshot.enabled' = '" + parallelismSnapshot + "',",
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

        submitSQLJob(sqlLines, sqlServerCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate change stream
        try (Connection conn = getSqlServerJdbcConnection();
                Statement statement = conn.createStatement()) {

            statement.execute(
                    "UPDATE inventory.dbo.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.dbo.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.dbo.products WHERE id=111;");
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
                        "104,hammer,12oz carpenter's hammer,0.75",
                        "105,hammer,14oz carpenter's hammer,0.875",
                        "106,hammer,18oz carpenter hammer,1.0",
                        "107,rocks,box of assorted rocks,5.1",
                        "108,jacket,water resistent black wind breaker,0.1",
                        "109,spare tire,24 inch spare tire,22.2",
                        "110,jacket,new water resistent white wind breaker,0.5");
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight"},
                80000L);
    }

    private void initializeSqlServerTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = SqlServerE2eITCase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getSqlServerJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getSqlServerJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                SQL_SERVER_CONTAINER.getJdbcUrl(),
                SQL_SERVER_CONTAINER.getUsername(),
                SQL_SERVER_CONTAINER.getPassword());
    }
}
