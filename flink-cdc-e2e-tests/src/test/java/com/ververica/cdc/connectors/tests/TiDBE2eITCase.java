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
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** End-to-end tests for tidb-cdc connector uber jar. */
public class TiDBE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBE2eITCase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public static final String PD_SERVICE_NAME = "pd0";
    public static final String TIKV_SERVICE_NAME = "tikv0";
    public static final String TIDB_SERVICE_NAME = "tidb0";

    public static final String TIDB_USER = "root";
    public static final String TIDB_PASSWORD = "";

    public static final int TIDB_PORT = 4000;
    public static final int TIKV_PORT = 20160;
    public static final int PD_PORT = 2379;

    private static final Path tidbCdcJar = TestUtils.getResource("tidb-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    @ClassRule
    public static final GenericContainer<?> PD =
            new GenericContainer<>("pingcap/pd:v6.0.0")
                    .withExposedPorts(PD_PORT)
                    .withFileSystemBind("src/test/resources/docker/tidb/pd.toml", "/pd.toml")
                    .withCommand(
                            "--name=pd0",
                            "--client-urls=http://0.0.0.0:2379",
                            "--peer-urls=http://0.0.0.0:2380",
                            "--advertise-client-urls=http://pd0:2379",
                            "--advertise-peer-urls=http://pd0:2380",
                            "--initial-cluster=pd0=http://pd0:2380",
                            "--data-dir=/data/pd0",
                            "--config=/pd.toml",
                            "--log-file=/logs/pd0.log")
                    .withNetwork(NETWORK)
                    .withNetworkMode("host")
                    .withNetworkAliases(PD_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> TIKV =
            new GenericContainer<>("pingcap/tikv:v6.0.0")
                    .withExposedPorts(TIKV_PORT)
                    .withFileSystemBind("src/test/resources/docker/tidb/tikv.toml", "/tikv.toml")
                    .withCommand(
                            "--addr=0.0.0.0:20160",
                            "--advertise-addr=tikv0:20160",
                            "--data-dir=/data/tikv0",
                            "--pd=pd0:2379",
                            "--config=/tikv.toml",
                            "--log-file=/logs/tikv0.log")
                    .withNetwork(NETWORK)
                    .dependsOn(PD)
                    .withNetworkAliases(TIKV_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> TIDB =
            new GenericContainer<>("pingcap/tidb:v6.0.0")
                    .withExposedPorts(TIDB_PORT)
                    .withFileSystemBind("src/test/resources/docker/tidb/tidb.toml", "/tidb.toml")
                    .withCommand(
                            "--store=tikv",
                            "--path=pd0:2379",
                            "--config=/tidb.toml",
                            "--advertise-address=tidb0")
                    .withNetwork(NETWORK)
                    .dependsOn(TIKV)
                    .withNetworkAliases(TIDB_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void before() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(PD, TIKV, TIDB)).join();
        LOG.info("Containers are started.");
        super.before();
        initializeTidbTable("tidb_inventory");
    }

    @After
    public void after() {
        LOG.info("Stopping containers...");
        Stream.of(TIDB, TIKV, PD).forEach(GenericContainer::stop);
        log.info("Containers are stopped.");
        super.after();
    }

    @Test
    public void testTIDBCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE tidb_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(20, 10),",
                        " PRIMARY KEY (`id`) NOT ENFORCED",
                        ") WITH (",
                        " 'connector' = 'tidb-cdc',",
                        " 'tikv.grpc.timeout_in_ms' = '20000',",
                        " 'pd-addresses' = '" + PD_SERVICE_NAME + ":" + PD_PORT + "',",
                        " 'database-name' = 'inventory',",
                        " 'table-name' = 'products'",
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
                        "SELECT * FROM tidb_source;");

        submitSQLJob(sqlLines, tidbCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
        try (Connection connection = getTidbJdbcConnection("inventory");
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");

            ResultSet resultSet = statement.executeQuery("SELECT count(1) FROM  products");
            int recordCount = 0;
            while (resultSet.next()) {
                recordCount = resultSet.getInt(1);
            }
            assertEquals(recordCount, 10);
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // assert final results
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        JdbcProxy proxy =
                new JdbcProxy(
                        mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
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
                360000L);
    }

    protected Connection getTidbJdbcConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mysql://"
                        + TIDB.getContainerIpAddress()
                        + ":"
                        + TIDB.getMappedPort(TIDB_PORT)
                        + "/"
                        + databaseName,
                TIDB_USER,
                TIDB_PASSWORD);
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeTidbTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TiDBE2eITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getTidbJdbcConnection("");
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
}
