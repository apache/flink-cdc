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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.supportCheckpointsAfterTasksFinished;

/** End-to-end tests for oceanbase-cdc connector uber jar. */
@Testcontainers
class OceanBaseE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseE2eITCase.class);

    private static final Path oceanbaseCdcJar =
            TestUtils.getResource("oceanbase-cdc-connector.jar");
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String[] CREATE_DATABASE_DDL =
            new String[] {"CREATE DATABASE `$DBNAME$`;", "USE `$DBNAME$`;"};
    private static final Path jdbcDriver = TestUtils.getResource("mysql-driver.jar");
    private static final String DATABASE_NAME = "oceanbase_inventory";

    private static final int OB_PROXY_PORT = 2883;
    protected static final String DEFAULT_USERNAME = "root@test";
    protected static final String DEFAULT_PASSWORD = "123456";
    protected static final String INTER_CONTAINER_OCEANBASE_ALIAS = "oceanbase";

    @Container
    @SuppressWarnings("resource")
    public static final GenericContainer<?> OB_BINLOG_CONTAINER =
            new GenericContainer<>("quay.io/oceanbase/obbinlog-ce:4.2.5-test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_OCEANBASE_ALIAS)
                    .withStartupTimeout(Duration.ofMinutes(5))
                    .withExposedPorts(2881, 2883)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .waitingFor(
                            new LogMessageWaitStrategy()
                                    .withRegEx(".*OBBinlog is ready!.*")
                                    .withTimes(1)
                                    .withStartupTimeout(Duration.ofMinutes(6)));

    @BeforeEach
    public void before() {
        super.before();
        createAndInitializeOBTable("oceanbase_inventory");
    }

    @AfterEach
    public void after() {
        super.after();
    }

    @Test
    void testOBBinlogCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "SET 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';",
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " point_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'oceanbase-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_OCEANBASE_ALIAS + "',",
                        " 'port' = '2883',",
                        " 'username' = '" + DEFAULT_USERNAME + "',",
                        " 'password' = '" + DEFAULT_PASSWORD + "',",
                        " 'database-name' = '" + DATABASE_NAME + "',",
                        " 'table-name' = 'products_source',",
                        " 'server-time-zone' = 'Asia/Shanghai',",
                        " 'server-id' = '5800-5900',",
                        " 'scan.incremental.snapshot.chunk.size' = '4',",
                        " 'scan.incremental.close-idle-reader.enabled' = '"
                                + supportCheckpointsAfterTasksFinished()
                                + "'",
                        ");",
                        "CREATE TABLE products_sink (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " point_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:2883/%s',",
                                INTER_CONTAINER_OCEANBASE_ALIAS, DATABASE_NAME),
                        " 'table-name' = 'products_sink',",
                        " 'username' = '" + DEFAULT_USERNAME + "',",
                        " 'password' = '" + DEFAULT_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT * FROM products_source;");

        submitSQLJob(sqlLines, oceanbaseCdcJar, jdbcJar, jdbcDriver);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        OB_BINLOG_CONTAINER.getHost(),
                        OB_BINLOG_CONTAINER.getMappedPort(OB_PROXY_PORT),
                        DATABASE_NAME);
        try (Connection conn =
                        DriverManager.getConnection(jdbcUrl, DEFAULT_USERNAME, DEFAULT_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE products_source SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products_source SET weight='5.1' WHERE id=107;");
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null);");
            stat.execute(
                    "UPDATE products_source SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products_source SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products_source WHERE id=111;");
            // add schema change event in the last.
            stat.execute("CREATE TABLE new_table (id int, age int);");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        // assert final results
        JdbcProxy proxy =
                new JdbcProxy(jdbcUrl, DEFAULT_USERNAME, DEFAULT_PASSWORD, MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"},{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"},{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"},{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"},{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"},{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106,hammer,18oz carpenter hammer,1.0,null,null,null",
                        "107,rocks,box of assorted rocks,5.1,null,null,null",
                        "108,jacket,water resistent black wind breaker,0.1,null,null,null",
                        "109,spare tire,24 inch spare tire,22.2,null,null,null",
                        "110,jacket,new water resistent white wind breaker,0.5,null,null,null");
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight", "enum_c", "json_c", "point_c"},
                60000L);
    }

    /** Creates the database and populates it with initialization SQL script. */
    public void createAndInitializeOBTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OceanBaseE2eITCase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try {
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                final List<String> statements =
                        Arrays.stream(
                                        Stream.concat(
                                                        Arrays.stream(CREATE_DATABASE_DDL),
                                                        Files.readAllLines(
                                                                Paths.get(ddlTestFile.toURI()))
                                                                .stream())
                                                .map(String::trim)
                                                .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                                .map(
                                                        x -> {
                                                            final Matcher m =
                                                                    COMMENT_PATTERN.matcher(x);
                                                            return m.matches() ? m.group(1) : x;
                                                        })
                                                .map(sql -> sql.replace("$DBNAME$", DATABASE_NAME))
                                                .collect(Collectors.joining("\n"))
                                                .split(";"))
                                .map(x -> x.replace("$$", ";"))
                                .collect(Collectors.toList());
                for (String stmt : statements) {
                    statement.execute(stmt);
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Connection getJdbcConnection() throws SQLException {
        String url =
                "jdbc:mysql://"
                        + OB_BINLOG_CONTAINER.getHost()
                        + ":"
                        + OB_BINLOG_CONTAINER.getMappedPort(OB_PROXY_PORT)
                        + "?useSSL=false";
        return DriverManager.getConnection(url, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }
}
