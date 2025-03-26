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
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceITCase;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.CONNECTOR_PWD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.CONNECTOR_USER;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.ORACLE_DATABASE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TEST_PWD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TEST_USER;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TOP_SECRET;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TOP_USER;

/** End-to-end tests for oracle-cdc connector uber jar. */
class OracleE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OracleE2eITCase.class);
    protected static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String INTER_CONTAINER_ORACLE_ALIAS = "oracle";
    private static final Path oracleCdcJar = TestUtils.getResource("oracle-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
    private static final Path oracleOjdbcJar = TestUtils.getResource("oracle-ojdbc.jar");
    private static final Path oracleXdbJar = TestUtils.getResource("oracle-xdb.jar");
    public static final String ORACLE_IMAGE = "goodboy008/oracle-19.3.0-ee";
    private static OracleContainer oracle;

    @BeforeEach
    public void before() {
        super.before();
        LOG.info("Starting containers...");

        oracle =
                new OracleContainer(
                                DockerImageName.parse(ORACLE_IMAGE)
                                        .withTag(
                                                DockerClientFactory.instance()
                                                                .client()
                                                                .versionCmd()
                                                                .exec()
                                                                .getArch()
                                                                .equals("amd64")
                                                        ? "non-cdb"
                                                        : "arm-non-cdb"))
                        .withUsername(CONNECTOR_USER)
                        .withPassword(CONNECTOR_PWD)
                        .withDatabaseName(ORACLE_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_ORACLE_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withReuse(true);

        Startables.deepStart(Stream.of(oracle)).join();
        initializeOracleTable("oracle_inventory");
        LOG.info("Containers are started.");
    }

    @AfterEach
    public void after() {
        if (oracle != null) {
            oracle.stop();
        }
        super.after();
    }

    @Test
    void testOracleCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "CREATE TABLE products_source (",
                        " ID INT NOT NULL,",
                        " NAME STRING,",
                        " DESCRIPTION STRING,",
                        " WEIGHT DECIMAL(10,3),",
                        " primary key (`ID`) not enforced",
                        ") WITH (",
                        " 'connector' = 'oracle-cdc',",
                        " 'hostname' = '" + oracle.getNetworkAliases().get(0) + "',",
                        " 'port' = '" + oracle.getExposedPorts().get(0) + "',",
                        // To analyze table for approximate rowCnt computation, use admin user
                        // before chunk splitting.
                        " 'username' = '" + TOP_USER + "',",
                        " 'password' = '" + TOP_SECRET + "',",
                        " 'database-name' = 'ORCLCDB',",
                        " 'schema-name' = 'DEBEZIUM',",
                        " 'scan.incremental.snapshot.enabled' = 'true',",
                        " 'debezium.log.mining.strategy' = 'online_catalog',",
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

        submitSQLJob(sqlLines, oracleCdcJar, jdbcJar, mysqlDriverJar, oracleOjdbcJar, oracleXdbJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate redo log
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
        // Oracle cdc's backfill task will cost much time, increase the timeout here
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight"},
                300000L);
    }

    private static Connection getOracleJdbcConnection() throws SQLException {
        return DriverManager.getConnection(oracle.getJdbcUrl(), TEST_USER, TEST_PWD);
    }

    private static void initializeOracleTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OracleSourceITCase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getOracleJdbcConnection();
                Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            // region Drop all user tables in Debezium schema
            listTables(connection)
                    .forEach(
                            tableId -> {
                                try {
                                    statement.execute(
                                            "DROP TABLE "
                                                    + String.join(
                                                            ".",
                                                            tableId.schema(),
                                                            tableId.table()));
                                } catch (SQLException e) {
                                    LOG.warn("drop table error, table:{}", tableId, e);
                                }
                            });
            // endregion

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
        } catch (SQLException | IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------ utils -----------------------
    protected static List<TableId> listTables(Connection connection) {

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX') "
                        + "AND NESTED = 'NO' AND TABLE_NAME NOT IN (SELECT PARENT_TABLE_NAME FROM ALL_NESTED_TABLES)";
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(queryTablesSql);
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                String tableName = resultSet.getString(2);
                TableId tableId = new TableId(ORACLE_DATABASE, schemaName, tableName);
                tableIdSet.add(tableId);
            }
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }
        return new ArrayList<>(tableIdSet);
    }
}
