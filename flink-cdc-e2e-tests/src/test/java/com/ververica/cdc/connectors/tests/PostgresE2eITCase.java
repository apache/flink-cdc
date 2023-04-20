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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

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

import static org.junit.Assert.assertNotNull;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** End-to-end tests for postgres-cdc connector uber jar. */
public class PostgresE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresE2eITCase.class);
    private static final String PG_TEST_USER = "postgres";
    private static final String PG_TEST_PASSWORD = "postgres";
    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String INTER_CONTAINER_PG_ALIAS = "postgres";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:9.6").asCompatibleSubstituteFor("postgres");

    private static final Path postgresCdcJar = TestUtils.getResource("postgres-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    @ClassRule
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername(PG_TEST_USER)
                    .withPassword(PG_TEST_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_PG_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void before() {
        super.before();
        initializePostgresTable("postgres_inventory");
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void testPostgresCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'postgres-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_PG_ALIAS + "',",
                        " 'port' = '" + POSTGRESQL_PORT + "',",
                        " 'username' = '" + PG_TEST_USER + "',",
                        " 'password' = '" + PG_TEST_PASSWORD + "',",
                        " 'database-name' = '" + POSTGRES.getDatabaseName() + "',",
                        " 'schema-name' = 'inventory',",
                        " 'table-name' = 'products',",
                        " 'slot.name' = 'flink',",
                        // dropping the slot allows WAL segments to be discarded by the database
                        " 'debezium.slot.drop_on_stop' = 'true'",
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

        submitSQLJob(sqlLines, postgresCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate binlogs
        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
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
                60000L);
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private void initializePostgresTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresE2eITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            Class.forName(PG_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (Connection connection = getPgJdbcConnection();
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

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
