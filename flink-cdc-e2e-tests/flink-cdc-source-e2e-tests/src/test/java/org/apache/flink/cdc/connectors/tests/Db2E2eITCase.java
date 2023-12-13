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
import org.apache.flink.cdc.connectors.db2.Db2TestBase;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.utility.DockerImageName;

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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** End-to-end tests for db2 cdc connector uber jar. */
public class Db2E2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(Db2E2eITCase.class);
    private static final String INTER_CONTAINER_DB2_ALIAS = "db2";

    private static final Path db2CdcJar = TestUtils.getResource("db2-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
    private static final Path db2JccJar = TestUtils.getResource("db2-jcc.jar");

    public static final String DB2_IMAGE = "ibmcom/db2";
    public static final String DB2_CUSTOM_IMAGE = "custom/db2-cdc:1.4";
    private static DockerImageName debeziumDockerImageName;
    private static boolean db2AsnAgentRunning = false;
    private static Db2Container db2Container;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    @Parameterized.Parameter(1)
    public boolean parallelismSnapshot;

    @Parameterized.Parameters(name = "flinkVersion: {0}, parallelismSnapshot: {1}")
    public static List<Object[]> parameters() {
        final List<String> flinkVersions = getFlinkVersion();
        List<Object[]> params = new ArrayList<>();
        for (String flinkVersion : flinkVersions) {
            params.add(new Object[] {flinkVersion, true});
            params.add(new Object[] {flinkVersion, false});
        }
        return params;
    }

    @BeforeClass
    public static void beforeClass() {
        debeziumDockerImageName =
                DockerImageName.parse(
                                new ImageFromDockerfile(DB2_CUSTOM_IMAGE)
                                        .withDockerfile(getFilePath("docker/db2/Dockerfile"))
                                        .get())
                        .asCompatibleSubstituteFor(DB2_IMAGE);
    }

    @Before
    public void before() {
        super.before();
        LOG.info("Starting db2 containers...");
        db2Container =
                new Db2Container(debeziumDockerImageName)
                        .withDatabaseName("testdb")
                        .withUsername("db2inst1")
                        .withPassword("flinkpw")
                        .withEnv("AUTOCONFIG", "false")
                        .withEnv("ARCHIVE_LOGS", "true")
                        .acceptLicense()
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_DB2_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withLogConsumer(
                                outputFrame -> {
                                    if (outputFrame
                                            .getUtf8String()
                                            .contains("The asncdc program enable finished")) {
                                        db2AsnAgentRunning = true;
                                    }
                                });
        Startables.deepStart(Stream.of(db2Container)).join();
        LOG.info("Db2 containers are started.");

        LOG.info("Waiting db2 asn agent start...");
        while (!db2AsnAgentRunning) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                LOG.error("unexpected interrupted exception", e);
            }
        }
        LOG.info("Db2 asn agent are started.");
    }

    @After
    public void after() {
        if (db2Container != null) {
            db2Container.close();
        }
        db2AsnAgentRunning = false;
        super.after();
    }

    @Test
    public void testDb2CDC() throws Exception {
        initializeDb2Table("inventory", "PRODUCTS");
        List<String> sqlLines =
                Arrays.asList(
                        String.format(
                                "CREATE TABLE products_source ("
                                        + " ID INT NOT NULL,"
                                        + " NAME STRING,"
                                        + " DESCRIPTION STRING,"
                                        + " WEIGHT DECIMAL(10,3),"
                                        + " primary key (`ID`) not enforced"
                                        + ") WITH ("
                                        + " 'connector' = 'db2-cdc',"
                                        + " 'hostname' = '%s',"
                                        + " 'port' = '%s',"
                                        + " 'username' = '%s',"
                                        + " 'password' = '%s',"
                                        + " 'database-name' = '%s',"
                                        + " 'table-name' = '%s',"
                                        + " 'scan.incremental.snapshot.enabled' = '"
                                        + parallelismSnapshot
                                        + "',"
                                        + " 'scan.incremental.snapshot.chunk.size' = '4'"
                                        + ");",
                                INTER_CONTAINER_DB2_ALIAS,
                                DB2_PORT,
                                db2Container.getUsername(),
                                db2Container.getPassword(),
                                db2Container.getDatabaseName(),
                                "DB2INST1.PRODUCTS"),
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

        submitSQLJob(sqlLines, db2CdcJar, jdbcJar, mysqlDriverJar, db2JccJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        try (Connection conn = getDb2Connection();
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.1' WHERE ID=107;");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='new water resistent white wind breaker', WEIGHT='0.5' WHERE ID=110;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.17' WHERE ID=111;");
            statement.execute("DELETE FROM DB2INST1.PRODUCTS WHERE ID=111;");
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
                150000L);
    }

    private Connection getDb2Connection() throws SQLException {
        return DriverManager.getConnection(
                db2Container.getJdbcUrl(), db2Container.getUsername(), db2Container.getPassword());
    }

    private static Path getFilePath(String resourceFilePath) {
        Path path = null;
        try {
            URL filePath = Db2E2eITCase.class.getClassLoader().getResource(resourceFilePath);
            assertNotNull("Cannot locate " + resourceFilePath, filePath);
            path = Paths.get(filePath.toURI());
        } catch (URISyntaxException e) {
            LOG.error("Cannot get path from URI.", e);
        }
        return path;
    }

    private static void dropTestTable(Connection connection, String tableName) {

        try {
            Awaitility.await(String.format("cdc remove table %s", tableName))
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "CALL ASNCDC.REMOVETABLE('DB2INST1', '%s')",
                                                    tableName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "cdc remove TABLE %s failed (will be retried): {}",
                                                    tableName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to cdc remove test table", e);
        }

        try {
            Awaitility.await(String.format("Dropping table %s", tableName))
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql = String.format("DROP TABLE DB2INST1.%s", tableName);
                                    connection.createStatement().execute(sql);
                                    connection.commit();
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "DROP TABLE %s failed (will be retried): {}",
                                                    tableName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeDb2Table(String sqlFile, String tableName) {
        final String ddlFile = String.format("docker/db2/%s.sql", sqlFile);
        final URL ddlTestFile = Db2TestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getDb2Connection();
                Statement statement = connection.createStatement()) {
            String tableExistSql =
                    String.format(
                            "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABNAME = '%s' AND "
                                    + "TABSCHEMA = 'DB2INST1';",
                            tableName);
            ResultSet resultSet = statement.executeQuery(tableExistSql);
            int count = 0;
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }
            if (count == 1) {
                LOG.info("{} table exist", tableName);
                dropTestTable(connection, tableName.toUpperCase(Locale.ROOT));
            }
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
                Thread.sleep(500);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
