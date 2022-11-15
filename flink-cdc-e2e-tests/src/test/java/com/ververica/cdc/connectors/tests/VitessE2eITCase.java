/*
 * Copyright 2023 Ververica Inc.
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
import com.vervetica.cdc.connectors.vitess.VitessTestBase;
import com.vervetica.cdc.connectors.vitess.container.VitessContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.GRPC_PORT;
import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.MYSQL_PORT;
import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.VTCTLD_GRPC_PORT;
import static org.junit.Assert.assertNotNull;

/** End-to-end test for Vitess CDC connector. */
public class VitessE2eITCase extends FlinkContainerTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(VitessE2eITCase.class);
    private static final String VITESS_CONTAINER_NETWORK_ALIAS = "vitess";
    private static final Path VITESS_CDC_JAR = TestUtils.getResource("vitess-cdc-connector.jar");
    private static final Path MYSQL_DRIVER_JAR = TestUtils.getResource("mysql-driver.jar");
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    @SuppressWarnings("unchecked")
    private static final VitessContainer VITESS_CONTAINER =
            (VitessContainer)
                    new VitessContainer()
                            .withKeyspace("test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpwd")
                            .withExposedPorts(MYSQL_PORT, GRPC_PORT, VTCTLD_GRPC_PORT)
                            .withLogConsumer(new Slf4jLogConsumer(LOG))
                            .withNetwork(NETWORK)
                            .withNetworkAliases(VITESS_CONTAINER_NETWORK_ALIAS);

    @Before
    public void setup() {
        LOG.info("Starting Vitess container...");
        VITESS_CONTAINER.start();
        LOG.info("Vitess container is started.");
    }

    @After
    public void tearDown() {
        LOG.info("Stopping Vitess container...");
        VITESS_CONTAINER.stop();
        LOG.info("Vitess container is stopped.");
    }

    @Test
    public void testVitessCDC() throws Exception {
        initializeTable();
        String sourceDDL =
                String.format(
                        "CREATE TABLE products_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'vitess-cdc',"
                                + " 'tablet-type' = 'MASTER',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'vtctl.hostname' = '%s',"
                                + " 'vtctl.port' = '%s',"
                                + " 'keyspace' = '%s',"
                                + " 'table-name' = '%s'"
                                + ");",
                        VITESS_CONTAINER_NETWORK_ALIAS,
                        GRPC_PORT,
                        VITESS_CONTAINER_NETWORK_ALIAS,
                        VTCTLD_GRPC_PORT,
                        VITESS_CONTAINER.getKeyspace(),
                        "test.products");
        String sinkDDL =
                String.format(
                        "CREATE TABLE products_sink (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    name STRING,\n"
                                + "    description STRING,\n"
                                + "    weight DECIMAL(10,3),\n"
                                + "    primary key (`id`) not enforced\n"
                                + ") WITH (\n"
                                + "    'connector' = 'jdbc',\n"
                                + "    'url' = 'jdbc:mysql://%s:3306/%s',\n"
                                + "    'table-name' = 'products_sink',\n"
                                + "    'username' = '%s',\n"
                                + "    'password' = '%s'\n"
                                + ");",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        mysqlInventoryDatabase.getDatabaseName(),
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD);
        List<String> sqlLines =
                Arrays.asList(
                        sourceDDL,
                        sinkDDL,
                        "INSERT INTO products_sink SELECT * FROM products_source;");
        submitSQLJob(sqlLines, VITESS_CDC_JAR, MYSQL_DRIVER_JAR, jdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        waitUntilBinlogDumpStarted();

        try (Connection connection = DriverManager.getConnection(VITESS_CONTAINER.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO test.products \n"
                            + "VALUES (default,'scooter','Small 2-wheel scooter',3.14),\n"
                            + "       (default,'car battery','12V car battery',8.1),\n"
                            + "       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),\n"
                            + "       (default,'hammer','12oz carpenter hammer',0.75),\n"
                            + "       (default,'hammer','14oz carpenter hammer',0.875),\n"
                            + "       (default,'hammer','16oz carpenter hammer',1.0),\n"
                            + "       (default,'rocks','box of assorted rocks',5.3),\n"
                            + "       (default,'jacket','water resistent black wind breaker',0.1),\n"
                            + "       (default,'spare tire','24 inch spare tire',22.2);");
            statement.execute(
                    "UPDATE test.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE test.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO test.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO test.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE test.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE test.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM test.products WHERE id=111;");
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
                        "104,hammer,12oz carpenter hammer,0.75",
                        "105,hammer,14oz carpenter hammer,0.875",
                        "106,hammer,18oz carpenter hammer,1.0",
                        "107,rocks,box of assorted rocks,5.1",
                        "108,jacket,water resistent black wind breaker,0.1",
                        "109,spare tire,24 inch spare tire,22.2",
                        "110,jacket,new water resistent white wind breaker,0.5");
        proxy.checkResultWithTimeout(
                expectResult,
                "products_sink",
                new String[] {"id", "name", "description", "weight"},
                Duration.ofSeconds(30).toMillis());
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private static void initializeTable() {
        final String ddlFile = String.format("ddl/%s.sql", "vitess_inventory");
        final URL ddlTestFile = VitessTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = DriverManager.getConnection(VITESS_CONTAINER.getJdbcUrl());
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

    private static void waitUntilBinlogDumpStarted() {
        new LogMessageWaitStrategy()
                .withRegEx(".*sending binlog dump command.*")
                .waitUntilReady(VITESS_CONTAINER);
    }
}
