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
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** End-to-end tests for db2 cdc connector uber jar. */
public class Db2E2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(Db2E2eITCase.class);
    private static final String INTER_CONTAINER_DB2_ALIAS = "db2";

    private static final Path db2CdcJar = TestUtils.getResource("db2-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    private static final DockerImageName DEBEZIUM_DOCKER_IMAGE_NAME =
            DockerImageName.parse(
                            new ImageFromDockerfile("custom/db2-cdc:1.4")
                                    .withDockerfile(getFilePath("docker/db2/Dockerfile"))
                                    .get())
                    .asCompatibleSubstituteFor("ibmcom/db2");
    private static boolean db2AsnAgentRunning = false;

    private Db2Container db2Container;

    @Before
    public void before() {
        super.before();
        LOG.info("Starting db2 containers...");
        db2Container =
                new Db2Container(DEBEZIUM_DOCKER_IMAGE_NAME)
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
        List<String> sqlLines =
                Arrays.asList(
                        String.format(
                                "CREATE TABLE products_source ("
                                        + " ID INT NOT NULL,"
                                        + " NAME STRING,"
                                        + " DESCRIPTION STRING,"
                                        + " WEIGHT DECIMAL(10,3)"
                                        + ") WITH ("
                                        + " 'connector' = 'db2-cdc',"
                                        + " 'hostname' = '%s',"
                                        + " 'port' = '%s',"
                                        + " 'username' = '%s',"
                                        + " 'password' = '%s',"
                                        + " 'database-name' = '%s',"
                                        + " 'schema-name' = '%s',"
                                        + " 'table-name' = '%s'"
                                        + ");",
                                INTER_CONTAINER_DB2_ALIAS,
                                DB2_PORT,
                                db2Container.getUsername(),
                                db2Container.getPassword(),
                                db2Container.getDatabaseName(),
                                "DB2INST1",
                                "PRODUCTS"),
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

        submitSQLJob(sqlLines, db2CdcJar, jdbcJar, mysqlDriverJar);
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
}
