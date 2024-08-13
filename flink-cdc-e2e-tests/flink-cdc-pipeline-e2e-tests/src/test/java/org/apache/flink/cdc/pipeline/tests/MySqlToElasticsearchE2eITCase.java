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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * End-to-end integration test for MySQL to Elasticsearch pipeline. This class sets up a MySQL
 * database and an Elasticsearch instance, and then verifies data synchronization between the two
 * using a Flink pipeline.
 */
@RunWith(Parameterized.class)
public class MySqlToElasticsearchE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToElasticsearchE2eITCase.class);

    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final long EVENT_DEFAULT_TIMEOUT = 60000L;

    @Parameterized.Parameter(1)
    public String elasticsearchVersion;

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(MySqlVersion.V8_0)
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("mysql");

    private ElasticsearchContainer elasticsearch;

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Parameterized.Parameters(name = "flinkVersion: {0}, elasticsearchVersion: {1}")
    public static Collection<Object[]> getTestParameters() {
        List<Object[]> parameters = new ArrayList<>();

        // Elasticsearch 8.x supports all Flink versions
        for (String flinkVersion : Arrays.asList("1.17.2", "1.18.1", "1.19.1", "1.20.0")) {
            parameters.add(new Object[] {flinkVersion, "8.12.1"});
        }

        // Elasticsearch 6.x and 7.x only support Flink 1.20.0
        parameters.add(new Object[] {"1.20.0", "6.8.20"});
        parameters.add(new Object[] {"1.20.0", "7.10.2"});

        return parameters;
    }

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();

        LOG.info("Starting Elasticsearch {} container...", elasticsearchVersion);
        elasticsearch =
                new ElasticsearchContainer(
                                "docker.elastic.co/elasticsearch/elasticsearch:"
                                        + elasticsearchVersion)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("elasticsearch")
                        .withEnv("discovery.type", "single-node")
                        .withEnv("xpack.security.enabled", "false")
                        .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                        .withExposedPorts(9200, 9300)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .waitingFor(
                                Wait.forHttp("/_cluster/health")
                                        .forStatusCodeMatching(
                                                response -> response == 200 || response == 401)
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        Startables.deepStart(Stream.of(elasticsearch)).join();
        LOG.info("Elasticsearch container is started.");
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
        dropElasticsearchIndex("customers");
        dropElasticsearchIndex("products");
        if (elasticsearch != null) {
            elasticsearch.stop();
        }
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: elasticsearch\n"
                                + "  hosts: http://elasticsearch:9200\n"
                                + "  version: %s\n"
                                + "  batch.size.max.bytes: 5242880\n"
                                + "  record.size.max.bytes: 5242880\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        elasticsearchVersion.split("\\.")[0] // Use major version number
                        );

        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path elasticsearchCdcConnector =
                TestUtils.getResource(
                        "flink-cdc-pipeline-connector-elasticsearch-3.2-SNAPSHOT.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

        submitPipelineJob(pipelineJob, mysqlCdcJar, elasticsearchCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateElasticsearchContent(
                "customers", Arrays.asList("user_1", "user_2", "user_3", "user_4"));
        validateElasticsearchContent(
                "products",
                Arrays.asList(
                        "scooter",
                        "car battery",
                        "12-pack drill bits",
                        "hammer",
                        "rocks",
                        "jacket",
                        "spare tire"));

        LOG.info("Begin incremental reading stage.");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());

        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);");

            validateElasticsearchContent("products", Arrays.asList("jacket"));

            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            stat.execute("DELETE FROM products WHERE id=101;");

            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter', null, 2.14, null, null);");

            validateElasticsearchContent("products", Arrays.asList("scooter"));
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateElasticsearchContent("products", Arrays.asList("jacket", "scooter"));
    }

    private void validateElasticsearchContent(String tableName, List<String> expectedDocuments)
            throws Exception {
        String indexName =
                String.format("%s.%s", mysqlInventoryDatabase.getDatabaseName(), tableName);
        int hostPort = elasticsearch.getMappedPort(9200);
        String url = String.format("http://localhost:%d/%s/_search?pretty", hostPort, indexName);
        waitForElasticsearchContent(url, expectedDocuments);
    }

    private void waitForElasticsearchContent(String url, List<String> expectedDocuments)
            throws Exception {
        long timeout = System.currentTimeMillis() + EVENT_DEFAULT_TIMEOUT;
        while (System.currentTimeMillis() < timeout) {
            ProcessBuilder processBuilder = new ProcessBuilder("curl", "-X", "GET", url);
            Process process = processBuilder.start();
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("Failed to query index.");
            }

            Scanner scanner = new Scanner(process.getInputStream()).useDelimiter("\\A");
            String response = scanner.hasNext() ? scanner.next() : "";
            LOG.info("Elasticsearch query response: \n{}", response);

            boolean allFound = true;
            for (String expectedDoc : expectedDocuments) {
                if (!response.contains(expectedDoc)) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                return;
            }

            Thread.sleep(1000);
        }

        throw new TimeoutException(
                "Failed to find expected documents in Elasticsearch within the timeout period.");
    }

    private void dropElasticsearchIndex(String tableName) {
        try {
            String indexName = String.format("mysql_inventory.%s", tableName);
            int hostPort = elasticsearch.getMappedPort(9200);
            String url = String.format("http://localhost:%d/%s", hostPort, indexName);
            String curlCommand = String.format("curl -X DELETE %s", url);
            LOG.info("Drop Index CURL Command: {}", curlCommand);

            ProcessBuilder processBuilder = new ProcessBuilder("curl", "-X", "DELETE", url);
            Process process = processBuilder.start();
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                LOG.error("Failed to delete index. Exit code: {}", exitCode);
                throw new RuntimeException("Failed to delete index. Exit code: " + exitCode);
            }
        } catch (Exception e) {
            LOG.error("Error while deleting Elasticsearch index", e);
            throw new RuntimeException("Failed to delete index", e);
        }
    }
}
