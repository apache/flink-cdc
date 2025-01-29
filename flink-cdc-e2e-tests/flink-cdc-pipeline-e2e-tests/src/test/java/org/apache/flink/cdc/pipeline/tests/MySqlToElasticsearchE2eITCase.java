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
import org.apache.flink.cdc.connectors.elasticsearch.sink.utils.ElasticsearchContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

/** End-to-end tests for mysql cdc to Elasticsearch pipeline job. */
@RunWith(Parameterized.class)
public class MySqlToElasticsearchE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToElasticsearchE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";

    @Parameterized.Parameter(1)
    public String elasticsearchVersion;

    public static final Duration DEFAULT_RESULT_VERIFY_TIMEOUT = Duration.ofSeconds(120);

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("mysql");

    private org.testcontainers.elasticsearch.ElasticsearchContainer elasticsearchContainer;
    private static final Integer MAX_ELASTICSERACH_FETCH_SIZE = 20;
    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Parameterized.Parameters(name = "flinkVersion: {0}, elasticsearchVersion: {1}")
    public static Collection<Object[]> getTestParameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (String flinkVersion : getFlinkVersion()) {
            parameters.add(new Object[] {flinkVersion, "6.8.20"});
            parameters.add(new Object[] {flinkVersion, "7.10.2"});
            parameters.add(new Object[] {flinkVersion, "8.12.1"});
        }

        return parameters;
    }

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        elasticsearchContainer =
                new ElasticsearchContainer(elasticsearchVersion)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("elasticsearch")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(Stream.of(MYSQL, elasticsearchContainer)).join();
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
        try {
            dropElasticsearchIndex(mysqlInventoryDatabase.getDatabaseName() + "." + "products");
            dropElasticsearchIndex(mysqlInventoryDatabase.getDatabaseName() + "." + "customers");
            if (elasticsearchContainer != null) {
                elasticsearchContainer.stop();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
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
                                + "  parallelism: %d",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        elasticsearchVersion.split("\\.")[0],
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path elasticsearchCdcConnector =
                TestUtils.getResource("elasticsearch-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, elasticsearchCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkResult(
                databaseName + "." + "products",
                Arrays.asList("id", "name", "description", "weight", "enum_c", "json_c", "point_c"),
                Arrays.asList(
                        "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | box of assorted rocks | 5.3 | null | null | null",
                        "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                        "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null"));

        validateSinkResult(
                databaseName + "." + "customers",
                Arrays.asList("id", "name", "address", "phone_number"),
                Arrays.asList(
                        "101 | user_1 | Shanghai | 123567891234",
                        "102 | user_2 | Shanghai | 123567891234",
                        "103 | user_3 | Shanghai | 123567891234",
                        "104 | user_4 | Shanghai | 123567891234"));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110

            validateSinkResult(
                    databaseName + "." + "products",
                    Arrays.asList(
                            "id", "name", "description", "weight", "enum_c", "json_c", "point_c"),
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                            "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.3 | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null"));

            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            validateSinkResult(
                    databaseName + "." + "products",
                    Arrays.asList(
                            "id", "name", "description", "weight", "enum_c", "json_c", "point_c"),
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                            "106 | hammer | 18oz carpenter hammer | 1.0 | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.1 | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null"));

            stat.execute("DELETE FROM products WHERE id=101;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null, null);"); // 112
            validateSinkResult(
                    databaseName + "." + "products",
                    Arrays.asList(
                            "id", "name", "description", "weight", "enum_c", "json_c", "point_c"),
                    Arrays.asList(
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                            "106 | hammer | 18oz carpenter hammer | 1.0 | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.1 | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null",
                            "111 | scooter | Big 2-wheel scooter  | 5.18 | null | null | null",
                            "112 | finally | null | 2.14 | null | null | null"));
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    private void validateSinkResult(String indexName, List<String> columns, List<String> expected)
            throws Exception {
        waitAndVerify(indexName, columns, expected, DEFAULT_RESULT_VERIFY_TIMEOUT.toMillis(), true);
    }

    private void waitAndVerify(
            String indexName,
            List<String> columns,
            List<String> expected,
            long timeoutMilliseconds,
            boolean inAnyOrder)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMilliseconds;
        while (System.currentTimeMillis() < deadline) {
            try {
                List<String> actual = fetchTableContent(indexName, columns);
                if (inAnyOrder) {
                    if (expected.stream()
                            .sorted()
                            .collect(Collectors.toList())
                            .equals(actual.stream().sorted().collect(Collectors.toList()))) {
                        return;
                    }
                } else {
                    if (expected.equals(actual)) {
                        return;
                    }
                }
                LOG.info(
                        "Executing {}:: didn't get expected results.\nExpected: {}\n  Actual: {}",
                        indexName,
                        expected,
                        actual);
            } catch (Exception t) {
                LOG.info("Database {} isn't ready yet. Waiting for the next loop...", indexName);
            }
            Thread.sleep(1000L);
        }
        fail(String.format("Failed to verify content of %s.", indexName));
    }

    private List<String> fetchTableContent(String indexName, List<String> columns)
            throws Exception {

        List<String> results = new ArrayList<>();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String url =
                String.format(
                        "http://%s:%d/%s/_search?from=0&size=%s",
                        elasticsearchContainer.getHost(),
                        elasticsearchContainer.getFirstMappedPort(),
                        indexName,
                        MAX_ELASTICSERACH_FETCH_SIZE);

        HttpGet request = new HttpGet(url);
        HttpResponse response = httpClient.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity());

        if (statusCode != 200) {
            LOG.error("Failed to query index. Status code: {}", statusCode);
            Thread.sleep(1000);
            throw new Exception();
        }
        ObjectMapper objectMapper = new ObjectMapper();
        Map responseMap = objectMapper.readValue(responseBody, Map.class);
        Map<String, Object> hitsMap = (Map<String, Object>) responseMap.get("hits");
        List<Map<String, Object>> hitsArr = (List<Map<String, Object>>) hitsMap.get("hits");

        for (int i = 0; i < hitsArr.size(); i++) {
            Map<String, Object> hit = hitsArr.get(i);
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            List<String> doc = new ArrayList<>();
            for (String column : columns) {
                doc.add(String.valueOf(source.get(column)));
            }
            results.add(String.join(" | ", doc));
        }
        return results;
    }

    private void dropElasticsearchIndex(String indexName) {
        String url =
                String.format(
                        "http://%s:%d/%s",
                        elasticsearchContainer.getHost(),
                        elasticsearchContainer.getFirstMappedPort(),
                        indexName);
        LOG.info("Dropping Elasticsearch index: {}", url);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpDelete request = new HttpDelete(url);
            HttpResponse response = httpClient.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(response.getEntity());
            LOG.info("Delete index response (status code {}): \n{}", statusCode, responseBody);

            if (statusCode != 200 && statusCode != 404) {
                LOG.error("Failed to delete index. Status code: {}", statusCode);
                throw new RuntimeException("Failed to delete index. Status code: " + statusCode);
            }
        } catch (IOException e) {
            LOG.error("Error while deleting Elasticsearch index", e);
            throw new RuntimeException("Failed to delete index", e);
        }
    }
}
