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
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.assertj.core.api.Assertions;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

class MysqlToElasticsearch8E2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlToElasticsearch8E2eITCase.class);

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private static final String ELASTICSEARCH_VERSION = "8.12.1";
    private static final String INTER_CONTAINER_ES_ALIAS = "elasticsearch8";
    private static final String DEFAULT_USERNAME = "elastic";
    private static final String DEFAULT_PASSWORD = "123456";

    private ElasticsearchClient client;

    @Container
    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER =
            createElasticsearchContainer();

    @BeforeAll
    static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL, ELASTICSEARCH_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        inventoryDatabase.createAndInitialize();
        client = createElasticsearchClient();
        initEsData();
    }

    @AfterEach
    public void after() {
        super.after();
        inventoryDatabase.dropDatabase();
        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String databaseName = inventoryDatabase.getDatabaseName();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: elasticsearch\n"
                                + "  hosts: %s:9200\n"
                                + "  version: 8\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        INTER_CONTAINER_ES_ALIAS,
                        DEFAULT_USERNAME,
                        DEFAULT_PASSWORD,
                        parallelism);
        Path esConnectorJar = TestUtils.getResource("elasticsearch-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, esConnectorJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        verifySnapshotData(databaseName);
        verifyIncrementalData(databaseName);
    }

    private void verifySnapshotData(String databaseName) throws Exception {
        String productsIndex = databaseName + ".products";
        String customersIndex = databaseName + ".customers";

        // products id=101 (all fields populated)
        waitForEsDocument(productsIndex, "101");
        GetResponse<Map> resp =
                client.get(
                        new GetRequest.Builder().index(productsIndex).id("101").build(), Map.class);
        Assertions.assertThat(resp.source())
                .containsEntry("name", "scooter")
                .containsEntry("description", "Small 2-wheel scooter")
                .containsEntry("weight", 3.14)
                .containsEntry("enum_c", "red")
                .containsEntry("json_c", "{\"key1\": \"value1\"}")
                .containsEntry("point_c", "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}");

        // products id=106 (enum_c, json_c, point_c are null)
        waitForEsDocument(productsIndex, "106");
        resp =
                client.get(
                        new GetRequest.Builder().index(productsIndex).id("106").build(), Map.class);
        Assertions.assertThat(resp.source())
                .containsEntry("name", "hammer")
                .containsEntry("description", "16oz carpenter's hammer")
                .containsEntry("weight", 1.0);

        // products id=109 (last snapshot row)
        waitForEsDocument(productsIndex, "109");
        resp =
                client.get(
                        new GetRequest.Builder().index(productsIndex).id("109").build(), Map.class);
        Assertions.assertThat(resp.source())
                .containsEntry("name", "spare tire")
                .containsEntry("description", "24 inch spare tire")
                .containsEntry("weight", 22.2);

        // customers id=101
        waitForEsDocument(customersIndex, "101");
        resp =
                client.get(
                        new GetRequest.Builder().index(customersIndex).id("101").build(),
                        Map.class);
        Assertions.assertThat(resp.source())
                .containsEntry("name", "user_1")
                .containsEntry("address", "Shanghai")
                .containsEntry("phone_number", "123567891234");
    }

    private void verifyIncrementalData(String databaseName) throws Exception {
        LOG.info("Begin incremental reading stage.");
        String productsIndex = databaseName + ".products";
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            // INSERT
            stat.execute(
                    "INSERT INTO products VALUES "
                            + "(default,'jacket','water resistent white wind breaker',0.2, null, null, null);");
            waitForEsDocument(productsIndex, "110");
            GetResponse<Map> resp =
                    client.get(
                            new GetRequest.Builder().index(productsIndex).id("110").build(),
                            Map.class);
            Assertions.assertThat(resp.source())
                    .containsEntry("name", "jacket")
                    .containsEntry("description", "water resistent white wind breaker")
                    .containsEntry("weight", 0.2);

            // UPDATE
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            waitForEsDocumentField(productsIndex, "106", "description", "18oz carpenter hammer");
            waitForEsDocumentField(productsIndex, "107", "weight", 5.1);

            // DELETE
            stat.execute("DELETE FROM products WHERE id=101;");
            waitForEsDocumentDeleted(productsIndex, "101");
        }
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        DockerImageName imageName =
                DockerImageName.parse(
                        "docker.elastic.co/elasticsearch/elasticsearch:" + ELASTICSEARCH_VERSION);
        ElasticsearchContainer esContainer = new ElasticsearchContainer(imageName);
        esContainer
                .withNetwork(NETWORK)
                .withNetworkAliases(INTER_CONTAINER_ES_ALIAS)
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.http.ssl.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                .withPassword(DEFAULT_PASSWORD)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
        return esContainer;
    }

    private ElasticsearchClient createElasticsearchClient() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(DEFAULT_USERNAME, DEFAULT_PASSWORD));
        RestClientTransport transport =
                new RestClientTransport(
                        RestClient.builder(
                                        new HttpHost(
                                                ELASTICSEARCH_CONTAINER.getHost(),
                                                ELASTICSEARCH_CONTAINER.getFirstMappedPort(),
                                                "http"))
                                .setHttpClientConfigCallback(
                                        httpAsyncClientBuilder ->
                                                httpAsyncClientBuilder
                                                        .setDefaultCredentialsProvider(
                                                                credentialsProvider))
                                .build(),
                        new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private void initEsData() throws IOException {
        String dbName = inventoryDatabase.getDatabaseName();
        createProductsIndex(dbName);
        createCustomersIndex(dbName);
    }

    private void createProductsIndex(String dbName) throws IOException {
        String indexName = dbName + ".products";
        client.indices()
                .create(
                        c ->
                                c.index(indexName)
                                        .mappings(
                                                m ->
                                                        m.properties("id", p -> p.integer(i -> i))
                                                                .properties(
                                                                        "name",
                                                                        p -> p.keyword(k -> k))
                                                                .properties(
                                                                        "description",
                                                                        p -> p.text(t -> t))
                                                                .properties(
                                                                        "weight",
                                                                        p -> p.float_(f -> f))
                                                                .properties(
                                                                        "enum_c",
                                                                        p -> p.keyword(k -> k))
                                                                .properties(
                                                                        "json_c",
                                                                        p -> p.keyword(k -> k))
                                                                .properties(
                                                                        "point_c",
                                                                        p -> p.keyword(k -> k))));
    }

    private void createCustomersIndex(String dbName) throws IOException {
        String indexName = dbName + ".customers";
        client.indices()
                .create(
                        c ->
                                c.index(indexName)
                                        .mappings(
                                                m ->
                                                        m.properties("id", p -> p.integer(i -> i))
                                                                .properties(
                                                                        "name",
                                                                        p -> p.keyword(k -> k))
                                                                .properties(
                                                                        "address",
                                                                        p -> p.text(t -> t))
                                                                .properties(
                                                                        "phone_number",
                                                                        p -> p.keyword(k -> k))));
    }

    private void waitForEsDocument(String indexName, String docId) throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            GetResponse<Map> response =
                    client.get(
                            new GetRequest.Builder().index(indexName).id(docId).build(), Map.class);
            if (response.source() != null) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assertions.fail("Timed out waiting for ES document: " + indexName + "/" + docId);
    }

    private void waitForEsDocumentField(
            String indexName, String docId, String field, Object expectedValue) throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            GetResponse<Map> response =
                    client.get(
                            new GetRequest.Builder().index(indexName).id(docId).build(), Map.class);
            if (response.source() != null && expectedValue.equals(response.source().get(field))) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assertions.fail(
                "Timed out waiting for ES document field: "
                        + indexName
                        + "/"
                        + docId
                        + " "
                        + field
                        + "="
                        + expectedValue);
    }

    private void waitForEsDocumentDeleted(String indexName, String docId) throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            GetResponse<Map> response =
                    client.get(
                            new GetRequest.Builder().index(indexName).id(docId).build(), Map.class);
            if (response.source() == null) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assertions.fail("Document was not deleted within timeout: " + indexName + "/" + docId);
    }
}
