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
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.get.GetRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.get.GetResponse;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RequestOptions;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestHighLevelClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;

import org.assertj.core.api.Assertions;
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
import java.util.stream.Stream;

class MysqlToElasticsearch7E2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlToElasticsearch7E2eITCase.class);

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private static final String ELASTICSEARCH_VERSION = "7.10.2";
    private static final String INTER_CONTAINER_ES_ALIAS = "elasticsearch7";

    private RestHighLevelClient client;

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
                client.close();
            } catch (IOException e) {
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
                                + "  version: 7\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        INTER_CONTAINER_ES_ALIAS,
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
        GetResponse resp =
                client.get(new GetRequest(productsIndex).id("101"), RequestOptions.DEFAULT);
        Assertions.assertThat(resp.getSource())
                .containsEntry("name", "scooter")
                .containsEntry("description", "Small 2-wheel scooter")
                .containsEntry("weight", 3.14)
                .containsEntry("enum_c", "red")
                .containsEntry("json_c", "{\"key1\": \"value1\"}")
                .containsEntry("point_c", "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}");

        // products id=106 (enum_c, json_c, point_c are null)
        waitForEsDocument(productsIndex, "106");
        resp = client.get(new GetRequest(productsIndex).id("106"), RequestOptions.DEFAULT);
        Assertions.assertThat(resp.getSource())
                .containsEntry("name", "hammer")
                .containsEntry("description", "16oz carpenter's hammer")
                .containsEntry("weight", 1.0);

        // products id=109 (last snapshot row)
        waitForEsDocument(productsIndex, "109");
        resp = client.get(new GetRequest(productsIndex).id("109"), RequestOptions.DEFAULT);
        Assertions.assertThat(resp.getSource())
                .containsEntry("name", "spare tire")
                .containsEntry("description", "24 inch spare tire")
                .containsEntry("weight", 22.2);

        // customers id=101
        waitForEsDocument(customersIndex, "101");
        resp = client.get(new GetRequest(customersIndex).id("101"), RequestOptions.DEFAULT);
        Assertions.assertThat(resp.getSource())
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
            GetResponse resp =
                    client.get(new GetRequest(productsIndex).id("110"), RequestOptions.DEFAULT);
            Assertions.assertThat(resp.getSource())
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
                                "docker.elastic.co/elasticsearch/elasticsearch:"
                                        + ELASTICSEARCH_VERSION)
                        .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        ElasticsearchContainer esContainer = new ElasticsearchContainer(imageName);
        esContainer
                .withNetwork(NETWORK)
                .withNetworkAliases(INTER_CONTAINER_ES_ALIAS)
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                .withLogConsumer(new Slf4jLogConsumer(LOG));
        return esContainer;
    }

    private RestHighLevelClient createElasticsearchClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                ELASTICSEARCH_CONTAINER.getHost(),
                                ELASTICSEARCH_CONTAINER.getFirstMappedPort(),
                                "http")));
    }

    private void initEsData() throws IOException {
        String dbName = inventoryDatabase.getDatabaseName();
        createProductsIndex(dbName);
        createCustomersIndex(dbName);
    }

    private void createProductsIndex(String dbName) throws IOException {
        String indexName = dbName + ".products";
        String source =
                "{\"mappings\":{\"_doc\":{\"properties\":{"
                        + "\"id\":{\"type\":\"integer\"},"
                        + "\"name\":{\"type\":\"keyword\"},"
                        + "\"description\":{\"type\":\"text\"},"
                        + "\"weight\":{\"type\":\"float\"},"
                        + "\"enum_c\":{\"type\":\"keyword\"},"
                        + "\"json_c\":{\"type\":\"keyword\"},"
                        + "\"point_c\":{\"type\":\"keyword\"}"
                        + "}}}}";
        client.indices()
                .create(
                        new CreateIndexRequest(indexName).source(source, XContentType.JSON),
                        RequestOptions.DEFAULT);
    }

    private void createCustomersIndex(String dbName) throws IOException {
        String indexName = dbName + ".customers";
        String source =
                "{\"mappings\":{\"_doc\":{\"properties\":{"
                        + "\"id\":{\"type\":\"integer\"},"
                        + "\"name\":{\"type\":\"keyword\"},"
                        + "\"address\":{\"type\":\"text\"},"
                        + "\"phone_number\":{\"type\":\"keyword\"}"
                        + "}}}}";
        client.indices()
                .create(
                        new CreateIndexRequest(indexName).source(source, XContentType.JSON),
                        RequestOptions.DEFAULT);
    }

    private void waitForEsDocument(String indexName, String docId) throws Exception {
        long deadline = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            GetRequest getRequest = new GetRequest(indexName).id(docId);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
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
            GetRequest getRequest = new GetRequest(indexName).id(docId);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists() && expectedValue.equals(response.getSource().get(field))) {
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
            GetRequest getRequest = new GetRequest(indexName).id(docId);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (!response.isExists()) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assertions.fail("Document was not deleted within timeout: " + indexName + "/" + docId);
    }
}
